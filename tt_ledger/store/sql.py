"""``SqlLedgerStore`` — async SQLAlchemy implementation of LedgerStore (docs/storage.md).

Backend is chosen entirely by the connection URL:
  * ``sqlite+aiosqlite:///ledger.db``  (bundled default)
  * ``postgresql+asyncpg://…``          ([postgres] extra)

The ONLY dialect branch is the upsert helper (``_insert`` below) — everything else is
dialect-agnostic Core SQL over ``Base.metadata`` tables.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from typing import Any

from sqlalchemy import bindparam, select, update
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ..enums import Ingest, Origin, ReviewStatus
from ..rows import (
    ActivityFilter,
    ActivityRow,
    EventRow,
    FillRow,
    LegRow,
    OrderFilter,
    OrderRow,
    PositionRow,
    SecurityRow,
    TradeFilter,
    TradeGroupRow,
    TradeRow,
    TxnRow,
)
from ..schema import metadata, models


def _insert(dialect: str):
    """Return the dialect-specific ``insert`` (the one place we branch)."""
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:  # sqlite and other ON CONFLICT dialects
        from sqlalchemy.dialects.sqlite import insert
    return insert


def _row_dict(dc: Any) -> dict[str, Any]:
    """A dataclass row -> a plain dict, enum members lowered to their ``.value``."""
    out = asdict(dc)
    for key, value in out.items():
        if isinstance(value, Enum):
            out[key] = value.value
    return out


def _day_start(d: date) -> datetime:
    """A calendar date -> its UTC midnight boundary (avoids ``CAST(... AS DATE)``, which SQLite's
    NUMERIC-affinity fallback for the ``DATE`` type does not evaluate usefully against stored
    ``DateTime`` strings)."""
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def _mapping_to(cls: type, row: Row, **enum_fields: type[Enum]) -> Any:
    """A DB row -> a ``cls`` dataclass instance; ``enum_fields`` names get wrapped in their enum."""
    data = dict(row._mapping)
    for field_name, enum_cls in enum_fields.items():
        if data.get(field_name) is not None:
            data[field_name] = enum_cls(data[field_name])
    valid = {f for f in cls.__dataclass_fields__}
    return cls(**{k: v for k, v in data.items() if k in valid})


class SqlLedgerStore:
    def __init__(self, url: str = "sqlite+aiosqlite:///ledger.db") -> None:
        engine_kwargs: dict[str, Any] = {"pool_pre_ping": True}
        if url.startswith("sqlite") and ":memory:" in url:
            # an in-memory SQLite DB is per-connection; pin the pool to one connection so
            # every session in this store instance sees the same database.
            from sqlalchemy.pool import StaticPool

            engine_kwargs = {"poolclass": StaticPool, "connect_args": {"check_same_thread": False}}
        self._engine = create_async_engine(url, **engine_kwargs)
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False)
        self._dialect = self._engine.dialect.name

    async def create_all(self) -> None:
        """Dev/standalone convenience (prod uses Alembic)."""
        async with self._engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

    async def dispose(self) -> None:
        await self._engine.dispose()

    # --- upsert plumbing ---------------------------------------------------------

    async def _upsert(
        self,
        session: AsyncSession,
        table,
        rows: list[dict[str, Any]],
        conflict_cols: list[str],
        *,
        index_where=None,
    ) -> list[int]:
        """Upsert ``rows`` and return their surrogate ids, in the same order as ``rows``.

        Relies on both dialects preserving VALUES-list order in a single-statement multi-row
        INSERT (with or without an ON CONFLICT branch) for RETURNING — observed behavior on
        SQLite and Postgres, exercised by ``test_upsert_orders_returns_ids_in_input_order``.
        """
        if not rows:
            return []
        insert = _insert(self._dialect)
        stmt = insert(table)
        immutable = {"id", "created_at", "first_seen_at"}
        update_cols = {
            c.name: getattr(stmt.excluded, c.name)
            for c in table.columns
            if c.name not in conflict_cols and c.name not in immutable
        }
        stmt = stmt.values(rows).on_conflict_do_update(
            index_elements=conflict_cols, index_where=index_where, set_=update_cols,
        ).returning(table.c.id)
        result = await session.execute(stmt)
        return [row.id for row in result]

    # --- writes --------------------------------------------------------------------

    async def upsert_orders(self, rows: list[OrderRow]) -> list[int]:
        if not rows:
            return []
        table = models.Order.__table__
        async with self._sessionmaker() as session, session.begin():
            return await self._upsert(
                session, table, [_row_dict(r) for r in rows], ["tt_order_id"],
                index_where=table.c.tt_order_id.isnot(None),
            )

    async def upsert_legs(self, rows: list[LegRow]) -> list[int]:
        if not rows:
            return []
        table = models.OrderLeg.__table__
        async with self._sessionmaker() as session, session.begin():
            return await self._upsert(session, table, [_row_dict(r) for r in rows], ["order_id", "leg_index"])

    async def upsert_fills(self, rows: list[FillRow]) -> None:
        if not rows:
            return
        table = models.OrderFill.__table__
        async with self._sessionmaker() as session, session.begin():
            await self._upsert(session, table, [_row_dict(r) for r in rows], ["fill_id"])

    async def upsert_transactions(self, rows: list[TxnRow]) -> None:
        if not rows:
            return
        table = models.Transaction.__table__
        async with self._sessionmaker() as session, session.begin():
            await self._upsert(session, table, [_row_dict(r) for r in rows], ["tt_transaction_id"])

    async def upsert_security(self, sec: SecurityRow) -> None:
        table = models.Security.__table__
        async with self._sessionmaker() as session, session.begin():
            await self._upsert(session, table, [_row_dict(sec)], ["security_id"])

    async def upsert_positions(self, rows: list[PositionRow]) -> None:
        if not rows:
            return
        table = models.Position.__table__
        async with self._sessionmaker() as session, session.begin():
            await self._upsert(session, table, [_row_dict(r) for r in rows], ["account", "security_id"])

    # --- linking + grouping ----------------------------------------------------------

    async def link_transactions_to_orders(self, account: str) -> int:
        txns = models.Transaction.__table__
        orders = models.Order.__table__
        async with self._sessionmaker() as session, session.begin():
            pending = (
                await session.execute(
                    select(txns.c.id, txns.c.tt_order_id).where(
                        txns.c.account == account,
                        txns.c.order_id.is_(None),
                        txns.c.tt_order_id.isnot(None),
                    )
                )
            ).all()
            if not pending:
                return 0

            tt_ids = {row.tt_order_id for row in pending}
            id_by_tt_order_id = {
                row.tt_order_id: row.id
                for row in (
                    await session.execute(
                        select(orders.c.id, orders.c.tt_order_id).where(
                            orders.c.account == account, orders.c.tt_order_id.in_(tt_ids),
                        )
                    )
                ).all()
            }

            updates = [
                {"_txn_id": txn_id, "_order_id": id_by_tt_order_id[tt_order_id]}
                for txn_id, tt_order_id in pending
                if tt_order_id in id_by_tt_order_id
            ]
            if updates:
                await session.execute(
                    update(txns).where(txns.c.id == bindparam("_txn_id")).values(order_id=bindparam("_order_id")),
                    updates,
                )
            return len(updates)

    async def upsert_trade_group(self, tg: TradeGroupRow) -> int:
        table = models.TradeGroup.__table__
        async with self._sessionmaker() as session, session.begin():
            ids = await self._upsert(session, table, [_row_dict(tg)], ["group_id"])
        return ids[0]

    async def add_trade_group_event(self, ev: EventRow) -> None:
        table = models.TradeGroupEvent.__table__
        data = _row_dict(ev)
        if data.get("event_at") is None:
            data["event_at"] = datetime.now(UTC)
        async with self._sessionmaker() as session, session.begin():
            await session.execute(table.insert().values(**data))

    async def attach_transactions_to_trade_group(self, tt_transaction_ids: list[str], trade_group_id: int) -> int:
        if not tt_transaction_ids:
            return 0
        table = models.Transaction.__table__
        async with self._sessionmaker() as session, session.begin():
            result = await session.execute(
                table.update()
                .where(table.c.tt_transaction_id.in_(tt_transaction_ids))
                .values(trade_group_id=trade_group_id)
            )
        return result.rowcount

    async def move_transactions_to_group(self, txn_ids: list[int], trade_group_id: int | None) -> int:
        if not txn_ids:
            return 0
        table = models.Transaction.__table__
        async with self._sessionmaker() as session, session.begin():
            result = await session.execute(
                table.update().where(table.c.id.in_(txn_ids)).values(trade_group_id=trade_group_id)
            )
        return result.rowcount

    # --- reads (consolidated views, as methods) ---------------------------------------

    async def get_order(self, tt_order_id: str) -> OrderRow | None:
        table = models.Order.__table__
        async with self._sessionmaker() as session:
            row = (await session.execute(select(table).where(table.c.tt_order_id == tt_order_id))).first()
        if row is None:
            return None
        return _mapping_to(OrderRow, row, origin=Origin, ingest=Ingest)

    async def get_position(self, account: str, security_id: str) -> PositionRow | None:
        table = models.Position.__table__
        async with self._sessionmaker() as session:
            row = (
                await session.execute(
                    select(table).where(table.c.account == account, table.c.security_id == security_id)
                )
            ).first()
        return _mapping_to(PositionRow, row) if row is not None else None

    async def get_security(self, security_id: str) -> SecurityRow | None:
        table = models.Security.__table__
        async with self._sessionmaker() as session:
            row = (await session.execute(select(table).where(table.c.security_id == security_id))).first()
        return _mapping_to(SecurityRow, row) if row is not None else None

    async def get_trade_group(self, group_id: str) -> TradeGroupRow | None:
        table = models.TradeGroup.__table__
        async with self._sessionmaker() as session:
            row = (await session.execute(select(table).where(table.c.group_id == group_id))).first()
        if row is None:
            return None
        return _mapping_to(TradeGroupRow, row, origin=Origin, review_status=ReviewStatus)

    async def get_trade_group_by_id(self, trade_group_id: int) -> TradeGroupRow | None:
        table = models.TradeGroup.__table__
        async with self._sessionmaker() as session:
            row = (await session.execute(select(table).where(table.c.id == trade_group_id))).first()
        if row is None:
            return None
        return _mapping_to(TradeGroupRow, row, origin=Origin, review_status=ReviewStatus)

    async def get_trade_group_id(self, group_id: str) -> int | None:
        table = models.TradeGroup.__table__
        async with self._sessionmaker() as session:
            row = (await session.execute(select(table.c.id).where(table.c.group_id == group_id))).first()
        return row.id if row is not None else None

    async def get_transactions_by_id(self, txn_ids: list[int]) -> list[TxnRow]:
        if not txn_ids:
            return []
        table = models.Transaction.__table__
        async with self._sessionmaker() as session:
            rows = (await session.execute(select(table).where(table.c.id.in_(txn_ids)))).all()
        return [_mapping_to(TxnRow, r) for r in rows]

    async def query_orders(self, f: OrderFilter) -> list[OrderRow]:
        table = models.Order.__table__
        stmt = select(table)
        if f.origin is not None:
            stmt = stmt.where(table.c.origin == f.origin.value)
        if f.account is not None:
            stmt = stmt.where(table.c.account == f.account)
        if f.status is not None:
            stmt = stmt.where(table.c.oms_status == f.status)
        if f.underlying is not None:
            stmt = stmt.where(table.c.underlying == f.underlying)
        if f.trade_group_id is not None:
            stmt = stmt.where(table.c.trade_group_id == f.trade_group_id)
        if f.start is not None:
            stmt = stmt.where(table.c.submitted_at >= _day_start(f.start))
        if f.end is not None:
            stmt = stmt.where(table.c.submitted_at < _day_start(f.end) + timedelta(days=1))
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [_mapping_to(OrderRow, r, origin=Origin, ingest=Ingest) for r in rows]

    async def unified_trades(self, f: TradeFilter) -> list[TradeRow]:
        table = models.TradeGroup.__table__
        stmt = select(table)
        if f.origin is not None:
            stmt = stmt.where(table.c.origin == f.origin.value)
        if f.review_status is not None:
            stmt = stmt.where(table.c.review_status == f.review_status.value)
        if f.status is not None:
            stmt = stmt.where(table.c.status == f.status)
        if f.account is not None:
            stmt = stmt.where(table.c.account == f.account)
        if f.underlying is not None:
            stmt = stmt.where(table.c.underlying == f.underlying)
        if f.start is not None:
            stmt = stmt.where(table.c.executed_at >= _day_start(f.start))
        if f.end is not None:
            stmt = stmt.where(table.c.executed_at < _day_start(f.end) + timedelta(days=1))
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [_mapping_to(TradeRow, r, origin=Origin, review_status=ReviewStatus) for r in rows]

    async def account_activity(self, f: ActivityFilter) -> list[ActivityRow]:
        txns = models.Transaction.__table__
        orders = models.Order.__table__
        groups = models.TradeGroup.__table__
        cols = [
            *[c for c in txns.columns if c.name not in ("id", "created_at", "updated_at")],
            orders.c.origin.label("origin"),
            groups.c.review_status.label("review_status"),
        ]
        stmt = (
            select(*cols)
            .select_from(
                txns.outerjoin(orders, txns.c.order_id == orders.c.id)
                .outerjoin(groups, txns.c.trade_group_id == groups.c.id)
            )
        )
        if f.account is not None:
            stmt = stmt.where(txns.c.account == f.account)
        if f.start is not None:
            stmt = stmt.where(txns.c.executed_at >= _day_start(f.start))
        if f.end is not None:
            stmt = stmt.where(txns.c.executed_at < _day_start(f.end) + timedelta(days=1))
        if f.unreconciled_only:
            stmt = stmt.where(txns.c.order_id.is_(None))
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [_mapping_to(ActivityRow, r, origin=Origin, review_status=ReviewStatus) for r in rows]
