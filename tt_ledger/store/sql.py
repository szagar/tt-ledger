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

from sqlalchemy import bindparam, case, func, select, text, update
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ..enums import Ingest, Origin, ReviewStatus
from ..rows import (
    AccountRow,
    ActivityFilter,
    ActivityRow,
    BalanceSnapshotRow,
    ClosedPositionRow,
    EventRow,
    FillRow,
    LegDetailRow,
    LegRow,
    OrderFilter,
    OrderRow,
    PositionRow,
    SecurityRow,
    TradeFilter,
    TradeGroupRow,
    TradeRow,
    TransactionDetailRow,
    TransactionQuery,
    TxnRow,
)
from ..schema import metadata, models
from ..schema.namespace import pg_schema, translate_map_for


# Linkage columns importers never know but reconcile/replay fill in -- preserved across
# re-upserts of the same tt_transaction_id (see _upsert's preserve_if_null).
_TXN_LINKAGE_COLS = {"order_id", "order_leg_id", "position_id", "closed_position_id", "trade_group_id"}

# Comfortably under asyncpg's 32767-bind-parameter statement cap (SQLite's modern cap is higher).
_MAX_BIND_PARAMS = 30000


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
        translate_map = translate_map_for(url)
        if translate_map is not None:
            # Postgres: ledger tables live in a dedicated schema (schema/namespace.py).
            engine_kwargs["execution_options"] = {"schema_translate_map": translate_map}
        self._engine = create_async_engine(url, **engine_kwargs)
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False)
        self._dialect = self._engine.dialect.name

    async def create_all(self) -> None:
        """Dev/standalone convenience (prod uses Alembic)."""
        async with self._engine.begin() as conn:
            if self._dialect == "postgresql":
                await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema()}"'))
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
        preserve_if_null: set[str] | None = None,
    ) -> list[int]:
        """Upsert ``rows`` and return their surrogate ids, in the same order as ``rows``.

        ``preserve_if_null`` names columns whose EXISTING value survives when the incoming row
        carries None — for linkage columns (e.g. ``transactions.trade_group_id``) that an
        importer never knows but a later pass (reconcile/replay) fills in; without this, every
        overlapping re-sync would wipe the linkage and reconcile would re-group already-grouped
        activity into duplicates.

        Relies on both dialects preserving VALUES-list order in a single-statement multi-row
        INSERT (with or without an ON CONFLICT branch) for RETURNING — observed behavior on
        SQLite and Postgres, exercised by ``test_upsert_orders_returns_ids_in_input_order``.
        """
        if not rows:
            return []
        insert = _insert(self._dialect)
        stmt = insert(table)
        immutable = {"id", "created_at", "first_seen_at"}
        update_cols = {}
        for c in table.columns:
            if c.name in conflict_cols or c.name in immutable:
                continue
            excluded_col = getattr(stmt.excluded, c.name)
            if preserve_if_null and c.name in preserve_if_null:
                update_cols[c.name] = func.coalesce(excluded_col, c)
            else:
                update_cols[c.name] = excluded_col

        # Chunk the VALUES list: asyncpg hard-caps a statement at 32767 bind parameters, so a
        # full-history backfill (thousands of rows x ~35 columns) must split. Sequential chunks
        # keep the returned surrogate ids in input order. Size on the TABLE's column count, not
        # the row dict: compilation adds Python-side column defaults (created_at/updated_at) as
        # extra binds per row.
        params_per_row = max(len(table.columns), 1)
        chunk_size = max(1, _MAX_BIND_PARAMS // params_per_row)
        ids: list[int] = []
        for start in range(0, len(rows), chunk_size):
            chunk = rows[start:start + chunk_size]
            chunk_stmt = stmt.values(chunk).on_conflict_do_update(
                index_elements=conflict_cols, index_where=index_where, set_=update_cols,
            ).returning(table.c.id)
            result = await session.execute(chunk_stmt)
            ids.extend(row.id for row in result)
        return ids

    # --- writes --------------------------------------------------------------------

    async def upsert_account(self, row: AccountRow) -> None:
        table = models.Account.__table__
        async with self._sessionmaker() as session, session.begin():
            await self._upsert(session, table, [_row_dict(row)], ["nickname"])

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
            await self._upsert(
                session, table, [_row_dict(r) for r in rows], ["tt_transaction_id"],
                preserve_if_null=_TXN_LINKAGE_COLS,
            )

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

    async def upsert_balance_snapshot(self, row: BalanceSnapshotRow) -> None:
        table = models.BalanceSnapshot.__table__
        async with self._sessionmaker() as session, session.begin():
            await self._upsert(session, table, [_row_dict(row)], ["account", "captured_at", "source"])

    async def upsert_closed_position(self, row: ClosedPositionRow) -> int:
        """App-level upsert on ``(account, security_id, opened_at, closed_at)`` -- there's no DB
        unique constraint for it (closed_positions predates this writer), so conflict detection is
        a plain SELECT-then-insert/update rather than ``ON CONFLICT`` (docs/ingestion.md → Replay)."""
        table = models.ClosedPosition.__table__
        data = _row_dict(row)
        async with self._sessionmaker() as session, session.begin():
            existing = (
                await session.execute(
                    select(table.c.id).where(
                        table.c.account == row.account, table.c.security_id == row.security_id,
                        table.c.opened_at == row.opened_at, table.c.closed_at == row.closed_at,
                    )
                )
            ).first()
            if existing is not None:
                await session.execute(table.update().where(table.c.id == existing.id).values(**data))
                return existing.id
            result = await session.execute(table.insert().values(**data).returning(table.c.id))
            return result.scalar_one()

    # --- linking + grouping ----------------------------------------------------------

    async def link_orders_to_groups(self, account: str) -> int:
        """Self-heal: stamp ``orders.trade_group_id`` from member transactions for orders whose
        transactions all agree on ONE group (skips ambiguous multi-group orders)."""
        orders = models.Order.__table__
        txns = models.Transaction.__table__
        sub = (
            select(txns.c.tt_order_id, func.min(txns.c.trade_group_id).label("tg"))
            .where(
                txns.c.account == account,
                txns.c.tt_order_id.isnot(None),
                txns.c.trade_group_id.isnot(None),
            )
            .group_by(txns.c.tt_order_id)
            .having(func.count(func.distinct(txns.c.trade_group_id)) == 1)
            .subquery()
        )
        async with self._sessionmaker() as session, session.begin():
            result = await session.execute(
                update(orders)
                .where(
                    orders.c.account == account,
                    orders.c.trade_group_id.is_(None),
                    orders.c.tt_order_id == sub.c.tt_order_id,
                )
                .values(trade_group_id=sub.c.tg)
            )
            return result.rowcount or 0

    async def link_transactions_to_positions(self, links: list[tuple[str, int | None, int | None]]) -> int:
        if not links:
            return 0
        table = models.Transaction.__table__
        updates = [
            {"_tt_transaction_id": tt_transaction_id, "_position_id": position_id, "_closed_position_id": closed_position_id}
            for tt_transaction_id, position_id, closed_position_id in links
        ]
        async with self._sessionmaker() as session, session.begin():
            await session.execute(
                update(table).where(table.c.tt_transaction_id == bindparam("_tt_transaction_id")).values(
                    position_id=bindparam("_position_id"), closed_position_id=bindparam("_closed_position_id"),
                ),
                updates,
            )
        return len(updates)

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

    async def get_position_id(self, account: str, security_id: str) -> int | None:
        table = models.Position.__table__
        async with self._sessionmaker() as session:
            row = (
                await session.execute(
                    select(table.c.id).where(table.c.account == account, table.c.security_id == security_id)
                )
            ).first()
        return row.id if row is not None else None

    async def get_positions(self, account: str) -> list[PositionRow]:
        table = models.Position.__table__
        async with self._sessionmaker() as session:
            rows = (await session.execute(select(table).where(table.c.account == account))).all()
        return [_mapping_to(PositionRow, r) for r in rows]

    async def get_closed_positions(self, account: str, security_id: str | None = None) -> list[ClosedPositionRow]:
        table = models.ClosedPosition.__table__
        stmt = select(table).where(table.c.account == account)
        if security_id is not None:
            stmt = stmt.where(table.c.security_id == security_id)
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [_mapping_to(ClosedPositionRow, r) for r in rows]

    async def get_latest_balance(self, account: str) -> BalanceSnapshotRow | None:
        table = models.BalanceSnapshot.__table__
        stmt = select(table).where(table.c.account == account).order_by(table.c.captured_at.desc()).limit(1)
        async with self._sessionmaker() as session:
            row = (await session.execute(stmt)).first()
        return _mapping_to(BalanceSnapshotRow, row) if row is not None else None

    async def get_balances(
        self, account: str, start: date | None = None, end: date | None = None,
    ) -> list[BalanceSnapshotRow]:
        table = models.BalanceSnapshot.__table__
        stmt = select(table).where(table.c.account == account)
        if start is not None:
            stmt = stmt.where(table.c.captured_at >= _day_start(start))
        if end is not None:
            stmt = stmt.where(table.c.captured_at < _day_start(end) + timedelta(days=1))
        stmt = stmt.order_by(table.c.captured_at.asc())
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [_mapping_to(BalanceSnapshotRow, r) for r in rows]

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

    async def get_group_transactions(self, trade_group_id: int) -> list[TxnRow]:
        table = models.Transaction.__table__
        stmt = select(table).where(table.c.trade_group_id == trade_group_id).order_by(table.c.executed_at.asc())
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
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
        if f.oms_order_id is not None:
            stmt = stmt.where(table.c.oms_order_id == f.oms_order_id)
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

    async def get_group_orders_with_ids(self, trade_group_id: int) -> list[tuple[int, OrderRow]]:
        table = models.Order.__table__
        stmt = (
            select(table)
            .where(table.c.trade_group_id == trade_group_id)
            .order_by(table.c.received_at.asc().nulls_last(), table.c.id.asc())
        )
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [(r.id, _mapping_to(OrderRow, r, origin=Origin, ingest=Ingest)) for r in rows]

    async def get_legs_for_orders(self, order_ids: list[int]) -> list[LegDetailRow]:
        if not order_ids:
            return []
        table = models.OrderLeg.__table__
        stmt = (
            select(table)
            .where(table.c.order_id.in_(order_ids))
            .order_by(table.c.order_id.asc(), table.c.leg_index.asc())
        )
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [_mapping_to(LegDetailRow, r) for r in rows]

    async def get_fills_for_orders(self, order_ids: list[int]) -> list[FillRow]:
        if not order_ids:
            return []
        table = models.OrderFill.__table__
        stmt = (
            select(table)
            .where(table.c.order_id.in_(order_ids))
            .order_by(table.c.filled_at.asc().nulls_last(), table.c.id.asc())
        )
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [_mapping_to(FillRow, r) for r in rows]

    async def query_transactions(self, q: TransactionQuery) -> tuple[list[TransactionDetailRow], int]:
        txns = models.Transaction.__table__
        orders = models.Order.__table__

        conditions = []
        if q.account is not None:
            conditions.append(txns.c.account == q.account)
        if q.accounts is not None:
            conditions.append(txns.c.account.in_(q.accounts))
        if q.start is not None:
            conditions.append(txns.c.executed_at >= _day_start(q.start))
        if q.end is not None:
            conditions.append(txns.c.executed_at < _day_start(q.end) + timedelta(days=1))
        if q.underlying is not None:
            conditions.append(txns.c.underlying == q.underlying)
        if q.transaction_type is not None:
            conditions.append(txns.c.transaction_type == q.transaction_type)
        if q.trade_group_id is not None:
            conditions.append(txns.c.trade_group_id == q.trade_group_id)

        cols = [
            *[c for c in txns.columns if c.name in {f.name for f in TransactionDetailRow.__dataclass_fields__.values()}],
            orders.c.signal_id.label("signal_id"),
        ]
        stmt = (
            select(*cols)
            .select_from(txns.outerjoin(orders, txns.c.order_id == orders.c.id))
            .where(*conditions)
            .order_by(txns.c.executed_at.desc().nulls_last(), txns.c.id.desc())
            .limit(q.limit)
            .offset(q.offset)
        )
        count_stmt = select(func.count()).select_from(txns).where(*conditions)
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
            total = (await session.execute(count_stmt)).scalar_one()
        return [_mapping_to(TransactionDetailRow, r) for r in rows], total

    async def get_open_position_groups(self, account: str | None = None) -> list[tuple[str, str, int]]:
        txns = models.Transaction.__table__
        groups = models.TradeGroup.__table__
        stmt = (
            select(txns.c.account, txns.c.security_id, txns.c.trade_group_id)
            .distinct()
            .select_from(txns.join(groups, txns.c.trade_group_id == groups.c.id))
            .where(groups.c.status == "open", txns.c.security_id.isnot(None))
        )
        if account is not None:
            stmt = stmt.where(txns.c.account == account)
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [(r.account, r.security_id, r.trade_group_id) for r in rows]

    async def net_open_by_group(self, trade_group_ids: list[int]) -> dict[int, dict[str, int]]:
        if not trade_group_ids:
            return {}
        txns = models.Transaction.__table__
        # Net open = Σ(opening qty) − Σ(closing qty) per (group, security_id). Only
        # trade actions move the count; settlements / corporate actions (NULL or
        # other actions) contribute 0, so a cash-settled leg's opening rows keep
        # the security present at a positive net (the caller's positions-gone path
        # confirms such a leg closed). Grouped in the DB over the indexed
        # trade_group_id FK — one round-trip regardless of how many groups.
        net = func.sum(
            case(
                (txns.c.action.like("%to Open"), txns.c.quantity),
                (txns.c.action.like("%to Close"), -txns.c.quantity),
                else_=0,
            )
        ).label("net_open")
        stmt = (
            select(txns.c.trade_group_id, txns.c.security_id, net)
            .where(
                txns.c.trade_group_id.in_(trade_group_ids),
                txns.c.security_id.isnot(None),
            )
            .group_by(txns.c.trade_group_id, txns.c.security_id)
        )
        async with self._sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        result: dict[int, dict[str, int]] = {}
        for r in rows:
            result.setdefault(r.trade_group_id, {})[r.security_id] = int(r.net_open or 0)
        return result

    async def account_activity(self, f: ActivityFilter) -> list[ActivityRow]:
        txns = models.Transaction.__table__
        orders = models.Order.__table__
        groups = models.TradeGroup.__table__
        cols = [
            *[c for c in txns.columns if c.name not in ("id", "created_at", "updated_at")],
            orders.c.origin.label("origin"),
            orders.c.trade_group_id.label("order_trade_group_id"),
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
