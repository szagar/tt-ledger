"""``InMemoryStore`` — pure-Python LedgerStore fake for fast unit tests (docs/storage.md).

No database; each entity lives in a small ``_Table`` (dict + natural-key index), mirroring the
SQL store's conflict keys exactly so repository/ingest/reconcile tests behave identically against
either backend. ``order_id`` / ``order_leg_id`` / ``trade_group_id`` / ``transaction_id`` cross-refs
use the same auto-increment surrogate ids the SQL schema would assign (docs/schema.md); like the SQL
store, those ids are not exposed through the read model — tests reach into ``_Table`` directly, the
same way the SQL test suite queries the underlying tables directly.
"""

from __future__ import annotations

from dataclasses import fields as dc_fields
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Callable, Generic, TypeVar

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

T = TypeVar("T")


class _Table(Generic[T]):
    """An auto-increment id + optional natural-key index — one per entity, like a SQL table.

    ``key(row)`` returning ``None`` means "no natural key for this row" (e.g. an order with no
    ``tt_order_id`` yet): such rows always insert and never conflict, matching the SQL store's
    partial-unique-index behavior.
    """

    def __init__(self, key: Callable[[T], Any | None] | None = None) -> None:
        self._rows: dict[int, T] = {}
        self._index: dict[Any, int] = {}
        self._next_id = 1
        self._key = key

    def upsert(self, row: T) -> int:
        key = self._key(row) if self._key else None
        if key is not None and key in self._index:
            row_id = self._index[key]
            self._rows[row_id] = row
            return row_id
        row_id = self._next_id
        self._next_id += 1
        self._rows[row_id] = row
        if key is not None:
            self._index[key] = row_id
        return row_id

    def insert(self, row: T) -> int:
        row_id = self._next_id
        self._next_id += 1
        self._rows[row_id] = row
        return row_id

    def get(self, row_id: int) -> T | None:
        return self._rows.get(row_id)

    def get_by_key(self, key: Any) -> T | None:
        row_id = self._index.get(key)
        return self._rows.get(row_id) if row_id is not None else None

    def id_of(self, key: Any) -> int | None:
        return self._index.get(key)

    def all(self) -> list[tuple[int, T]]:
        return list(self._rows.items())


def _project(src: Any, cls: type[T], **overrides: Any) -> T:
    """``src`` (any object with matching attributes) -> a ``cls`` dataclass, field-by-field."""
    data = {f.name: getattr(src, f.name) for f in dc_fields(cls) if hasattr(src, f.name)}
    data.update(overrides)
    return cls(**data)


def _in_range(value: datetime | None, start, end) -> bool:
    if value is None:
        return start is None and end is None
    if start is not None and value.date() < start:
        return False
    if end is not None and value.date() > end:
        return False
    return True


class InMemoryStore:
    def __init__(self) -> None:
        self._accounts: _Table[AccountRow] = _Table(key=lambda r: r.nickname)
        self._orders: _Table[OrderRow] = _Table(key=lambda r: r.tt_order_id)
        self._legs: _Table[LegRow] = _Table(key=lambda r: (r.order_id, r.leg_index))
        self._fills: _Table[FillRow] = _Table(key=lambda r: r.fill_id)
        self._transactions: _Table[TxnRow] = _Table(key=lambda r: r.tt_transaction_id)
        self._securities: _Table[SecurityRow] = _Table(key=lambda r: r.security_id)
        self._positions: _Table[PositionRow] = _Table(key=lambda r: (r.account, r.security_id))
        self._closed_positions: _Table[ClosedPositionRow] = _Table(
            key=lambda r: (r.account, r.security_id, r.opened_at, r.closed_at)
        )
        self._trade_groups: _Table[TradeGroupRow] = _Table(key=lambda r: r.group_id)
        self._events: _Table[EventRow] = _Table()
        self._balance_snapshots: _Table[BalanceSnapshotRow] = _Table(
            key=lambda r: (r.account, r.captured_at, r.source)
        )

    # --- writes ------------------------------------------------------------------

    async def upsert_account(self, row: AccountRow) -> None:
        self._accounts.upsert(row)

    async def upsert_orders(self, rows: list[OrderRow]) -> list[int]:
        return [self._orders.upsert(row) for row in rows]

    async def upsert_legs(self, rows: list[LegRow]) -> list[int]:
        return [self._legs.upsert(row) for row in rows]

    async def upsert_fills(self, rows: list[FillRow]) -> None:
        for row in rows:
            self._fills.upsert(row)

    async def upsert_transactions(self, rows: list[TxnRow]) -> None:
        for row in rows:
            # mirror the SQL store's preserve-if-null on linkage columns: a re-synced/re-imported
            # transaction must not wipe the order/position/trade-group linkage reconcile+replay set.
            existing = self._transactions.get_by_key(row.tt_transaction_id)
            if existing is not None:
                for f in ("order_id", "order_leg_id", "position_id", "closed_position_id", "trade_group_id"):
                    if getattr(row, f, None) is None:
                        setattr(row, f, getattr(existing, f, None))
            self._transactions.upsert(row)

    async def upsert_security(self, sec: SecurityRow) -> None:
        self._securities.upsert(sec)

    async def upsert_positions(self, rows: list[PositionRow]) -> None:
        for row in rows:
            self._positions.upsert(row)

    async def upsert_closed_position(self, row: ClosedPositionRow) -> int:
        return self._closed_positions.upsert(row)

    async def upsert_balance_snapshot(self, row: BalanceSnapshotRow) -> None:
        self._balance_snapshots.upsert(row)

    # --- linking + grouping --------------------------------------------------------

    async def link_transactions_to_orders(self, account: str) -> int:
        linked = 0
        for _, txn in self._transactions.all():
            if txn.account != account or txn.order_id is not None or txn.tt_order_id is None:
                continue
            order = self._orders.get_by_key(txn.tt_order_id)
            if order is None or order.account != account:
                continue
            txn.order_id = self._orders.id_of(txn.tt_order_id)
            linked += 1
        return linked

    async def link_orders_to_groups(self, account: str) -> int:
        by_order: dict[str, set[int]] = {}
        for _, txn in self._transactions.all():
            if txn.account == account and txn.tt_order_id and txn.trade_group_id is not None:
                by_order.setdefault(txn.tt_order_id, set()).add(txn.trade_group_id)
        linked = 0
        for _, order in self._orders.all():
            if order.account != account or order.trade_group_id is not None or not order.tt_order_id:
                continue
            groups = by_order.get(order.tt_order_id)
            if groups is not None and len(groups) == 1:
                order.trade_group_id = next(iter(groups))
                linked += 1
        return linked

    async def link_transactions_to_positions(self, links: list[tuple[str, int | None, int | None]]) -> int:
        by_tt_transaction_id = {tt_transaction_id: (position_id, closed_position_id) for tt_transaction_id, position_id, closed_position_id in links}
        linked = 0
        for _, txn in self._transactions.all():
            if txn.tt_transaction_id not in by_tt_transaction_id:
                continue
            txn.position_id, txn.closed_position_id = by_tt_transaction_id[txn.tt_transaction_id]
            linked += 1
        return linked

    async def upsert_trade_group(self, tg: TradeGroupRow) -> int:
        return self._trade_groups.upsert(tg)

    async def add_trade_group_event(self, ev: EventRow) -> None:
        if ev.event_at is None:
            ev.event_at = datetime.now(UTC)
        self._events.insert(ev)

    async def attach_transactions_to_trade_group(self, tt_transaction_ids: list[str], trade_group_id: int) -> int:
        ids = set(tt_transaction_ids)
        attached = 0
        for _, txn in self._transactions.all():
            if txn.tt_transaction_id in ids:
                txn.trade_group_id = trade_group_id
                attached += 1
        return attached

    async def move_transactions_to_group(self, txn_ids: list[int], trade_group_id: int | None) -> int:
        ids = set(txn_ids)
        moved = 0
        for row_id, txn in self._transactions.all():
            if row_id in ids:
                txn.trade_group_id = trade_group_id
                moved += 1
        return moved

    # --- reads (consolidated views, as methods) -----------------------------------

    async def get_order(self, tt_order_id: str) -> OrderRow | None:
        return self._orders.get_by_key(tt_order_id)

    async def get_position(self, account: str, security_id: str) -> PositionRow | None:
        return self._positions.get_by_key((account, security_id))

    async def get_position_id(self, account: str, security_id: str) -> int | None:
        return self._positions.id_of((account, security_id))

    async def get_positions(self, account: str) -> list[PositionRow]:
        return [row for _, row in self._positions.all() if row.account == account]

    async def get_closed_positions(self, account: str, security_id: str | None = None) -> list[ClosedPositionRow]:
        return [
            row for _, row in self._closed_positions.all()
            if row.account == account and (security_id is None or row.security_id == security_id)
        ]

    async def get_latest_balance(self, account: str) -> BalanceSnapshotRow | None:
        rows = [row for _, row in self._balance_snapshots.all() if row.account == account]
        return max(rows, key=lambda r: r.captured_at) if rows else None

    async def get_balances(
        self, account: str, start: date | None = None, end: date | None = None,
    ) -> list[BalanceSnapshotRow]:
        def _in_range(row: BalanceSnapshotRow) -> bool:
            if start is not None and row.captured_at < datetime.combine(start, time.min, tzinfo=UTC):
                return False
            if end is not None and row.captured_at >= datetime.combine(end, time.min, tzinfo=UTC) + timedelta(days=1):
                return False
            return True

        rows = [row for _, row in self._balance_snapshots.all() if row.account == account and _in_range(row)]
        return sorted(rows, key=lambda r: r.captured_at)

    async def get_security(self, security_id: str) -> SecurityRow | None:
        return self._securities.get_by_key(security_id)

    async def get_trade_group(self, group_id: str) -> TradeGroupRow | None:
        return self._trade_groups.get_by_key(group_id)

    async def get_trade_group_by_id(self, trade_group_id: int) -> TradeGroupRow | None:
        return self._trade_groups.get(trade_group_id)

    async def get_trade_group_id(self, group_id: str) -> int | None:
        return self._trade_groups.id_of(group_id)

    async def get_transactions_by_id(self, txn_ids: list[int]) -> list[TxnRow]:
        ids = set(txn_ids)
        return [txn for row_id, txn in self._transactions.all() if row_id in ids]

    async def get_group_transactions(self, trade_group_id: int) -> list[TxnRow]:
        rows = [txn for _, txn in self._transactions.all() if txn.trade_group_id == trade_group_id]
        return sorted(rows, key=lambda t: (t.executed_at is None, t.executed_at))

    async def net_open_by_group(self, trade_group_ids: list[int]) -> dict[int, dict[str, int]]:
        wanted = set(trade_group_ids)
        result: dict[int, dict[str, int]] = {}
        for _, txn in self._transactions.all():
            if txn.trade_group_id not in wanted or txn.security_id is None:
                continue
            action = (txn.action or "").strip()
            if action.endswith("to Open"):
                delta = int(txn.quantity or 0)
            elif action.endswith("to Close"):
                delta = -int(txn.quantity or 0)
            else:
                delta = 0
            group = result.setdefault(txn.trade_group_id, {})
            group[txn.security_id] = group.get(txn.security_id, 0) + delta
        return result

    async def query_orders(self, f: OrderFilter) -> list[OrderRow]:
        out = []
        for _, row in self._orders.all():
            if f.origin is not None and row.origin != f.origin:
                continue
            if f.account is not None and row.account != f.account:
                continue
            if f.status is not None and row.oms_status != f.status:
                continue
            if f.underlying is not None and row.underlying != f.underlying:
                continue
            if f.trade_group_id is not None and row.trade_group_id != f.trade_group_id:
                continue
            if f.oms_order_id is not None and row.oms_order_id != f.oms_order_id:
                continue
            if not _in_range(row.submitted_at, f.start, f.end):
                continue
            out.append(row)
        return out

    async def unified_trades(self, f: TradeFilter) -> list[TradeRow]:
        out = []
        for _, tg in self._trade_groups.all():
            if f.origin is not None and tg.origin != f.origin:
                continue
            if f.review_status is not None and tg.review_status != f.review_status:
                continue
            if f.status is not None and tg.status != f.status:
                continue
            if f.account is not None and tg.account != f.account:
                continue
            if f.underlying is not None and tg.underlying != f.underlying:
                continue
            if not _in_range(tg.executed_at, f.start, f.end):
                continue
            out.append(_project(tg, TradeRow))
        return out

    async def get_group_orders_with_ids(self, trade_group_id: int) -> list[tuple[int, OrderRow]]:
        rows = [
            (row_id, row) for row_id, row in self._orders.all() if row.trade_group_id == trade_group_id
        ]
        rows.sort(key=lambda pair: (pair[1].received_at is None, pair[1].received_at, pair[0]))
        return rows

    async def get_legs_for_orders(self, order_ids: list[int]) -> list[LegDetailRow]:
        wanted = set(order_ids)
        rows = [
            _project(leg, LegDetailRow, id=row_id)
            for row_id, leg in self._legs.all()
            if leg.order_id in wanted
        ]
        return sorted(rows, key=lambda r: (r.order_id, r.leg_index))

    async def get_fills_for_orders(self, order_ids: list[int]) -> list[FillRow]:
        wanted = set(order_ids)
        rows = [
            (row_id, fill) for row_id, fill in self._fills.all() if fill.order_id in wanted
        ]
        rows.sort(key=lambda pair: (pair[1].filled_at is None, pair[1].filled_at, pair[0]))
        return [fill for _, fill in rows]

    async def query_transactions(self, q: TransactionQuery) -> tuple[list[TransactionDetailRow], int]:
        matched: list[tuple[int, TxnRow]] = []
        for row_id, txn in self._transactions.all():
            if q.account is not None and txn.account != q.account:
                continue
            if q.accounts is not None and txn.account not in q.accounts:
                continue
            if not _in_range(txn.executed_at, q.start, q.end):
                continue
            if q.underlying is not None and txn.underlying != q.underlying:
                continue
            if q.transaction_type is not None and txn.transaction_type != q.transaction_type:
                continue
            if q.trade_group_id is not None and txn.trade_group_id != q.trade_group_id:
                continue
            matched.append((row_id, txn))
        # newest first; NULL executed_at sorts last (mirrors the SQL store's NULLS LAST)
        matched.sort(
            key=lambda pair: (
                pair[1].executed_at is None,
                -(pair[1].executed_at.timestamp() if pair[1].executed_at else 0),
                -pair[0],
            )
        )
        total = len(matched)
        page = matched[q.offset : q.offset + q.limit]
        out = []
        for row_id, txn in page:
            order = self._orders.get(txn.order_id) if txn.order_id is not None else None
            out.append(
                _project(txn, TransactionDetailRow, id=row_id,
                         signal_id=order.signal_id if order is not None else None)
            )
        return out, total

    async def get_open_position_groups(self, account: str | None = None) -> list[tuple[str, str, int]]:
        seen: set[tuple[str, str, int]] = set()
        for _, txn in self._transactions.all():
            if txn.trade_group_id is None or txn.security_id is None:
                continue
            if account is not None and txn.account != account:
                continue
            group = self._trade_groups.get(txn.trade_group_id)
            if group is None or group.status != "open":
                continue
            seen.add((txn.account, txn.security_id, txn.trade_group_id))
        return sorted(seen)

    async def account_activity(self, f: ActivityFilter) -> list[ActivityRow]:
        out = []
        for _, txn in self._transactions.all():
            if f.account is not None and txn.account != f.account:
                continue
            if not _in_range(txn.executed_at, f.start, f.end):
                continue
            if f.unreconciled_only and txn.order_id is not None:
                continue
            order = self._orders.get(txn.order_id) if txn.order_id is not None else None
            group = self._trade_groups.get(txn.trade_group_id) if txn.trade_group_id is not None else None
            out.append(
                _project(
                    txn, ActivityRow,
                    origin=order.origin if order is not None else None,
                    order_trade_group_id=order.trade_group_id if order is not None else None,
                    review_status=group.review_status if group is not None else None,
                )
            )
        return out
