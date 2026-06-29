"""The ``LedgerStore`` Protocol — the typed persistence seam (docs/storage.md).

Repositories depend on this Protocol, not on SQLAlchemy. One SQL implementation
(``SqlLedgerStore``) covers SQLite + Postgres; ``InMemoryStore`` is the test fake.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

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


@runtime_checkable
class LedgerStore(Protocol):
    # --- idempotent writes (conflict key in comments) ---
    async def upsert_orders(self, rows: list[OrderRow]) -> None: ...          # tt_order_id
    async def upsert_legs(self, rows: list[LegRow]) -> None: ...
    async def upsert_fills(self, rows: list[FillRow]) -> None: ...            # fill_id
    async def upsert_transactions(self, rows: list[TxnRow]) -> None: ...      # tt_transaction_id
    async def upsert_security(self, sec: SecurityRow) -> None: ...            # security_id
    async def upsert_positions(self, rows: list[PositionRow]) -> None: ...    # (account, security_id)

    # --- linking + grouping ---
    async def link_transactions_to_orders(self, account: str) -> int: ...    # by tt_order_id
    async def upsert_trade_group(self, tg: TradeGroupRow) -> None: ...
    async def add_trade_group_event(self, ev: EventRow) -> None: ...

    # --- reads (consolidated views, as methods) ---
    async def get_trade_group(self, group_id: str) -> TradeGroupRow | None: ...
    async def query_orders(self, f: OrderFilter) -> list[OrderRow]: ...
    async def unified_trades(self, f: TradeFilter) -> list[TradeRow]: ...
    async def account_activity(self, f: ActivityFilter) -> list[ActivityRow]: ...
