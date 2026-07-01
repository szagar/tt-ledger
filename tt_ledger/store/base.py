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
    async def upsert_orders(self, rows: list[OrderRow]) -> list[int]: ...     # tt_order_id; returns surrogate ids, input order
    async def upsert_legs(self, rows: list[LegRow]) -> list[int]: ...         # (order_id, leg_index); returns surrogate ids, input order
    async def upsert_fills(self, rows: list[FillRow]) -> None: ...            # fill_id
    async def upsert_transactions(self, rows: list[TxnRow]) -> None: ...      # tt_transaction_id
    async def upsert_security(self, sec: SecurityRow) -> None: ...            # security_id
    async def upsert_positions(self, rows: list[PositionRow]) -> None: ...    # (account, security_id)

    # --- linking + grouping ---
    async def link_transactions_to_orders(self, account: str) -> int: ...    # by tt_order_id
    async def upsert_trade_group(self, tg: TradeGroupRow) -> int: ...        # group_id; returns surrogate id
    async def add_trade_group_event(self, ev: EventRow) -> None: ...
    async def attach_transactions_to_trade_group(
        self, tt_transaction_ids: list[str], trade_group_id: int,
    ) -> int: ...                                                            # by tt_transaction_id; returns count updated
    async def move_transactions_to_group(
        self, txn_ids: list[int], trade_group_id: int | None,
    ) -> int: ...                                                            # by surrogate id (remap/regroup); returns count updated

    # --- reads (consolidated views, as methods) ---
    async def get_order(self, tt_order_id: str) -> OrderRow | None: ...
    async def get_position(self, account: str, security_id: str) -> PositionRow | None: ...
    async def get_security(self, security_id: str) -> SecurityRow | None: ...
    async def get_trade_group(self, group_id: str) -> TradeGroupRow | None: ...
    async def get_trade_group_by_id(self, trade_group_id: int) -> TradeGroupRow | None: ...
    async def get_trade_group_id(self, group_id: str) -> int | None: ...
    async def get_transactions_by_id(self, txn_ids: list[int]) -> list[TxnRow]: ...
    async def query_orders(self, f: OrderFilter) -> list[OrderRow]: ...
    async def unified_trades(self, f: TradeFilter) -> list[TradeRow]: ...
    async def account_activity(self, f: ActivityFilter) -> list[ActivityRow]: ...
