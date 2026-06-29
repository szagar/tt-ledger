"""``InMemoryStore`` — pure-Python LedgerStore fake for fast unit tests (docs/storage.md).

No database; dicts keyed on the same natural keys as the SQL store. Lets repositories,
ingest, and reconcile be tested without a backend. Methods are stubs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..rows import (
        ActivityFilter, ActivityRow, EventRow, FillRow, LegRow, OrderFilter, OrderRow,
        PositionRow, SecurityRow, TradeFilter, TradeGroupRow, TradeRow, TxnRow,
    )


class InMemoryStore:
    def __init__(self) -> None:
        self.orders: dict[str, "OrderRow"] = {}            # by tt_order_id
        self.fills: dict[str, "FillRow"] = {}              # by fill_id
        self.transactions: dict[str, "TxnRow"] = {}        # by tt_transaction_id
        self.securities: dict[str, "SecurityRow"] = {}     # by security_id
        self.positions: dict[tuple[str, str], "PositionRow"] = {}   # by (account, security_id)
        self.trade_groups: dict[str, "TradeGroupRow"] = {}  # by group_id
        self.events: list["EventRow"] = []
        self.legs: list["LegRow"] = []

    async def upsert_orders(self, rows: "list[OrderRow]") -> None: raise NotImplementedError
    async def upsert_legs(self, rows: "list[LegRow]") -> None: raise NotImplementedError
    async def upsert_fills(self, rows: "list[FillRow]") -> None: raise NotImplementedError
    async def upsert_transactions(self, rows: "list[TxnRow]") -> None: raise NotImplementedError
    async def upsert_security(self, sec: "SecurityRow") -> None: raise NotImplementedError
    async def upsert_positions(self, rows: "list[PositionRow]") -> None: raise NotImplementedError
    async def link_transactions_to_orders(self, account: str) -> int: raise NotImplementedError
    async def upsert_trade_group(self, tg: "TradeGroupRow") -> None: raise NotImplementedError
    async def add_trade_group_event(self, ev: "EventRow") -> None: raise NotImplementedError
    async def get_trade_group(self, group_id: str) -> "TradeGroupRow | None": raise NotImplementedError
    async def query_orders(self, f: "OrderFilter") -> "list[OrderRow]": raise NotImplementedError
    async def unified_trades(self, f: "TradeFilter") -> "list[TradeRow]": raise NotImplementedError
    async def account_activity(self, f: "ActivityFilter") -> "list[ActivityRow]": raise NotImplementedError
