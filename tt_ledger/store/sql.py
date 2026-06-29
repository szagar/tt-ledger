"""``SqlLedgerStore`` — async SQLAlchemy implementation of LedgerStore (docs/storage.md).

Backend is chosen entirely by the connection URL:
  * ``sqlite+aiosqlite:///ledger.db``  (bundled default)
  * ``postgresql+asyncpg://…``          ([postgres] extra)

The ONLY dialect branch is the upsert helper (``_insert`` below) — everything else is
dialect-agnostic. Methods are stubs; implement per docs/schema.md.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

if TYPE_CHECKING:
    from ..rows import (
        ActivityFilter, ActivityRow, EventRow, FillRow, LegRow, OrderFilter, OrderRow,
        PositionRow, SecurityRow, TradeFilter, TradeGroupRow, TradeRow, TxnRow,
    )


def _insert(dialect: str):
    """Return the dialect-specific ``insert`` (the one place we branch)."""
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:  # sqlite and other ON CONFLICT dialects
        from sqlalchemy.dialects.sqlite import insert
    return insert


class SqlLedgerStore:
    def __init__(self, url: str = "sqlite+aiosqlite:///ledger.db") -> None:
        self._engine = create_async_engine(url, pool_pre_ping=True)
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False)
        self._dialect = self._engine.dialect.name

    async def create_all(self) -> None:
        """Dev/standalone convenience (prod uses Alembic). TODO: implement."""
        raise NotImplementedError

    async def dispose(self) -> None:
        await self._engine.dispose()

    # --- writes ---
    async def upsert_orders(self, rows: "list[OrderRow]") -> None: raise NotImplementedError
    async def upsert_legs(self, rows: "list[LegRow]") -> None: raise NotImplementedError
    async def upsert_fills(self, rows: "list[FillRow]") -> None: raise NotImplementedError
    async def upsert_transactions(self, rows: "list[TxnRow]") -> None: raise NotImplementedError
    async def upsert_security(self, sec: "SecurityRow") -> None: raise NotImplementedError
    async def upsert_positions(self, rows: "list[PositionRow]") -> None: raise NotImplementedError

    # --- linking + grouping ---
    async def link_transactions_to_orders(self, account: str) -> int: raise NotImplementedError
    async def upsert_trade_group(self, tg: "TradeGroupRow") -> None: raise NotImplementedError
    async def add_trade_group_event(self, ev: "EventRow") -> None: raise NotImplementedError

    # --- reads ---
    async def get_trade_group(self, group_id: str) -> "TradeGroupRow | None": raise NotImplementedError
    async def query_orders(self, f: "OrderFilter") -> "list[OrderRow]": raise NotImplementedError
    async def unified_trades(self, f: "TradeFilter") -> "list[TradeRow]": raise NotImplementedError
    async def account_activity(self, f: "ActivityFilter") -> "list[ActivityRow]": raise NotImplementedError
