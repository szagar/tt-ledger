"""``LedgerClient`` — the in-process Python API (docs/api.md).

The canonical entry point. Accepts nicknames + security_id only (Rule 1/Rule 2). The HTTP
server (tt_ledger.api) and CLI (tt_ledger.cli) are thin wrappers over this.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from .identity import PassthroughResolver
from .store import make_store

if TYPE_CHECKING:
    from .identity import AccountMapper, SecurityResolver
    from .rows import ActivityRow, OrderRow, SyncResult, TradeRow
    from .store import LedgerStore


class LedgerClient:
    def __init__(
        self,
        store: "LedgerStore",
        *,
        accounts: "AccountMapper",
        resolver: "SecurityResolver | None" = None,
    ) -> None:
        self._store = store
        self._accounts = accounts
        # Injectable symbology. Default: canonical security_id == the raw vendor symbol.
        self._resolver: SecurityResolver = resolver or PassthroughResolver()

    @classmethod
    def open(
        cls,
        url: str = "sqlite+aiosqlite:///ledger.db",
        *,
        accounts: "AccountMapper",
        resolver: "SecurityResolver | None" = None,
    ) -> "LedgerClient":
        """Open a ledger on ``url`` (SQLite default, Postgres opt-in).

        ``resolver`` translates broker symbols to your canonical ``security_id``; if omitted,
        the vendor symbol is used as the canonical id (PassthroughResolver).
        """
        return cls(make_store(url), accounts=accounts, resolver=resolver)

    # --- capture ---
    async def sync(self, account: str, since: date | None = None) -> "SyncResult":
        """Pull (orders + transactions + positions) then reconcile. TODO."""
        raise NotImplementedError

    async def record_order(self, order) -> "OrderRow":  # noqa: ANN001  (OrderInput)
        raise NotImplementedError

    async def apply_fill(self, evt) -> None:  # noqa: ANN001  (FillEvent)
        raise NotImplementedError

    # --- read (consolidated views) ---
    async def orders(self, **f) -> "list[OrderRow]": raise NotImplementedError
    async def trades(self, **f) -> "list[TradeRow]": raise NotImplementedError
    async def trade(self, group_id: str) -> "TradeRow | None": raise NotImplementedError
    async def account_activity(self, account: str, **f) -> "list[ActivityRow]": raise NotImplementedError

    # --- remap ---
    async def remap_trade(self, group_id: str, *, strategy=None, bot=None, signal=None,  # noqa: ANN001
                          strategy_type=None, reviewed_by: str) -> "TradeRow":
        raise NotImplementedError

    async def regroup(self, txn_ids: list[int], *, target: str | None, reviewed_by: str) -> "list[TradeRow]":
        raise NotImplementedError

    async def dismiss_trade(self, group_id: str, *, reviewed_by: str) -> "TradeRow":
        raise NotImplementedError

    async def close(self) -> None:
        dispose = getattr(self._store, "dispose", None)
        if dispose is not None:
            await dispose()
