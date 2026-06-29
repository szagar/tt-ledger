"""Pull (REST) ingestion — idempotent (docs/ingestion.md).

The broker client (``httpx``/``websockets``, the ``[tastytrade]`` extra) is imported LAZILY
inside the functions, so the base package imports without that extra installed.

Requires a broker ``get_order_history(account, start, end)`` method (GET /accounts/{a}/orders,
page-offset) returning orders with ``legs[]`` and per-leg ``fills[]`` — add it to the client.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..rows import SyncResult
    from ..store import LedgerStore


async def sync_orders(store: "LedgerStore", account: str, *, since: date | None = None) -> int:
    """Upsert orders on tt_order_id, legs, and fills on fill_id. One importer, both origins:
    enrich existing origin=zts rows (fill/status only), create broker rows. TODO: implement."""
    raise NotImplementedError


async def sync_transactions(store: "LedgerStore", account: str, *, since: date | None = None) -> int:
    """Upsert transactions on tt_transaction_id; capture the broker order-id into tt_order_id. TODO."""
    raise NotImplementedError


async def sync_positions(store: "LedgerStore", account: str) -> int:
    """Upsert positions on (account, security_id). TODO: implement."""
    raise NotImplementedError


async def sync_all(store: "LedgerStore", account: str, *, since: date | None = None) -> "SyncResult":
    """orders -> transactions -> positions, then reconcile. TODO: implement."""
    raise NotImplementedError
