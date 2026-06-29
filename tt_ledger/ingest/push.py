"""Push (stream) ingestion — real-time visibility (docs/ingestion.md).

Standalone: connect the broker account-stream WebSocket directly (``[tastytrade]`` extra).
Host-platform: consume the existing ``acct:order`` / ``acct:position`` / ``acct:balance`` Redis
pub/sub (``[redis]`` extra). Both deps are imported LAZILY inside the methods.

The stream does NOT create order structure: a broker fill with no local order is published for
visibility but the authoritative row (legs + fills) is created by ``pull.sync_orders``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..rows import FillEvent
    from ..store import LedgerStore


class StreamConsumer:
    def __init__(self, store: "LedgerStore") -> None:
        self._store = store

    async def run(self) -> None:
        """Consume order/position/balance messages until stopped. TODO: implement."""
        raise NotImplementedError

    async def apply_fill(self, evt: "FillEvent") -> None:
        """Update an existing order's fill status (never creates). TODO: implement."""
        raise NotImplementedError
