"""Push (stream) ingestion — real-time visibility (docs/ingestion.md).

``StreamConsumer`` consumes an injected ``MessageSource`` — an async iterator of the same
broker-native shapes ``pull.py`` already uses (``FillEvent`` for order/fill updates,
``BrokerPosition`` for live position updates, ``BalanceMessage`` for balances, which have no
schema home here and are only forwarded to an optional hook). This mirrors ``pull.py``'s
``BrokerClient`` Protocol / ``MockTastyTradeClient`` pattern: the *transport* is injected, not
hardcoded here -- ``TastyTradeMessageSource`` (``tastytrade_stream.py``) is the real broker
WebSocket, ``MockMessageSource`` is the test fake, and a host platform's ``acct:*`` Redis pub/sub
(see docs/integration-zts.md) would be a third. ``LedgerClient.stream_consumer()``/``tt-ledger
listen`` wire ``StreamConsumer`` to a real transport for standalone use.

The stream does NOT create order structure: a broker fill with no local order is a no-op —
the authoritative row (legs + fills) is created by ``pull.sync_orders``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, AsyncIterator, Callable, Protocol, runtime_checkable

from ..identity import PassthroughResolver
from ..repositories import PositionRepository, apply_fill_event
from ..rows import FillEvent
from .broker import BalanceMessage, BrokerPosition

if TYPE_CHECKING:
    from ..identity import AccountMapper, SecurityResolver
    from ..rows import OrderRow
    from ..store import LedgerStore


@runtime_checkable
class MessageSource(Protocol):
    """The transport seam: an async stream of broker-native messages. A real implementation
    (broker WebSocket, or the host platform's Redis pub/sub) translates its wire format into
    ``FillEvent`` / ``BrokerPosition`` / ``BalanceMessage`` and yields them here;
    ``MockMessageSource`` (``mock_broker.py``) is the test fake."""

    def messages(self) -> AsyncIterator["FillEvent | BrokerPosition | BalanceMessage"]: ...


class StreamConsumer:
    def __init__(
        self,
        store: "LedgerStore",
        source: "MessageSource",
        *,
        accounts: "AccountMapper",
        resolver: "SecurityResolver | None" = None,
        on_balance: "Callable[[BalanceMessage], None] | None" = None,
    ) -> None:
        self._store = store
        self._source = source
        self._accounts = accounts
        self._resolver: SecurityResolver = resolver or PassthroughResolver()
        self._on_balance = on_balance
        self._stopped: asyncio.Event | None = None

    def stop(self) -> None:
        """Signal ``run()`` to exit after its current message. A finite/exhausted source (e.g.
        a disconnected pub/sub, or the test fake) also ends ``run()`` on its own."""
        if self._stopped is not None:
            self._stopped.set()

    async def run(self) -> None:
        """Consume order/position/balance messages until stopped or the source is exhausted."""
        self._stopped = asyncio.Event()
        async for msg in self._source.messages():
            if self._stopped.is_set():
                break
            if isinstance(msg, FillEvent):
                await self.apply_fill(msg)
            elif isinstance(msg, BrokerPosition):
                await self._apply_position(msg)
            elif isinstance(msg, BalanceMessage):
                if self._on_balance is not None:
                    self._on_balance(msg)

    async def apply_fill(self, evt: "FillEvent") -> "OrderRow | None":
        """Update an existing order's fill status (never creates)."""
        return await apply_fill_event(self._store, evt)

    async def _apply_position(self, pos: "BrokerPosition") -> None:
        account = self._accounts.to_nickname(pos.account_number)
        await PositionRepository(self._store, resolver=self._resolver).upsert([pos], account=account)
