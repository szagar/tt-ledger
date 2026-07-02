"""Push (stream) ingestion — real-time visibility (docs/ingestion.md).

``StreamConsumer`` consumes an injected ``MessageSource`` — an async iterator of the same
broker-native shapes ``pull.py`` already uses (``FillEvent`` for order/fill updates,
``BrokerPosition`` for live position updates, ``BalanceMessage`` for balances, persisted
throttled to ``balance_snapshots`` and forwarded to an optional hook). This mirrors ``pull.py``'s
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
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, AsyncIterator, Callable, Protocol, runtime_checkable

from ..identity import PassthroughResolver
from ..repositories import BalanceRepository, PositionRepository, apply_fill_event, ensure_account
from ..rows import FillEvent
from .broker import BalanceMessage, BrokerPosition

if TYPE_CHECKING:
    from ..identity import AccountMapper, SecurityResolver
    from ..rows import OrderRow
    from ..store import LedgerStore

logger = logging.getLogger(__name__)


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
        persist_balances: bool = True,
        balance_min_interval_seconds: float = 60.0,
    ) -> None:
        self._store = store
        self._source = source
        self._accounts = accounts
        self._resolver: SecurityResolver = resolver or PassthroughResolver()
        self._on_balance = on_balance
        self._persist_balances = persist_balances
        self._balance_min_interval = timedelta(seconds=balance_min_interval_seconds)
        # per-account throttle state: (last persisted instant, last persisted NLV)
        self._balance_last: dict[str, tuple[datetime, Decimal | None]] = {}
        self._ensured_accounts: set[str] = set()
        self._stopped: asyncio.Event | None = None

    def stop(self) -> None:
        """Signal ``run()`` to exit after its current message. A finite/exhausted source (e.g.
        a disconnected pub/sub, or the test fake) also ends ``run()`` on its own."""
        if self._stopped is not None:
            self._stopped.set()

    async def run(self) -> None:
        """Consume order/position/balance messages until stopped or the source is exhausted.

        A failure applying ONE message is logged and skipped, not fatal — a daemon stream
        shouldn't die on a single malformed/unmappable update. (The source's own connection
        errors still propagate; reconnecting is the transport's job.)"""
        self._stopped = asyncio.Event()
        logger.info("stream consumer started")
        async for msg in self._source.messages():
            if self._stopped.is_set():
                break
            try:
                if isinstance(msg, FillEvent):
                    await self.apply_fill(msg)
                elif isinstance(msg, BrokerPosition):
                    await self._apply_position(msg)
                elif isinstance(msg, BalanceMessage):
                    await self._apply_balance(msg)
            except Exception:  # noqa: BLE001 - one bad message must not kill the daemon stream
                logger.exception("failed to apply stream message %r", msg)
        logger.info("stream consumer stopped")

    async def apply_fill(self, evt: "FillEvent") -> "OrderRow | None":
        """Update an existing order's fill status (never creates)."""
        return await apply_fill_event(self._store, evt)

    async def _apply_position(self, pos: "BrokerPosition") -> None:
        account = self._accounts.to_nickname(pos.account_number)
        await ensure_account(self._store, self._accounts, account, self._ensured_accounts)
        await PositionRepository(self._store, resolver=self._resolver).upsert([pos], account=account)

    async def _apply_balance(self, msg: "BalanceMessage") -> None:
        """Persist a throttled balance time series, then forward to the optional hook.

        Balances stream chattily during fills, so persistence is capped at one row per account
        per ``balance_min_interval_seconds`` — EXCEPT when net-liquidating-value changed since
        the last persisted row (a material update is never dropped)."""
        if self._persist_balances:
            account = self._accounts.to_nickname(msg.account_number)
            await ensure_account(self._store, self._accounts, account, self._ensured_accounts)
            now = msg.captured_at or datetime.now(UTC)
            last = self._balance_last.get(account)
            due = last is None or (now - last[0]) >= self._balance_min_interval
            nlv_changed = last is not None and msg.net_liquidating_value != last[1]
            if due or nlv_changed:
                await BalanceRepository(self._store).record(msg, account=account, source="stream")
                self._balance_last[account] = (now, msg.net_liquidating_value)
        if self._on_balance is not None:
            self._on_balance(msg)
