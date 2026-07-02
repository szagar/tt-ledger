"""``TastyTradeMessageSource`` — the real WebSocket ``MessageSource`` (docs/ingestion.md → Push).

Requires the ``[tastytrade]`` extra (``websockets``), imported LAZILY inside ``__init__`` — same
pattern as ``TastyTradeClient`` (``httpx``) — so ``import tt_ledger.ingest`` works without it.

Protocol verified against developer.tastytrade.com's "Streaming Account Data" guide: the
account-streamer is a plain JSON-over-WebSocket protocol (**not** the DXLink binary protocol used
for market data) at ``wss://streamer.tastyworks.com`` (production) / ``wss://streamer.cert.
tastyworks.com`` (sandbox). The client sends ``{"action": "connect", "value": [account numbers],
"auth-token": <the same OAuth access token used for REST>, "request-id": N}`` once, then
``{"action": "heartbeat", "auth-token": ..., "request-id": N}`` every 2s-1m to keep the connection
alive, and receives ``{"type": "Order", "data": {...same shape as the REST Order object...},
"timestamp": ...}``-style push notifications with no further request needed.

One piece is inferred, not confirmed: the docs' own "Notification Nuances" section (which would
presumably list every notification ``type`` and any per-type quirks) is rendered from a separate
CMS entry that isn't reachable through static scraping, so it never yielded content. The doc prose
says notifications cover "orders, balances, and positions" and the one worked example uses
``type: "Order"`` (matching the Order object's own name) — so ``"CurrentPosition"`` and
``"AccountBalance"`` (the exact names TastyTrade's own OpenAPI defs use for those objects) are a
reasonable, but not directly verified, guess for the other two `type` values. Anything else is
ignored rather than guessed at further (e.g. public-watchlist/quote-alert notifications, which
tt-ledger has no use for anyway).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import TYPE_CHECKING, AsyncIterator, Awaitable, Callable

from ..enums import OrderStatus
from ..repositories import map_order_status, order_level_fill_fields
from ..rows import FillEvent
from .broker import BalanceMessage, BrokerPosition
from .tastytrade_client import order_from_json, position_from_json

if TYPE_CHECKING:
    from .tastytrade_client import TastyTradeClient

STREAMER_PRODUCTION_URL = "wss://streamer.tastyworks.com"
STREAMER_SANDBOX_URL = "wss://streamer.cert.tastyworks.com"  # aka "cert" -- use for initial testing

_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30.0  # docs say every 2s-1m; 30s is comfortably inside that


class TastyTradeStreamError(Exception):
    """The account-streamer rejected a connect/heartbeat action, or closed unexpectedly."""


class TastyTradeMessageSource:
    """Implements the ``MessageSource`` Protocol (``ingest/push.py``) against the real TastyTrade
    account-streamer. One connection can subscribe to multiple accounts at once (the ``connect``
    action's ``value`` is a list) — pass every account number this login should watch."""

    def __init__(
        self,
        *,
        access_token_provider: "Callable[[], Awaitable[str]]",
        account_numbers: str | list[str],
        url: str = STREAMER_PRODUCTION_URL,
        heartbeat_interval: float = _DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        try:
            import websockets  # noqa: F401 -- import only to fail fast & clearly if missing
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError(
                "The account streamer needs the [tastytrade] extra: pip install tt-ledger[tastytrade]"
            ) from exc

        self._access_token_provider = access_token_provider
        self._account_numbers = [account_numbers] if isinstance(account_numbers, str) else list(account_numbers)
        self._url = url
        self._heartbeat_interval = heartbeat_interval
        self._next_request_id = 0

    @classmethod
    def from_client(
        cls, client: "TastyTradeClient", account_numbers: str | list[str], *,
        url: str = STREAMER_PRODUCTION_URL, heartbeat_interval: float = _DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> "TastyTradeMessageSource":
        """Build a source that reuses a ``TastyTradeClient``'s auto-refreshed OAuth token —
        the account-streamer authenticates with the exact same access token as the REST API."""
        return cls(
            access_token_provider=client.access_token, account_numbers=account_numbers,
            url=url, heartbeat_interval=heartbeat_interval,
        )

    def _request_id(self) -> int:
        self._next_request_id += 1
        return self._next_request_id

    async def messages(self) -> "AsyncIterator[FillEvent | BrokerPosition | BalanceMessage]":
        import websockets

        async with websockets.connect(self._url) as ws:
            await self._connect(ws)
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
            try:
                async for raw in ws:
                    parsed = _parse_notification(json.loads(raw))
                    if parsed is not None:
                        yield parsed
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

    async def _connect(self, ws) -> None:  # noqa: ANN001
        token = await self._access_token_provider()
        await ws.send(json.dumps({
            "action": "connect", "value": self._account_numbers,
            "auth-token": token, "request-id": self._request_id(),
        }))
        ack = json.loads(await ws.recv())
        if ack.get("status") != "ok":
            raise TastyTradeStreamError(f"account-streamer connect failed: {ack}")

    async def _heartbeat_loop(self, ws) -> None:  # noqa: ANN001
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            token = await self._access_token_provider()
            await ws.send(json.dumps({
                "action": "heartbeat", "auth-token": token, "request-id": self._request_id(),
            }))


def _parse_notification(msg: dict) -> "FillEvent | BrokerPosition | BalanceMessage | None":
    """A raw streamer message -> a ``MessageSource`` item, or ``None`` for anything that isn't a
    data notification (action-response acks) or isn't one tt-ledger has a use for (watchlists,
    quote alerts)."""
    msg_type = msg.get("type")
    if msg_type is None:
        return None
    data = msg.get("data", {})
    if msg_type == "Order":
        return _order_notification_to_fill_event(data)
    if msg_type == "CurrentPosition":
        return position_from_json(data)
    if msg_type == "AccountBalance":
        return BalanceMessage(account_number=str(data.get("account-number", "")), raw=data)
    return None


def _order_notification_to_fill_event(data: dict) -> FillEvent:
    order = order_from_json(data)
    average_fill_price, filled_quantity, remaining_quantity = order_level_fill_fields(order.legs)
    is_filled = map_order_status(order.status) is OrderStatus.FILLED
    return FillEvent(
        tt_order_id=order.id,
        status=order.status,
        average_fill_price=average_fill_price,
        filled_quantity=filled_quantity,
        remaining_quantity=remaining_quantity,
        filled_at=(order.terminal_at if is_filled else None),
    )
