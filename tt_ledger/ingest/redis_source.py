"""``RedisMessageSource`` — the host-platform ``MessageSource`` (docs/integration-zts.md).

Consumes a host platform's account-stream **Redis pub/sub** (the ``acct:order`` /
``acct:position`` / ``acct:balance`` channels its account-stream service publishes) instead of
connecting the broker WebSocket directly — the host already holds one streamer connection per
login; a second would double it. Requires the ``[redis]`` extra, imported lazily in ``__init__``
(same pattern as ``TastyTradeMessageSource`` / ``websockets``).

Wire format (the host's pub/sub envelope, one JSON object per message)::

    {"type": "order"|"position"|"balance", "account_number": <nickname>, "source": ...,
     "timestamp": <iso>, "data": {...snake_case fields...}}

Two impedance points this module owns:

* **Nicknames vs account numbers.** The host publishes account *nicknames*; the
  ``MessageSource`` protocol yields broker-native shapes carrying *account numbers*
  (``StreamConsumer`` maps them back to nicknames itself). The injected ``AccountMapper``
  translates; a nickname the mapper doesn't know is skipped, not an error.
* **snake_case host fields vs dasherized broker fields.** The mapping functions below are the
  single place the host envelope is interpreted; they never see raw broker JSON.

Reconnects: the generator retries forever with capped exponential backoff (a daemon transport),
resetting the backoff after any successfully received message. ``max_connect_attempts`` bounds
it for tests.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, AsyncIterator

from ..enums import OrderStatus
from ..repositories import map_order_status
from ..rows import FillEvent
from .broker import BalanceMessage, BrokerPosition

if TYPE_CHECKING:
    from ..identity import AccountMapper

logger = logging.getLogger(__name__)

_DEFAULT_CHANNEL_PREFIX = "acct"
_RECONNECT_INITIAL_SECONDS = 1.0
_RECONNECT_MAX_SECONDS = 30.0


def _dec(value) -> Decimal | None:  # noqa: ANN001
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _dt(value) -> datetime | None:  # noqa: ANN001
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def fill_event_from_pubsub(msg: dict) -> FillEvent | None:
    """An ``{"type": "order"}`` envelope -> ``FillEvent`` (enrich-only, like the WS path)."""
    data = msg.get("data", {})
    tt_order_id = str(data.get("order_id") or "")
    if not tt_order_id:
        return None
    status = data.get("status")
    is_filled = status is not None and map_order_status(status) is OrderStatus.FILLED
    return FillEvent(
        tt_order_id=tt_order_id,
        status=status,
        average_fill_price=_dec(data.get("fill_price")),
        filled_quantity=_dec(data.get("filled_quantity")),
        remaining_quantity=_dec(data.get("remaining_quantity")),
        filled_at=(_dt(msg.get("timestamp")) if is_filled else None),
    )


def position_from_pubsub(msg: dict, account_number: str) -> BrokerPosition | None:
    """An ``{"type": "position"}`` envelope -> ``BrokerPosition``.

    The host envelope carries no ``realized-day-gain-effect`` (its account-stream service drops
    the effect string), so ``realized_day_gain_effect`` is ``None`` — the repository stores the
    magnitude unsigned. The host's REST ``sync()`` remains authoritative for that field.
    """
    data = msg.get("data", {})
    symbol = data.get("symbol") or ""
    quantity = _dec(data.get("quantity"))
    if not symbol or quantity is None:
        return None
    return BrokerPosition(
        account_number=account_number,
        symbol=symbol,
        quantity=quantity,
        quantity_direction=data.get("quantity_direction") or "Long",
        underlying_symbol=data.get("underlying_symbol"),
        instrument_type=data.get("instrument_type"),
        average_open_price=_dec(data.get("average_open_price")),
        mark_price=_dec(data.get("mark_price")),
        close_price=_dec(data.get("close_price")),
        realized_day_gain=_dec(data.get("realized_day_gain")),
        realized_day_gain_effect=None,
        multiplier=int(float(data.get("multiplier") or 1)),
        expires_at=_dt(data.get("expires_at")),
    )


def balance_from_pubsub(msg: dict, account_number: str) -> BalanceMessage:
    """An ``{"type": "balance"}`` envelope -> ``BalanceMessage``. The host's snake_case ``data``
    dict rides in ``raw``; the typed fields are parsed here (this module owns the host wire
    format, ``balance_from_json`` owns the broker's dasherized one)."""
    data = msg.get("data", {})
    return BalanceMessage(
        account_number=account_number,
        raw=data,
        net_liquidating_value=_dec(data.get("net_liquidating_value")),
        cash_balance=_dec(data.get("cash_balance")),
        equity_buying_power=_dec(data.get("equity_buying_power")),
        derivative_buying_power=_dec(data.get("derivative_buying_power")),
        maintenance_requirement=_dec(data.get("maintenance_requirement")),
        pending_cash=_dec(data.get("pending_cash")),
        day_trading_buying_power=_dec(data.get("day_trading_buying_power")),
        captured_at=_dt(msg.get("timestamp")),
    )


class RedisMessageSource:
    """Implements the ``MessageSource`` Protocol (``ingest/push.py``) over a host platform's
    ``acct:*`` Redis pub/sub. ``nicknames`` (optional) filters to one login's accounts — a
    multi-login host publishes every account on the same global channels."""

    def __init__(
        self,
        url: str,
        *,
        accounts: "AccountMapper",
        nicknames: set[str] | None = None,
        channel_prefix: str = _DEFAULT_CHANNEL_PREFIX,
        max_connect_attempts: int | None = None,
        client=None,  # noqa: ANN001 -- test seam: a pre-built redis.asyncio.Redis
    ) -> None:
        if client is None:
            try:
                import redis  # noqa: F401 -- import only to fail fast & clearly if missing
            except ModuleNotFoundError as exc:  # pragma: no cover
                raise RuntimeError(
                    "RedisMessageSource needs the [redis] extra: pip install tt-ledger[redis]"
                ) from exc
        self._url = url
        self._accounts = accounts
        self._nicknames = nicknames
        self._channels = [f"{channel_prefix}:order", f"{channel_prefix}:position", f"{channel_prefix}:balance"]
        self._max_connect_attempts = max_connect_attempts
        self._client = client

    def _make_client(self):  # noqa: ANN202
        if self._client is not None:
            return self._client
        import redis.asyncio as aioredis

        return aioredis.from_url(self._url, decode_responses=True)

    async def messages(self) -> "AsyncIterator[FillEvent | BrokerPosition | BalanceMessage]":
        attempts = 0
        backoff = _RECONNECT_INITIAL_SECONDS
        while True:
            client = self._make_client()
            try:
                pubsub = client.pubsub()
                await pubsub.subscribe(*self._channels)
                async for raw in pubsub.listen():
                    if raw.get("type") != "message":
                        continue
                    parsed = self._parse(raw.get("data"))
                    if parsed is not None:
                        attempts, backoff = 0, _RECONNECT_INITIAL_SECONDS  # healthy again
                        yield parsed
                return  # listen() ended cleanly (client closed) -- treat as stop
            except Exception as exc:
                attempts += 1
                if self._max_connect_attempts is not None and attempts >= self._max_connect_attempts:
                    raise
                logger.warning(
                    "redis pub/sub connection lost (%s); reconnecting in %.1fs (attempt %d)",
                    exc, backoff, attempts,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_MAX_SECONDS)
            finally:
                # a client handed in via the test seam is owned by the caller
                if self._client is None:
                    try:
                        await client.aclose()
                    except Exception:
                        pass

    def _parse(self, payload) -> "FillEvent | BrokerPosition | BalanceMessage | None":  # noqa: ANN001
        try:
            msg = json.loads(payload)
        except (TypeError, ValueError):
            return None
        if not isinstance(msg, dict):
            return None

        nickname = msg.get("account_number")
        if not nickname or (self._nicknames is not None and nickname not in self._nicknames):
            return None
        try:
            account_number = self._accounts.to_account_number(nickname)
        except KeyError:
            return None  # not one of ours (unknown to this mapper) -- skip, don't fail the stream

        msg_type = msg.get("type")
        if msg_type == "order":
            return fill_event_from_pubsub(msg)
        if msg_type == "position":
            return position_from_pubsub(msg, account_number)
        if msg_type == "balance":
            return balance_from_pubsub(msg, account_number)
        return None
