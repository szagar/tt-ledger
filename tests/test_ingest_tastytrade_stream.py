"""``TastyTradeMessageSource`` (docs/ingestion.md -> Push) — the real account-streamer client.

No mocked transport (``websockets`` has no ``httpx.MockTransport`` equivalent): the integration
tests spin up a real local ``websockets.serve`` server implementing the documented connect/
heartbeat/notification protocol and connect to it over an actual (loopback) WebSocket. The
notification-parsing logic itself is also tested directly, with no network at all.
"""

from __future__ import annotations

import json
from datetime import datetime, UTC
from decimal import Decimal

import pytest
import websockets

from tt_ledger.ingest.broker import BalanceMessage, BrokerPosition
from tt_ledger.ingest.tastytrade_stream import (
    STREAMER_PRODUCTION_URL,
    TastyTradeMessageSource,
    TastyTradeStreamError,
    _order_notification_to_fill_event,
    _parse_notification,
)
from tt_ledger.rows import FillEvent

# The "Order Notification" example from developer.tastytrade.com's Streaming Account Data guide,
# verbatim -- note it omits fields the REST order-history response always has (e.g. received-at).
ORDER_NOTIFICATION_DATA = {
    "id": 1,
    "account-number": "5WT00000",
    "time-in-force": "Day",
    "order-type": "Market",
    "size": 100,
    "underlying-symbol": "AAPL",
    "underlying-instrument-type": "Equity",
    "status": "Live",
    "cancellable": True,
    "editable": True,
    "edited": False,
    "legs": [
        {
            "instrument-type": "Equity",
            "symbol": "AAPL",
            "quantity": 100,
            "remaining-quantity": 100,
            "action": "Buy to Open",
            "fills": [],
        }
    ],
}

POSITION_DATA = {
    "account-number": "5WT00000",
    "symbol": "AAPL",
    "quantity": "100",
    "quantity-direction": "Long",
    "average-open-price": "150.00",
    "mark-price": "155.50",
    "multiplier": 1,
}


async def _token_provider() -> str:
    return "token-1"


# --- _parse_notification / _order_notification_to_fill_event: pure unit tests --------------


def test_parse_notification_ignores_action_acks():
    assert _parse_notification({"status": "ok", "action": "connect"}) is None


def test_parse_notification_ignores_unrecognized_types():
    assert _parse_notification({"type": "PublicWatchlist", "data": {}}) is None


def test_parse_order_notification_to_fill_event():
    msg = _parse_notification({"type": "Order", "data": ORDER_NOTIFICATION_DATA, "timestamp": 1688595114405})
    assert isinstance(msg, FillEvent)
    assert msg.tt_order_id == "1"
    assert msg.status == "Live"
    assert msg.filled_at is None  # not a terminal status


def test_parse_order_notification_derives_single_leg_fill_fields():
    data = {
        **ORDER_NOTIFICATION_DATA,
        "id": 2,
        "status": "Filled",
        "terminal-at": "2023-07-05T19:07:32.737+00:00",
        "legs": [
            {
                "instrument-type": "Equity", "symbol": "AAPL", "action": "Buy to Open",
                "quantity": 10, "remaining-quantity": 0,
                "fills": [{"fill-id": "F-1", "quantity": 10, "fill-price": "150.25", "filled-at": "2023-07-05T19:07:32.496+00:00"}],
            }
        ],
    }
    fill_event = _order_notification_to_fill_event(data)
    assert fill_event.average_fill_price == Decimal("150.25")
    assert fill_event.filled_quantity == Decimal("10")
    assert fill_event.remaining_quantity == Decimal("0")
    assert fill_event.filled_at == datetime(2023, 7, 5, 19, 7, 32, 737000, tzinfo=UTC)


def test_parse_order_notification_multi_leg_leaves_aggregates_none():
    data = {
        **ORDER_NOTIFICATION_DATA,
        "legs": [
            {"instrument-type": "Equity Option", "symbol": "AAPL  260117C00150000", "action": "Sell to Open", "quantity": 1, "remaining-quantity": 0, "fills": []},
            {"instrument-type": "Equity Option", "symbol": "AAPL  260117P00150000", "action": "Sell to Open", "quantity": 1, "remaining-quantity": 0, "fills": []},
        ],
    }
    fill_event = _order_notification_to_fill_event(data)
    assert fill_event.average_fill_price is None
    assert fill_event.filled_quantity is None
    assert fill_event.remaining_quantity is None


def test_parse_position_notification():
    msg = _parse_notification({"type": "CurrentPosition", "data": POSITION_DATA})
    assert isinstance(msg, BrokerPosition)
    assert msg.symbol == "AAPL"
    assert msg.quantity == Decimal("100")
    assert msg.mark_price == Decimal("155.50")


def test_parse_balance_notification():
    msg = _parse_notification({"type": "AccountBalance", "data": {"account-number": "5WT00000", "cash-balance": "1000.00"}})
    assert isinstance(msg, BalanceMessage)
    assert msg.account_number == "5WT00000"
    assert msg.raw == {"account-number": "5WT00000", "cash-balance": "1000.00"}


# --- constructor / factory -------------------------------------------------------------------


def test_accepts_a_single_account_number_string():
    source = TastyTradeMessageSource(access_token_provider=_token_provider, account_numbers="5WT00000")
    assert source._account_numbers == ["5WT00000"]


def test_accepts_multiple_account_numbers():
    source = TastyTradeMessageSource(access_token_provider=_token_provider, account_numbers=["A1", "A2"])
    assert source._account_numbers == ["A1", "A2"]


def test_default_url_is_production():
    source = TastyTradeMessageSource(access_token_provider=_token_provider, account_numbers="A1")
    assert source._url == STREAMER_PRODUCTION_URL


async def test_from_client_reuses_the_rest_clients_token_method():
    class FakeClient:
        async def access_token(self) -> str:
            return "reused-token"

    source = TastyTradeMessageSource.from_client(FakeClient(), "5WT00000")
    assert source._account_numbers == ["5WT00000"]
    assert await source._access_token_provider() == "reused-token"


def test_without_websockets_installed_raises_a_clear_error(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "websockets":
            raise ModuleNotFoundError("No module named 'websockets'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    with pytest.raises(RuntimeError, match=r"\[tastytrade\]"):
        TastyTradeMessageSource(access_token_provider=_token_provider, account_numbers="A1")


# --- integration: a real local websockets.serve server ---------------------------------------


async def _serve(handler):
    server = await websockets.serve(handler, "localhost", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"ws://localhost:{port}"


async def test_connects_subscribes_and_receives_notifications():
    seen_connect = {}

    async def handler(ws):
        seen_connect.update(json.loads(await ws.recv()))
        await ws.send(json.dumps({
            "status": "ok", "action": "connect", "web-socket-session-id": "sess-1",
            "value": seen_connect["value"], "request-id": seen_connect["request-id"],
        }))
        await ws.send(json.dumps({"type": "Order", "data": ORDER_NOTIFICATION_DATA, "timestamp": 1}))
        await ws.send(json.dumps({"type": "CurrentPosition", "data": POSITION_DATA}))
        await ws.send(json.dumps({"type": "AccountBalance", "data": {"account-number": "5WT00000"}}))
        await ws.send(json.dumps({"type": "PublicWatchlist", "data": {}}))  # ignored by the consumer
        await ws.close()

    server, url = await _serve(handler)
    try:
        source = TastyTradeMessageSource(
            access_token_provider=_token_provider, account_numbers="5WT00000",
            url=url, heartbeat_interval=999,
        )
        results = [msg async for msg in source.messages()]
    finally:
        server.close()
        await server.wait_closed()

    assert seen_connect["action"] == "connect"
    assert seen_connect["value"] == ["5WT00000"]
    assert seen_connect["auth-token"] == "token-1"

    assert len(results) == 3
    assert isinstance(results[0], FillEvent)
    assert isinstance(results[1], BrokerPosition)
    assert isinstance(results[2], BalanceMessage)


async def test_raises_when_connect_is_rejected():
    async def handler(ws):
        await ws.recv()
        await ws.send(json.dumps({"status": "error", "action": "connect", "reason": "bad token"}))
        await ws.wait_closed()

    server, url = await _serve(handler)
    try:
        source = TastyTradeMessageSource(access_token_provider=_token_provider, account_numbers="A1", url=url)
        with pytest.raises(TastyTradeStreamError, match="connect failed"):
            async for _ in source.messages():
                pass
    finally:
        server.close()
        await server.wait_closed()


async def test_sends_periodic_heartbeats_with_a_fresh_token():
    heartbeat_count = 0

    async def handler(ws):
        await ws.recv()
        await ws.send(json.dumps({"status": "ok", "action": "connect", "request-id": 1}))
        async for raw in ws:
            nonlocal heartbeat_count
            msg = json.loads(raw)
            assert msg["action"] == "heartbeat"
            assert msg["auth-token"] == "token-1"
            heartbeat_count += 1
            if heartbeat_count >= 2:
                await ws.close()
                return

    server, url = await _serve(handler)
    try:
        source = TastyTradeMessageSource(
            access_token_provider=_token_provider, account_numbers="A1", url=url, heartbeat_interval=0.05,
        )
        async for _ in source.messages():
            pass  # the server closes the connection after 2 heartbeats
    finally:
        server.close()
        await server.wait_closed()

    assert heartbeat_count >= 2
