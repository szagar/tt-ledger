"""``RedisMessageSource`` (docs/integration-zts.md — host-platform stream variant).

The transport is faked with a minimal in-memory pubsub double (no [redis] extra needed);
the mapping functions are tested directly against the host's documented envelope shapes.
End-to-end, a faked stream drives the real ``StreamConsumer`` exactly like
``test_ingest_push.py`` does with ``MockMessageSource``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tt_ledger.enums import Ingest, Origin
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BalanceMessage, BrokerPosition
from tt_ledger.ingest.push import MessageSource, StreamConsumer
from tt_ledger.ingest.redis_source import (
    RedisMessageSource,
    balance_from_pubsub,
    fill_event_from_pubsub,
    position_from_pubsub,
)
from tt_ledger.rows import FillEvent, OrderRow
from tt_ledger.store.memory import InMemoryStore


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1", "roth": "ACCT2"})


def _envelope(msg_type: str, nickname: str, data: dict, *, timestamp: str = "2026-01-05T15:30:00+00:00") -> dict:
    """The host platform's pub/sub envelope shape."""
    return {"type": msg_type, "account_number": nickname, "source": "streamer", "timestamp": timestamp, "data": data}


# --------------------------------------------------------------------- mapping functions


def test_order_envelope_maps_to_fill_event():
    msg = _envelope("order", "main", {
        "order_id": "O-99", "status": "Filled", "filled_quantity": "2", "remaining_quantity": "0",
        "fill_price": "1.25", "underlying_symbol": "SPXW", "event_type": "FILL",
    })
    evt = fill_event_from_pubsub(msg)
    assert evt == FillEvent(
        tt_order_id="O-99", status="Filled", average_fill_price=Decimal("1.25"),
        filled_quantity=Decimal("2"), remaining_quantity=Decimal("0"),
        filled_at=datetime(2026, 1, 5, 15, 30, tzinfo=UTC),
    )


def test_non_terminal_order_has_no_filled_at():
    msg = _envelope("order", "main", {
        "order_id": "O-99", "status": "Live", "filled_quantity": "0", "remaining_quantity": "2",
        "fill_price": None,
    })
    evt = fill_event_from_pubsub(msg)
    assert evt.status == "Live"
    assert evt.filled_at is None
    assert evt.average_fill_price is None


def test_order_without_id_is_dropped():
    assert fill_event_from_pubsub(_envelope("order", "main", {"status": "Filled"})) is None


def test_position_envelope_maps_to_broker_position():
    msg = _envelope("position", "main", {
        "symbol": "SPXW  260105P05900000", "security_id": "option:SPXW:2026-01-05:put:5900",
        "instrument_type": "Equity Option", "underlying_symbol": "SPXW", "streamer_symbol": ".SPXW260105P5900",
        "quantity": "2", "quantity_direction": "Short", "average_open_price": "1.20",
        "close_price": "0.95", "mark": "190.0", "mark_price": "0.95", "multiplier": 100,
        "expires_at": "2026-01-05T21:15:00+00:00", "realized_day_gain": "50.0",
        "created_at": "2026-01-05T14:31:00+00:00",
    })
    pos = position_from_pubsub(msg, "ACCT1")
    assert pos == BrokerPosition(
        account_number="ACCT1", symbol="SPXW  260105P05900000", quantity=Decimal("2"),
        quantity_direction="Short", underlying_symbol="SPXW", instrument_type="Equity Option",
        average_open_price=Decimal("1.20"), mark_price=Decimal("0.95"), close_price=Decimal("0.95"),
        realized_day_gain=Decimal("50.0"), realized_day_gain_effect=None, multiplier=100,
        expires_at=datetime(2026, 1, 5, 21, 15, tzinfo=UTC),
    )


def test_position_without_symbol_or_quantity_is_dropped():
    assert position_from_pubsub(_envelope("position", "main", {"quantity": "1"}), "ACCT1") is None
    assert position_from_pubsub(_envelope("position", "main", {"symbol": "AAPL"}), "ACCT1") is None


def test_balance_envelope_maps_to_balance_message():
    data = {"net_liquidating_value": "50000.0", "cash_balance": "12000.0"}
    msg = balance_from_pubsub(_envelope("balance", "main", data), "ACCT1")
    assert isinstance(msg, BalanceMessage)
    assert msg.account_number == "ACCT1"
    assert msg.raw == data
    assert msg.net_liquidating_value == Decimal("50000.0")
    assert msg.cash_balance == Decimal("12000.0")
    assert msg.equity_buying_power is None
    assert msg.captured_at == datetime(2026, 1, 5, 15, 30, tzinfo=UTC)


# --------------------------------------------------------------------- fake pubsub transport


class _FakePubSub:
    def __init__(self, payloads: list) -> None:
        self._payloads = payloads
        self.subscribed: list[str] = []

    async def subscribe(self, *channels: str) -> None:
        self.subscribed.extend(channels)

    async def listen(self):
        for p in self._payloads:
            yield p


class _FakeRedis:
    """The slice of redis.asyncio.Redis the source touches."""

    def __init__(self, payloads: list) -> None:
        self._pubsub = _FakePubSub(payloads)
        self.closed = False

    def pubsub(self) -> _FakePubSub:
        return self._pubsub

    async def aclose(self) -> None:
        self.closed = True


def _wire(msg: dict) -> dict:
    return {"type": "message", "channel": f"acct:{msg['type']}", "data": json.dumps(msg)}


def _source(payloads: list, accounts: AccountMapper, **kw) -> RedisMessageSource:
    return RedisMessageSource("redis://unused:6379", accounts=accounts, client=_FakeRedis(payloads), **kw)


async def _drain(source: RedisMessageSource) -> list:
    return [m async for m in source.messages()]


def test_conforms_to_protocol(accounts):
    assert isinstance(_source([], accounts), MessageSource)


async def test_translates_nickname_to_account_number(accounts):
    out = await _drain(_source([
        _wire(_envelope("balance", "roth", {"net_liquidating_value": "1"})),
    ], accounts))
    assert [m.account_number for m in out] == ["ACCT2"]


async def test_unknown_nickname_and_non_message_frames_are_skipped(accounts):
    out = await _drain(_source([
        {"type": "subscribe", "channel": "acct:order", "data": 1},        # pubsub control frame
        _wire(_envelope("balance", "someone_elses_account", {})),          # unmappable nickname
        {"type": "message", "channel": "acct:order", "data": "not json"},  # garbage payload
        _wire(_envelope("balance", "main", {})),
    ], accounts))
    assert [m.account_number for m in out] == ["ACCT1"]


async def test_nickname_filter_scopes_to_one_login(accounts):
    out = await _drain(_source([
        _wire(_envelope("balance", "main", {})),
        _wire(_envelope("balance", "roth", {})),
    ], accounts, nicknames={"roth"}))
    assert [m.account_number for m in out] == ["ACCT2"]


async def test_end_to_end_fill_and_position_through_stream_consumer(accounts):
    store = InMemoryStore()
    await store.upsert_orders(
        [OrderRow(tt_order_id="O-7", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT, oms_status="submitted")]
    )
    source = _source([
        _wire(_envelope("order", "main", {
            "order_id": "O-7", "status": "Filled", "filled_quantity": "1", "remaining_quantity": "0",
            "fill_price": "2.50",
        })),
        _wire(_envelope("position", "main", {
            "symbol": "AAPL", "instrument_type": "Equity", "quantity": "100",
            "quantity_direction": "Long", "average_open_price": "150.0", "multiplier": 1,
        })),
    ], accounts)
    consumer = StreamConsumer(store, source, accounts=accounts, resolver=PassthroughResolver())

    await consumer.run()

    order = await store.get_order("O-7")
    assert order.oms_status == "filled"
    assert order.average_fill_price == Decimal("2.50")
    pos = await store.get_position("main", "AAPL")
    assert pos is not None and pos.quantity == Decimal("100")


async def test_reconnect_gives_up_after_max_attempts(accounts):
    class _ExplodingRedis(_FakeRedis):
        def pubsub(self):
            raise ConnectionError("redis down")

    source = RedisMessageSource(
        "redis://unused:6379", accounts=accounts,
        client=_ExplodingRedis([]), max_connect_attempts=1,
    )
    with pytest.raises(ConnectionError):
        await _drain(source)
