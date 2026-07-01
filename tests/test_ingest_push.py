"""``StreamConsumer`` / ``MessageSource`` (docs/ingestion.md -> Push).

The transport (a real broker WebSocket, or the host platform's Redis pub/sub) isn't implemented
-- same deferral as the real REST client -- so these tests drive the consumer through
``MockMessageSource``, mirroring how ``test_ingest_pull.py`` drives sync_* through
``MockTastyTradeClient``.
"""

from __future__ import annotations

from datetime import datetime, UTC
from decimal import Decimal

import pytest

from tt_ledger.enums import Ingest, Origin
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BalanceMessage, BrokerPosition
from tt_ledger.ingest.mock_broker import MockMessageSource
from tt_ledger.ingest.push import MessageSource, StreamConsumer
from tt_ledger.rows import FillEvent, OrderRow
from tt_ledger.store.memory import InMemoryStore


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def resolver() -> PassthroughResolver:
    return PassthroughResolver()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


def test_mock_message_source_conforms_to_protocol():
    assert isinstance(MockMessageSource(), MessageSource)


async def test_apply_fill_enriches_an_existing_order(store, accounts, resolver):
    await store.upsert_orders(
        [OrderRow(tt_order_id="O-1", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT, oms_status="submitted")]
    )
    source = MockMessageSource()
    source.push(
        FillEvent(
            tt_order_id="O-1", status="Filled", average_fill_price=Decimal("150.25"),
            filled_quantity=Decimal("10"), remaining_quantity=Decimal("0"),
            filled_at=datetime(2026, 1, 5, tzinfo=UTC),
        )
    )
    consumer = StreamConsumer(store, source, accounts=accounts, resolver=resolver)

    await consumer.run()

    order = await store.get_order("O-1")
    assert order.oms_status == "filled"
    assert order.average_fill_price == Decimal("150.25")
    assert order.origin is Origin.ZTS  # untouched


async def test_apply_fill_for_unknown_order_is_a_noop(store, accounts, resolver):
    source = MockMessageSource()
    source.push(FillEvent(tt_order_id="does-not-exist", status="Filled"))
    consumer = StreamConsumer(store, source, accounts=accounts, resolver=resolver)

    await consumer.run()

    assert await store.get_order("does-not-exist") is None


async def test_position_message_creates_and_resolves_the_position(store, accounts, resolver):
    source = MockMessageSource()
    source.push(
        BrokerPosition(
            account_number="ACCT1", symbol="AAPL", instrument_type="Equity",
            quantity=Decimal("100"), quantity_direction="Long",
            mark_price=Decimal("155.50"), unrealized_pnl=Decimal("550"),
        )
    )
    consumer = StreamConsumer(store, source, accounts=accounts, resolver=resolver)

    await consumer.run()

    position = await store.get_position("main", "AAPL")
    assert position is not None
    assert position.quantity == Decimal("100")
    assert position.mark_price == Decimal("155.50")

    security = await store.get_security("AAPL")
    assert security is not None  # resolved + upserted, same as sync_positions


async def test_position_message_preserves_existing_attribution(store, accounts, resolver):
    from dataclasses import replace

    from tt_ledger.rows import PositionRow

    await store.upsert_positions(
        [PositionRow(account="main", security_id="AAPL", quantity=Decimal("100"), quantity_direction="Long")]
    )
    existing = await store.get_position("main", "AAPL")
    await store.upsert_positions([replace(existing, trade_group_id=7, strategy_id=3)])

    source = MockMessageSource()
    source.push(BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("120"), quantity_direction="Long", mark_price=Decimal("160")))
    consumer = StreamConsumer(store, source, accounts=accounts, resolver=resolver)

    await consumer.run()

    updated = await store.get_position("main", "AAPL")
    assert updated.quantity == Decimal("120")
    assert updated.mark_price == Decimal("160")
    assert updated.trade_group_id == 7  # preserved, not clobbered
    assert updated.strategy_id == 3


async def test_balance_message_is_forwarded_to_the_hook_and_never_persisted(store, accounts, resolver):
    received = []
    source = MockMessageSource()
    source.push(BalanceMessage(account_number="ACCT1", raw={"cash": "10000.00"}))
    consumer = StreamConsumer(store, source, accounts=accounts, resolver=resolver, on_balance=received.append)

    await consumer.run()

    assert len(received) == 1
    assert received[0].raw == {"cash": "10000.00"}


async def test_balance_message_without_a_hook_is_a_noop(store, accounts, resolver):
    source = MockMessageSource()
    source.push(BalanceMessage(account_number="ACCT1", raw={}))
    consumer = StreamConsumer(store, source, accounts=accounts, resolver=resolver)

    await consumer.run()  # must not raise


async def test_run_processes_a_mixed_stream_in_order(store, accounts, resolver):
    await store.upsert_orders(
        [OrderRow(tt_order_id="O-1", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)]
    )
    balances = []
    source = MockMessageSource()
    source.push(FillEvent(tt_order_id="O-1", status="Filled"))
    source.push(BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("1"), quantity_direction="Long"))
    source.push(BalanceMessage(account_number="ACCT1", raw={}))
    consumer = StreamConsumer(store, source, accounts=accounts, resolver=resolver, on_balance=balances.append)

    await consumer.run()

    assert (await store.get_order("O-1")).oms_status == "filled"
    assert await store.get_position("main", "AAPL") is not None
    assert len(balances) == 1


async def test_stop_halts_processing_of_remaining_messages(store, accounts, resolver):
    await store.upsert_orders(
        [OrderRow(tt_order_id="O-1", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)]
    )
    source = MockMessageSource()
    source.push(BalanceMessage(account_number="ACCT1", raw={}))  # triggers stop() via the hook
    source.push(FillEvent(tt_order_id="O-1", status="Filled"))  # must NOT be processed

    consumer = StreamConsumer(store, source, accounts=accounts, resolver=resolver)
    consumer._on_balance = lambda _msg: consumer.stop()

    await consumer.run()

    order = await store.get_order("O-1")
    assert order.oms_status is None  # the fill after the stop was never applied
