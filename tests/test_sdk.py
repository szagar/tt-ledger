"""``LedgerClient`` (docs/api.md) — the in-process SDK over everything built in earlier milestones."""

from __future__ import annotations

from datetime import datetime, UTC
from decimal import Decimal

import pytest

from tt_ledger.enums import Ingest, Origin, ReviewStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BalanceMessage, BrokerPosition
from tt_ledger.ingest.mock_broker import MockMessageSource, MockTastyTradeClient
from tt_ledger.rows import FillEvent, OrderInput, OrderRow
from tt_ledger.sdk import LedgerClient
from tt_ledger.store.memory import InMemoryStore


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def resolver() -> PassthroughResolver:
    return PassthroughResolver()


@pytest.fixture
def broker() -> MockTastyTradeClient:
    return MockTastyTradeClient()


@pytest.fixture
def client(accounts, resolver, broker) -> LedgerClient:
    return LedgerClient(InMemoryStore(), accounts=accounts, resolver=resolver, client=broker)


# --- sync ---------------------------------------------------------------------------------


async def test_sync_without_a_client_raises_a_clear_error(accounts, resolver):
    c = LedgerClient(InMemoryStore(), accounts=accounts, resolver=resolver)
    with pytest.raises(RuntimeError, match="requires a broker client"):
        await c.sync("main")


async def test_sync_pulls_and_reconciles(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )

    result = await client.sync("main")
    assert result.orders == 1
    assert result.transactions == 1
    assert result.positions == 0
    assert result.trade_groups == 1
    assert result.errors == []

    trades = await client.trades(account="main")
    assert len(trades) == 1
    assert trades[0].strategy_type == "single"

    orders = await client.orders(account="main")
    assert len(orders) == 1
    assert orders[0].tt_order_id == "O-1"


# --- record_order / apply_fill (oms_submit + push paths) -----------------------------------


async def test_record_order_creates_a_zts_row_with_no_tt_order_id(client):
    row = await client.record_order(
        OrderInput(account="main", security_id="AAPL", order_type="Limit", price=Decimal("150"), signal_id="SIG-1")
    )
    assert row.tt_order_id is None
    assert row.origin is Origin.ZTS
    assert row.ingest is Ingest.OMS_SUBMIT
    assert row.signal_id == "SIG-1"

    orders = await client.orders(account="main", origin=Origin.ZTS)
    assert len(orders) == 1
    assert orders[0].security_id == "AAPL"


async def test_apply_fill_enriches_an_existing_order_by_tt_order_id(client):
    await client._store.upsert_orders(
        [OrderRow(tt_order_id="O-1", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT, oms_status="submitted")]
    )

    await client.apply_fill(
        FillEvent(
            tt_order_id="O-1", status="Filled", average_fill_price=Decimal("150.25"),
            filled_quantity=Decimal("10"), remaining_quantity=Decimal("0"),
            filled_at=datetime(2026, 1, 5, tzinfo=UTC),
        )
    )

    orders = await client.orders(account="main")
    assert len(orders) == 1
    assert orders[0].oms_status == "filled"
    assert orders[0].average_fill_price == Decimal("150.25")
    assert orders[0].origin is Origin.ZTS  # untouched


async def test_apply_fill_for_unknown_order_is_a_noop(client):
    await client.apply_fill(FillEvent(tt_order_id="does-not-exist", status="Filled"))
    assert await client.orders(account="main") == []


# --- reads ------------------------------------------------------------------------------


async def test_trade_returns_none_for_unknown_group(client):
    assert await client.trade("does-not-exist") is None


async def test_trade_returns_the_matching_trade_row(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await client.sync("main")
    trades = await client.trades(account="main")
    group_id = trades[0].group_id

    trade = await client.trade(group_id)
    assert trade is not None
    assert trade.group_id == group_id
    assert trade.strategy_type == "single"


async def test_trades_accepts_plain_string_filters(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await client.sync("main")

    trades = await client.trades(account="main", origin="broker", review_status="needs_review")
    assert len(trades) == 1


async def test_orders_accepts_plain_string_origin_filter(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await client.sync("main")

    orders = await client.orders(account="main", origin="broker")
    assert len(orders) == 1


async def test_account_activity(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await client.sync("main")

    activity = await client.account_activity("main")
    assert len(activity) == 1
    assert activity[0].origin is Origin.BROKER


# --- stream_consumer ------------------------------------------------------------------------


async def test_stream_consumer_is_bound_to_this_clients_store_and_accounts(client):
    source = MockMessageSource()
    source.push(
        BrokerPosition(
            account_number="ACCT1", symbol="AAPL", instrument_type="Equity",
            quantity=Decimal("100"), quantity_direction="Long", mark_price=Decimal("155.50"),
        )
    )
    consumer = client.stream_consumer(source)
    await consumer.run()

    position = await client.position("main", "AAPL")
    assert position is not None
    assert position.quantity == Decimal("100")


async def test_stream_consumer_forwards_balance_messages_to_the_hook(client):
    received = []
    source = MockMessageSource()
    source.push(BalanceMessage(account_number="ACCT1", raw={"cash": "10000.00"}))
    consumer = client.stream_consumer(source, on_balance=received.append)

    await consumer.run()
    assert len(received) == 1
    assert received[0].raw == {"cash": "10000.00"}


# --- positions ----------------------------------------------------------------------------


async def test_position_returns_none_for_unknown_security(client):
    assert await client.position("main", "AAPL") is None


async def test_rebuild_positions_populates_positions_and_closed_positions(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    broker.fill(
        account_number="ACCT1", order_id="O-2", symbol="AAPL", instrument_type="Equity",
        action="Sell to Close", quantity=Decimal("10"), fill_price=Decimal("170"),
        filled_at=datetime(2026, 1, 8, tzinfo=UTC), status="Filled",
    )
    await client.sync("main")

    result = await client.rebuild_positions("main")
    assert result.positions == 1
    assert result.errors == []

    position = await client.position("main", "AAPL")
    assert position.quantity == Decimal("0")

    # open_only (default) filters the now-flat AAPL row out
    assert await client.positions("main") == []
    assert len(await client.positions("main", open_only=False)) == 1

    closed = await client.closed_positions("main")
    assert len(closed) == 1
    assert closed[0].realized_pnl == Decimal("200")
    assert await client.closed_positions("main", "MSFT") == []


async def test_positions_open_only_excludes_flat_rows_but_not_open_ones(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await client.sync("main")
    await client.rebuild_positions("main")

    open_positions = await client.positions("main")
    assert len(open_positions) == 1
    assert open_positions[0].security_id == "AAPL"


# --- reconcile (without a broker pull) ------------------------------------------------------


async def test_reconcile_groups_already_synced_data(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    # sync without reconcile happening twice: call the raw pull pieces via sync(), then
    # reconcile() again directly should be a no-op (idempotent).
    await client.sync("main")
    result = await client.reconcile("main")
    assert result.trade_groups == 0


# --- remap / regroup / dismiss --------------------------------------------------------------


async def test_remap_trade_delegates_and_persists(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await client.sync("main")
    trade = (await client.trades(account="main"))[0]

    result = await client.remap_trade(trade.group_id, strategy=42, bot="my-bot", reviewed_by="alice")
    assert result.strategy_id == 42
    assert result.manually_attributed is True
    assert result.review_status is ReviewStatus.CONFIRMED


async def test_dismiss_trade_delegates_and_persists(client, broker):
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await client.sync("main")
    trade = (await client.trades(account="main"))[0]

    result = await client.dismiss_trade(trade.group_id, reviewed_by="alice")
    assert result.review_status is ReviewStatus.IGNORED


async def test_regroup_delegates_and_persists(client, broker):
    t0 = datetime(2026, 1, 5, 10, 0, tzinfo=UTC)
    broker.fill(account_number="ACCT1", order_id="O-A", symbol="AAPL", instrument_type="Equity", action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=t0, status="Filled")
    broker.fill(account_number="ACCT1", order_id="O-B", symbol="MSFT", instrument_type="Equity", action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("300"), filled_at=t0, status="Filled")
    await client.sync("main")

    trades = await client.trades(account="main")
    assert len(trades) == 1
    assert trades[0].leg_count == 2

    store = client._store
    txn = next(row for _, row in store._transactions.all() if row.tt_order_id == "O-B")
    txn_id = store._transactions.id_of(txn.tt_transaction_id)

    updated = await client.regroup([txn_id], target=None, reviewed_by="alice")
    assert len(updated) == 2
    trades_after = await client.trades(account="main")
    assert len(trades_after) == 2


# --- close ------------------------------------------------------------------------------


async def test_close_disposes_the_store(accounts, resolver):
    c = LedgerClient(InMemoryStore(), accounts=accounts, resolver=resolver)
    await c.close()  # InMemoryStore has no dispose() -- must not raise


# --- confirmatory test against the real SQL store -------------------------------------------


async def test_open_and_sync_against_sql_store(accounts, resolver, broker):
    c = LedgerClient.open("sqlite+aiosqlite:///:memory:", accounts=accounts, resolver=resolver, client=broker)
    await c._store.create_all()
    try:
        broker.fill(
            account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
            action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
            filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
        )
        async with c._store._sessionmaker() as session, session.begin():
            from tt_ledger.schema import models

            await session.execute(
                models.Account.__table__.insert().values(nickname="main", account_number="ACCT1", login="user1")
            )

        result = await c.sync("main")
        assert result.orders == 1
        assert result.trade_groups == 1

        trades = await c.trades(account="main")
        assert len(trades) == 1
    finally:
        await c.close()
