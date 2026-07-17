"""Submit-time intent: ``open_trade_group`` + ``record_order(trade_group=...)`` +
reconcile attaching fills to the intent group instead of clustering (docs/ingestion.md).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.enums import Origin, ReviewStatus, TradeGroupEventType, TradeGroupStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import reconcile
from tt_ledger.rows import OrderInput, OrderLegInput, TradeFilter
from tt_ledger.sdk import LedgerClient
from tt_ledger.store.memory import InMemoryStore

T0 = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)
PUT_A = "SPY   260116P00580000"


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


@pytest.fixture
def client(store, accounts) -> LedgerClient:
    return LedgerClient(store, accounts=accounts, resolver=PassthroughResolver())


def _fill(broker: MockTastyTradeClient, *, order_id: str, action: str, net_value: str,
          executed_at: datetime, quantity: str = "1") -> None:
    broker.fill(
        account_number="ACCT1", order_id=order_id, symbol=PUT_A, instrument_type="Equity Option",
        action=action, quantity=Decimal(quantity), fill_price=Decimal("2.5"),
        filled_at=executed_at, underlying_symbol="SPY",
    )
    broker._transactions["ACCT1"][-1].net_value = Decimal(net_value)


async def _sync(store, accounts, broker):
    resolver = PassthroughResolver()
    await sync_orders(store, "main", client=broker, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=broker, accounts=accounts, resolver=resolver)


async def test_open_trade_group_creates_confirmed_intent_group(client, store):
    trade = await client.open_trade_group(
        "main", strategy_type="vertical", underlying="SPY", quantity=Decimal("1"),
        max_loss=Decimal("400"), bot="spy_put_spread", signal="sig-123", strategy_id=7,
        reviewed_by="oms",
    )

    assert trade.origin is Origin.ZTS
    assert trade.review_status is ReviewStatus.CONFIRMED
    assert trade.manually_attributed is True
    assert trade.status == TradeGroupStatus.OPEN.value
    assert trade.bot_name == "spy_put_spread"
    assert trade.signal_id == "sig-123"

    pk = await store.get_trade_group_id(trade.group_id)
    events = [ev for _, ev in store._events.all() if ev.trade_group_id == pk]
    assert [e.event_type for e in events] == [TradeGroupEventType.ENTRY.value]


async def test_record_order_carries_the_intent_group(client, store):
    trade = await client.open_trade_group("main", strategy_type="single", underlying="SPY")
    order = await client.record_order(
        OrderInput(account="main", tt_order_id="O-1", underlying="SPY",
                   signal_id="sig-1", trade_group=trade.group_id)
    )
    assert order.tt_order_id == "O-1"
    assert order.trade_group_id == await store.get_trade_group_id(trade.group_id)


async def test_record_order_rejects_an_unknown_group(client):
    with pytest.raises(ValueError, match="unknown trade_group"):
        await client.record_order(OrderInput(account="main", trade_group="nope"))


async def test_record_order_writes_legs_at_submission(client, store):
    """Legs passed with the intent write land in order_legs immediately — a resting
    (working) order carries its structure from the first read, no pull-path wait."""
    call_b = "SPY   260116C00600000"
    trade = await client.open_trade_group("main", strategy_type="vertical", underlying="SPY")
    await client.record_order(
        OrderInput(
            account="main", tt_order_id="O-1", underlying="SPY", trade_group=trade.group_id,
            legs=[
                OrderLegInput(symbol=PUT_A, instrument_type="Equity Option",
                              action="Buy to Open", quantity=Decimal("1")),
                OrderLegInput(symbol=call_b, instrument_type="Equity Option",
                              action="Sell to Open", quantity=Decimal("2")),
            ],
        )
    )

    details = await client.trade_structure(trade.group_id)
    assert len(details) == 1
    legs = details[0].legs
    assert [(leg.security_id, leg.action, leg.quantity, leg.remaining_quantity) for leg in legs] == [
        (PUT_A, "Buy to Open", Decimal("1"), Decimal("1")),
        (call_b, "Sell to Open", Decimal("2"), Decimal("2")),
    ]
    # The securities dimension rows exist (order_legs FKs securities.security_id in SQL).
    for sid in (PUT_A, call_b):
        assert await store.get_security(sid) is not None


async def test_sync_enriches_intent_legs_in_place(client, store, accounts):
    """The pull path upserts on (order_id, leg_index) — intent-recorded legs are enriched
    with fill data, never duplicated."""
    trade = await client.open_trade_group("main", strategy_type="single", underlying="SPY")
    await client.record_order(
        OrderInput(
            account="main", tt_order_id="O-1", underlying="SPY", trade_group=trade.group_id,
            legs=[OrderLegInput(symbol=PUT_A, instrument_type="Equity Option",
                                action="Sell to Open", quantity=Decimal("1"))],
        )
    )
    broker = MockTastyTradeClient()
    _fill(broker, order_id="O-1", action="Sell to Open", net_value="250", executed_at=T0)
    await _sync(store, accounts, broker)

    details = await client.trade_structure(trade.group_id)
    assert len(details) == 1
    legs = details[0].legs
    assert len(legs) == 1  # enriched in place, not duplicated
    assert legs[0].security_id == PUT_A
    assert legs[0].fill_price == Decimal("2.5")


async def test_entry_fills_attach_to_the_intent_group_not_a_new_one(client, store, accounts):
    trade = await client.open_trade_group(
        "main", strategy_type="single", underlying="SPY", bot="bot-1", signal="sig-1",
    )
    await client.record_order(
        OrderInput(account="main", tt_order_id="O-1", underlying="SPY", trade_group=trade.group_id)
    )
    broker = MockTastyTradeClient()
    _fill(broker, order_id="O-1", action="Sell to Open", net_value="250", executed_at=T0)
    await _sync(store, accounts, broker)

    result = await reconcile(store, "main")

    assert result.trade_groups == 0  # nothing clustered -- the fill joined the intent group
    trades = await store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 1
    refreshed = trades[0]
    assert refreshed.group_id == trade.group_id
    assert refreshed.bot_name == "bot-1"
    assert refreshed.total_premium == Decimal("250")  # refined from the actual fill
    assert refreshed.strategy_type == "single"        # submit-time intent preserved


async def test_structure_descriptor_round_trips_and_survives_reconcile(client, store, accounts):
    structure = {
        "legs": [{"action": "Sell to Open", "security_id": "option:SPY:2026-01-16:put:580"}],
        "expiry": "2026-01-16",
        "dte": 11,
    }
    trade = await client.open_trade_group(
        "main", strategy_type="single", underlying="SPY", structure=structure,
    )
    assert trade.structure == structure

    await client.record_order(
        OrderInput(account="main", tt_order_id="O-1", underlying="SPY", trade_group=trade.group_id)
    )
    broker = MockTastyTradeClient()
    _fill(broker, order_id="O-1", action="Sell to Open", net_value="250", executed_at=T0)
    await _sync(store, accounts, broker)

    await reconcile(store, "main")  # financial refresh must not clobber the descriptor

    trades = await store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 1
    assert trades[0].structure == structure


async def test_exit_fills_close_the_intent_group_with_events(client, store, accounts):
    trade = await client.open_trade_group("main", strategy_type="single", underlying="SPY")
    await client.record_order(
        OrderInput(account="main", tt_order_id="O-1", trade_group=trade.group_id)
    )
    await client.record_order(
        OrderInput(account="main", tt_order_id="O-2", trade_group=trade.group_id)
    )
    broker = MockTastyTradeClient()
    _fill(broker, order_id="O-1", action="Sell to Open", net_value="250", executed_at=T0)
    _fill(broker, order_id="O-2", action="Buy to Close", net_value="-100", executed_at=T0 + timedelta(hours=3))
    await _sync(store, accounts, broker)

    result = await reconcile(store, "main")

    assert result.trade_groups == 0
    trades = await store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 1
    closed = trades[0]
    assert closed.status == TradeGroupStatus.CLOSED.value
    assert closed.realized_pnl == Decimal("150")

    pk = await store.get_trade_group_id(trade.group_id)
    events = [ev.event_type for _, ev in store._events.all() if ev.trade_group_id == pk]
    assert events == [TradeGroupEventType.ENTRY.value, TradeGroupEventType.FULL_EXIT.value]


async def test_initial_risk_persists_and_survives_reconcile(client, store, accounts):
    """initial_risk is the planned 1R, frozen at open — distinct from max_loss (an IC
    managed at a 2x-credit stop has 1R = the stop, not the wings), and reconcile's
    financial refresh must never recompute or clobber it."""
    trade = await client.open_trade_group(
        "main", strategy_type="iron_condor", underlying="SPY",
        max_loss=Decimal("1000"), initial_risk=Decimal("400"),
    )
    assert trade.initial_risk == Decimal("400")
    assert trade.max_loss == Decimal("1000")

    await client.record_order(
        OrderInput(account="main", tt_order_id="O-1", underlying="SPY", trade_group=trade.group_id)
    )
    broker = MockTastyTradeClient()
    _fill(broker, order_id="O-1", action="Sell to Open", net_value="250", executed_at=T0)
    await _sync(store, accounts, broker)

    await reconcile(store, "main")

    trades = await store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 1
    assert trades[0].initial_risk == Decimal("400")
    assert trades[0].max_loss == Decimal("1000")


async def test_pnl_net_derives_realized_minus_fees(client, store, accounts):
    """TradeRow.pnl_net is the R numerator: realized_pnl - total_fees, derived (never
    stored), None until the group has realized PnL, missing fees counting as zero."""
    from tt_ledger.rows import TradeRow

    open_row = TradeRow(group_id="g", account="main", origin=Origin.ZTS)
    assert open_row.pnl_net is None

    no_fees = TradeRow(
        group_id="g", account="main", origin=Origin.ZTS, realized_pnl=Decimal("150"),
    )
    assert no_fees.pnl_net == Decimal("150")

    with_fees = TradeRow(
        group_id="g", account="main", origin=Origin.ZTS,
        realized_pnl=Decimal("150"), total_fees=Decimal("12.5"),
    )
    assert with_fees.pnl_net == Decimal("137.5")

    # end-to-end: a closed intent group's pnl_net reflects the reconciled financials
    trade = await client.open_trade_group("main", strategy_type="single", underlying="SPY")
    await client.record_order(OrderInput(account="main", tt_order_id="O-1", trade_group=trade.group_id))
    await client.record_order(OrderInput(account="main", tt_order_id="O-2", trade_group=trade.group_id))
    broker = MockTastyTradeClient()
    _fill(broker, order_id="O-1", action="Sell to Open", net_value="250", executed_at=T0)
    _fill(broker, order_id="O-2", action="Buy to Close", net_value="-100", executed_at=T0 + timedelta(hours=3))
    await _sync(store, accounts, broker)
    await reconcile(store, "main")

    trades = await store.unified_trades(TradeFilter(account="main"))
    closed = trades[0]
    assert closed.pnl_net == closed.realized_pnl - (closed.total_fees or Decimal("0"))


# --------------------------------------------------------------------- synthetic imports (paper)


async def test_import_transactions_settles_a_paper_expiration(client, store, accounts):
    from tt_ledger.ingest.broker import BrokerTransaction

    # a paper entry recorded through the intent path
    trade = await client.open_trade_group("main", strategy_type="single", underlying="SPY")
    await client.record_order(OrderInput(account="main", tt_order_id="O-1", trade_group=trade.group_id))
    broker = MockTastyTradeClient()
    _fill(broker, order_id="O-1", action="Sell to Open", net_value="250", executed_at=T0)
    await _sync(store, accounts, broker)
    await reconcile(store, "main")

    settlement = BrokerTransaction(
        id=f"paper-exp-{PUT_A}-2026-01-16", account_number="ACCT1",
        symbol=PUT_A, instrument_type="Equity Option", underlying_symbol="SPY",
        transaction_type="Receive Deliver", transaction_sub_type="Expiration",
        quantity=Decimal("1"), net_value=Decimal("0"),
        executed_at=T0 + timedelta(days=11), transaction_date=(T0 + timedelta(days=11)).date(),
    )

    result = await client.import_transactions("main", [settlement])
    again = await client.import_transactions("main", [settlement])  # idempotent

    assert result.transactions == 1
    assert again.trade_groups == 0
    trades = await store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 1
    assert trades[0].status == TradeGroupStatus.EXPIRED.value
    assert trades[0].realized_pnl == Decimal("250")
