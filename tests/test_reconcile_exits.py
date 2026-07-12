"""Reconcile exit/roll linking (docs/ingestion.md → Reconcile, lifecycle extension).

Closing activity (``* to Close`` trades, Receive Deliver expiration/assignment/exercise)
attaches to the open group it offsets — with the matching lifecycle event, status flip, and
cash-basis realized_pnl — instead of clustering into a bogus new "entry" group. Rolls link
old → new group via a ``roll`` event.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.enums import TradeGroupEventType, TradeGroupStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BrokerTransaction, PlacedLeg, PlacedOrder
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import reconcile
from tt_ledger.rows import TradeFilter
from tt_ledger.store.memory import InMemoryStore

T0 = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)

PUT_A = "SPY   260116P00580000"
PUT_B = "SPY   260220P00575000"
CALL_A = "SPY   260116C00610000"


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def resolver() -> PassthroughResolver:
    return PassthroughResolver()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


def _trade(client: MockTastyTradeClient, *, order_id: str, symbol: str, action: str,
           quantity: str, net_value: str, executed_at: datetime, underlying: str = "SPY") -> None:
    client.fill(
        account_number="ACCT1", order_id=order_id, symbol=symbol, instrument_type="Equity Option",
        action=action, quantity=Decimal(quantity), fill_price=Decimal("1"),
        filled_at=executed_at, underlying_symbol=underlying,
    )
    client._transactions["ACCT1"][-1].net_value = Decimal(net_value)  # fill() doesn't set cash fields


def _receive_deliver(client: MockTastyTradeClient, *, txn_id: str, symbol: str, sub_type: str,
                     quantity: str, net_value: str, executed_at: datetime, underlying: str = "SPY") -> None:
    client.add_transaction(
        BrokerTransaction(
            id=txn_id, account_number="ACCT1", order_id=None, underlying_symbol=underlying,
            symbol=symbol, instrument_type="Equity Option", transaction_type="Receive Deliver",
            transaction_sub_type=sub_type, action=None, quantity=Decimal(quantity),
            net_value=Decimal(net_value), executed_at=executed_at, transaction_date=executed_at.date(),
        )
    )


async def _sync_and_reconcile(store, accounts, resolver, client):
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    return await reconcile(store, "main")


async def _events(store, group_id: str) -> list:
    pk = await store.get_trade_group_id(group_id)
    return [ev for _, ev in store._events.all() if ev.trade_group_id == pk]


async def _trades(store) -> list:
    return await store.unified_trades(TradeFilter(account="main"))


# --------------------------------------------------------------------- exits


async def test_full_exit_attaches_to_entry_group_and_closes_it(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=T0 + timedelta(hours=4))

    result = await _sync_and_reconcile(store, accounts, resolver, client)

    assert result.trade_groups == 1  # the exit did NOT become a second group
    trades = await _trades(store)
    assert len(trades) == 1
    trade = trades[0]
    assert trade.status == TradeGroupStatus.CLOSED.value
    assert trade.realized_pnl == Decimal("150")  # 250 credit - 100 debit, cash basis
    assert trade.closed_at == T0 + timedelta(hours=4)

    events = await _events(store, trade.group_id)
    assert [e.event_type for e in events] == [TradeGroupEventType.ENTRY.value, TradeGroupEventType.FULL_EXIT.value]
    exit_event = events[-1]
    assert exit_event.quantity_change == Decimal("-1")
    assert exit_event.premium_change == Decimal("-100")


async def test_partial_exit_keeps_the_group_open(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="2",
           net_value="500", executed_at=T0)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=T0 + timedelta(hours=1))

    await _sync_and_reconcile(store, accounts, resolver, client)

    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.OPEN.value
    events = await _events(store, trade.group_id)
    assert [e.event_type for e in events] == [TradeGroupEventType.ENTRY.value, TradeGroupEventType.PARTIAL_EXIT.value]

    # the remaining lot closes later -> FULL_EXIT + closed
    client2 = MockTastyTradeClient()
    _trade(client2, order_id="O-3", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-50", executed_at=T0 + timedelta(hours=2))
    await _sync_and_reconcile(store, accounts, resolver, client2)

    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.CLOSED.value
    assert trade.realized_pnl == Decimal("350")
    events = await _events(store, trade.group_id)
    assert [e.event_type for e in events][-1] == TradeGroupEventType.FULL_EXIT.value


async def test_expiration_receive_deliver_expires_the_group(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _receive_deliver(client, txn_id="RD-1", symbol=PUT_A, sub_type="Expiration", quantity="1",
                     net_value="0", executed_at=T0 + timedelta(days=11))

    result = await _sync_and_reconcile(store, accounts, resolver, client)

    assert result.trade_groups == 1
    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.EXPIRED.value
    assert trade.realized_pnl == Decimal("250")  # full credit kept
    events = await _events(store, trade.group_id)
    assert [e.event_type for e in events] == [TradeGroupEventType.ENTRY.value, TradeGroupEventType.EXPIRATION.value]


async def test_mixed_close_causes_flag_the_group_mixed(store, accounts, resolver):
    client = MockTastyTradeClient()
    # strangle entry: both legs in one cluster -> one group
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _trade(client, order_id="O-2", symbol=CALL_A, action="Sell to Open", quantity="1",
           net_value="200", executed_at=T0 + timedelta(seconds=1))
    # put leg bought back; call leg assigned
    _trade(client, order_id="O-3", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-50", executed_at=T0 + timedelta(days=1))
    _receive_deliver(client, txn_id="RD-1", symbol=CALL_A, sub_type="Assignment", quantity="1",
                     net_value="0", executed_at=T0 + timedelta(days=2))

    result = await _sync_and_reconcile(store, accounts, resolver, client)

    assert result.trade_groups == 1
    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.MIXED.value
    events = await _events(store, trade.group_id)
    assert [e.event_type for e in events] == [
        TradeGroupEventType.ENTRY.value,
        TradeGroupEventType.PARTIAL_EXIT.value,
        TradeGroupEventType.ASSIGNMENT.value,
    ]


async def test_close_with_no_matching_open_group_becomes_its_own_group(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=T0)

    result = await _sync_and_reconcile(store, accounts, resolver, client)

    assert result.trade_groups == 1  # history doesn't reach the entry -- still visible, own group
    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.OPEN.value


# --------------------------------------------------------------------- rolls


async def test_same_cluster_roll_links_old_group_to_new(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    # one cluster: close old put + open next month's put
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=T0 + timedelta(days=7))
    _trade(client, order_id="O-3", symbol=PUT_B, action="Sell to Open", quantity="1",
           net_value="300", executed_at=T0 + timedelta(days=7, seconds=2))

    result = await _sync_and_reconcile(store, accounts, resolver, client)

    assert result.trade_groups == 2  # entry group + rolled-to group
    trades = sorted(await _trades(store), key=lambda t: t.executed_at)
    old, new = trades
    assert old.status == TradeGroupStatus.CLOSED.value
    assert new.status == TradeGroupStatus.OPEN.value

    old_events = await _events(store, old.group_id)
    roll = next(e for e in old_events if e.event_type == TradeGroupEventType.ROLL.value)
    assert roll.rolled_to_group_id == await store.get_trade_group_id(new.group_id)


async def test_cross_cluster_roll_within_tolerance_links_groups(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    # two separate orders 30s apart (beyond the 5s cluster window, inside the 60s roll window)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=T0 + timedelta(days=7))
    _trade(client, order_id="O-3", symbol=PUT_B, action="Sell to Open", quantity="1",
           net_value="300", executed_at=T0 + timedelta(days=7, seconds=30))

    await _sync_and_reconcile(store, accounts, resolver, client)

    trades = sorted(await _trades(store), key=lambda t: t.executed_at)
    old, new = trades
    old_events = await _events(store, old.group_id)
    roll = next(e for e in old_events if e.event_type == TradeGroupEventType.ROLL.value)
    assert roll.rolled_to_group_id == await store.get_trade_group_id(new.group_id)


# --------------------------------------------------------------------- idempotency


async def test_reconcile_is_idempotent_over_exits(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=T0 + timedelta(hours=4))

    await _sync_and_reconcile(store, accounts, resolver, client)
    again = await reconcile(store, "main")

    assert again.trade_groups == 0
    trades = await _trades(store)
    assert len(trades) == 1
    events = await _events(store, trades[0].group_id)
    assert len(events) == 2  # ENTRY + FULL_EXIT, not duplicated


# --------------------------------------------------------------------- assignment deliveries


async def test_assignment_delivery_forms_linked_group_and_bare_cover_closes_it(store, accounts, resolver):
    """The trade-550 shape from the live rehearsal: a short call is assigned, delivering a
    short future (Receive Deliver / Sell to Open, no order id); the next day a bare
    Trade/Buy covers it. The delivery must form its own group, CONTINUATION-LINKED from the
    option group's assignment event (rolled_to_group_id), and the bare cover must close the
    delivery group with real P&L -- not open a third phantom group."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="500", executed_at=T0)
    # assignment settles the option and delivers the underlying at the same instant
    _receive_deliver(client, txn_id="RD-A", symbol=PUT_A, sub_type="Assignment", quantity="1",
                     net_value="-0.44", executed_at=T0 + timedelta(days=6), underlying="/ESM6")
    client.add_transaction(BrokerTransaction(
        id="RD-DLV", account_number="ACCT1", symbol="/ESM6", instrument_type="Future",
        underlying_symbol="/ES", transaction_type="Receive Deliver", transaction_sub_type="Sell to Open",
        action="Sell to Open", quantity=Decimal("1"), price=Decimal("7350"),
        executed_at=T0 + timedelta(days=6), transaction_date=(T0 + timedelta(days=6)).date(),
    ))
    # bare futures Buy covers the delivered short the next day
    client.add_transaction(BrokerTransaction(
        id="TXN-COVER", account_number="ACCT1", order_id="O-2", symbol="/ESM6",
        instrument_type="Future", underlying_symbol="/ES", transaction_type="Trade",
        transaction_sub_type="Buy", action="Buy", quantity=Decimal("1"), price=Decimal("7421.25"),
        net_value=Decimal("-3562.50"), net_value_effect=None,
        executed_at=T0 + timedelta(days=7), transaction_date=(T0 + timedelta(days=7)).date(),
    ))
    client.add_order(PlacedOrder(
        id="O-2", account_number="ACCT1", received_at=T0 + timedelta(days=7),
        underlying_symbol="/ES", status="Filled", terminal_at=T0 + timedelta(days=7),
        legs=[PlacedLeg(instrument_type="Future", symbol="/ESM6", action="Buy",
                        quantity=Decimal("1"), remaining_quantity=Decimal("0"))],
    ))

    await _sync_and_reconcile(store, accounts, resolver, client)

    trades = sorted(await _trades(store), key=lambda t: t.executed_at)
    assert len(trades) == 2, [t.underlying for t in trades]
    option_group, future_group = trades

    # option group: assigned, with the continuation link on its assignment event
    assert option_group.status == TradeGroupStatus.ASSIGNED.value
    option_events = await _events(store, option_group.group_id)
    assignment = next(e for e in option_events if e.event_type == TradeGroupEventType.ASSIGNMENT.value)
    future_pk = await store.get_trade_group_id(future_group.group_id)
    assert assignment.rolled_to_group_id == future_pk

    # delivery group: closed by the bare cover, cash-basis P&L booked
    assert future_group.status == TradeGroupStatus.CLOSED.value
    assert future_group.realized_pnl == Decimal("-3562.50")
    future_events = await _events(store, future_group.group_id)
    assert TradeGroupEventType.FULL_EXIT.value in [e.event_type for e in future_events]


# ------------------------------------------------- net-aware close routing (2026-07-12 fix)


async def test_same_instant_closes_spread_across_holding_groups(store, accounts, resolver):
    """The group-251/252 regression: two same-instant settlements, one lot each in two open
    groups — first-match membership routing piled both onto the first group and left the
    second stuck open forever. Net-aware routing draws down each group's remaining net, so
    the second row moves on to the second group."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Buy to Open", quantity="1",
           net_value="-100", executed_at=T0)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Open", quantity="1",
           net_value="-105", executed_at=T0 + timedelta(minutes=10))  # own cluster -> own group
    _receive_deliver(client, txn_id="RD-1", symbol=PUT_A, sub_type="Expiration", quantity="1",
                     net_value="0", executed_at=T0 + timedelta(days=11))
    _receive_deliver(client, txn_id="RD-2", symbol=PUT_A, sub_type="Expiration", quantity="1",
                     net_value="0", executed_at=T0 + timedelta(days=11))

    result = await _sync_and_reconcile(store, accounts, resolver, client)

    assert result.trade_groups == 2  # the settlements created nothing new
    trades = await _trades(store)
    assert [t.status for t in trades] == [TradeGroupStatus.EXPIRED.value] * 2
    for trade in trades:
        events = await _events(store, trade.group_id)
        expirations = [e for e in events if e.event_type == TradeGroupEventType.EXPIRATION.value]
        assert len(expirations) == 1  # exactly ONE settlement each, not both on the first group
        assert expirations[0].quantity_change == Decimal("-1")


async def test_close_routes_to_the_group_with_offsetting_direction(store, accounts, resolver):
    """A ``Buy to Close`` offsets a SHORT net: it must route to the short group even when a
    long group holding the same contract comes first."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Buy to Open", quantity="1",
           net_value="-100", executed_at=T0)  # first group: LONG
    _trade(client, order_id="O-2", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0 + timedelta(minutes=10))  # second group: SHORT
    _trade(client, order_id="O-3", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-90", executed_at=T0 + timedelta(hours=2))

    await _sync_and_reconcile(store, accounts, resolver, client)

    trades = sorted(await _trades(store), key=lambda t: t.executed_at)
    long_group, short_group = trades
    assert long_group.status == TradeGroupStatus.OPEN.value  # untouched
    assert short_group.status == TradeGroupStatus.CLOSED.value
    assert short_group.realized_pnl == Decimal("160")  # 250 credit - 90 debit


async def test_overclose_falls_back_to_membership(store, accounts, resolver):
    """A close no group has net for (broker over-close / window artifact) still attaches to
    the group whose membership includes the security — old behavior preserved; it must NOT
    orphan into a junk rest-group."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=T0 + timedelta(hours=1))
    _trade(client, order_id="O-3", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=T0 + timedelta(hours=1))  # same cluster, over-closes

    result = await _sync_and_reconcile(store, accounts, resolver, client)

    assert result.trade_groups == 1  # no junk group from the over-close
    trades = await _trades(store)
    assert len(trades) == 1
