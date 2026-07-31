"""Group-level P&L trio semantics: ``realized_pnl`` GROSS, ``total_fees``
lifecycle-complete, ``pnl_net`` derived (``TradeRow.pnl_net``).

Every other fixture in the suite is feeless, where gross == the broker's net —
which is exactly how the original defect (``realized_pnl`` = Σ signed
``net_value``, i.e. fees already netted, while ``total_fees`` held only the
OPENING cluster's fees) stayed invisible: paper accounts carry no fees, and
every downstream ``realized_pnl - total_fees`` double-counted fees for live
groups. Found 2026-07-30 by the excursion backfill's realized cross-check on
group 2542 (fills +$7.00, fees $4.96, stored 2.04, total_fees 4.48).

These fixtures model that real group's shape: fees on both sides of the
round-trip, including the index-option proprietary fee the old fee sum
dropped.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.enums import TradeGroupStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import reconcile
from tt_ledger.rows import TradeFilter
from tt_ledger.store.memory import InMemoryStore

T0 = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)
CLOSED_AT = T0 + timedelta(hours=4)

PUT_A = "SPY   260116P00580000"


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def resolver() -> PassthroughResolver:
    return PassthroughResolver()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


def _trade(
    client: MockTastyTradeClient,
    *,
    order_id: str,
    action: str,
    net_value: str,
    executed_at: datetime,
    commission: str = "0",
    clearing_fees: str = "0",
    regulatory_fees: str = "0",
    proprietary_index_option_fees: str = "0",
) -> None:
    client.fill(
        account_number="ACCT1", order_id=order_id, symbol=PUT_A,
        instrument_type="Equity Option", action=action, quantity=Decimal("1"),
        fill_price=Decimal("1"), filled_at=executed_at, underlying_symbol="SPY",
    )
    txn = client._transactions["ACCT1"][-1]  # fill() doesn't set cash fields
    txn.net_value = Decimal(net_value)
    txn.commission = Decimal(commission)
    txn.clearing_fees = Decimal(clearing_fees)
    txn.regulatory_fees = Decimal(regulatory_fees)
    txn.proprietary_index_option_fees = Decimal(proprietary_index_option_fees)


async def _sync_and_reconcile(store, accounts, resolver, client, **kwargs):
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    return await reconcile(store, "main", **kwargs)


async def _round_trip_with_fees(store, accounts, resolver):
    """Sold at 250 gross, bought back at 100 gross. Broker nets fees into
    net_value on both sides: open carries 1.00 + 0.10 + 0.02 commission/
    clearing/regulatory, close carries 0.10 + 0.02 + 0.30 with the
    proprietary index-option fee the old fee sum dropped."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", action="Sell to Open", net_value="248.88",
           executed_at=T0, commission="1.00", clearing_fees="0.10", regulatory_fees="0.02")
    _trade(client, order_id="O-2", action="Buy to Close", net_value="-100.42",
           executed_at=CLOSED_AT, clearing_fees="0.10", regulatory_fees="0.02",
           proprietary_index_option_fees="0.30")
    await _sync_and_reconcile(store, accounts, resolver, client)
    return (await store.unified_trades(TradeFilter(account="main")))[0]


async def test_realized_pnl_is_gross_with_complete_fees_beside_it(store, accounts, resolver):
    trade = await _round_trip_with_fees(store, accounts, resolver)

    assert trade.status == TradeGroupStatus.CLOSED.value
    assert trade.realized_pnl == Decimal("150")  # 250 - 100, price moves only
    assert trade.total_fees == Decimal("1.54")  # both sides, all four fee kinds


async def test_pnl_net_reproduces_the_broker_cash(store, accounts, resolver):
    """The trio identity: gross - fees == the signed sum of the broker's own
    net_values. This is what the old semantics broke — realized_pnl WAS the
    cash figure, so subtracting fees again double-counted them."""
    trade = await _round_trip_with_fees(store, accounts, resolver)

    broker_cash = Decimal("248.88") - Decimal("100.42")
    assert trade.pnl_net == broker_cash == Decimal("148.46")


async def test_feeless_group_keeps_gross_equal_to_cash(store, accounts, resolver):
    """Paper accounts carry no fees: gross == net == cash, unchanged by the fix."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", action="Sell to Open", net_value="250", executed_at=T0)
    _trade(client, order_id="O-2", action="Buy to Close", net_value="-100", executed_at=CLOSED_AT)
    await _sync_and_reconcile(store, accounts, resolver, client)

    trade = (await store.unified_trades(TradeFilter(account="main")))[0]
    assert trade.realized_pnl == Decimal("150")
    assert trade.total_fees == Decimal("0")
    assert trade.pnl_net == Decimal("150")


async def test_heal_stamps_gross_and_complete_fees(store, accounts, resolver):
    """The self-heal path shares the trio math with ``_apply_exit``."""
    trade = await _round_trip_with_fees(store, accounts, resolver)
    pk = await store.get_trade_group_id(trade.group_id)
    tg = await store.get_trade_group_by_id(pk)
    await store.upsert_trade_group(
        replace(tg, status="open", closed_at=None, realized_pnl=None, total_fees=None)
    )

    result = await reconcile(store, "main")

    assert result.healed_groups == 1
    healed = await store.get_trade_group_by_id(pk)
    assert healed.realized_pnl == Decimal("150")
    assert healed.total_fees == Decimal("1.54")
