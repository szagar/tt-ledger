"""Reconcile self-heal (``heal_fully_closed_groups``): OPEN groups whose member transactions
already net to zero get their overdue status flip (+ closed_at, realized_pnl, adjustment
event). Targets legacy data where closes attached on a path that skipped ``_apply_exit``
(first seen: paper groups 1023/1024).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.enums import TradeGroupEventType, TradeGroupStatus
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


def _trade(client: MockTastyTradeClient, *, order_id: str, symbol: str, action: str,
           quantity: str, net_value: str, executed_at: datetime) -> None:
    client.fill(
        account_number="ACCT1", order_id=order_id, symbol=symbol, instrument_type="Equity Option",
        action=action, quantity=Decimal(quantity), fill_price=Decimal("1"),
        filled_at=executed_at, underlying_symbol="SPY",
    )
    client._transactions["ACCT1"][-1].net_value = Decimal(net_value)  # fill() doesn't set cash fields


async def _sync_and_reconcile(store, accounts, resolver, client, **kwargs):
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    return await reconcile(store, "main", **kwargs)


async def _trade_and_pk(store):
    trade = (await store.unified_trades(TradeFilter(account="main")))[0]
    return trade, await store.get_trade_group_id(trade.group_id)


async def _make_legacy_stuck_group(store, accounts, resolver):
    """A round-tripped group whose status flip is then undone — mimicking the legacy state
    where the closes attached without ``_apply_exit`` running."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=CLOSED_AT)
    await _sync_and_reconcile(store, accounts, resolver, client)
    trade, pk = await _trade_and_pk(store)
    assert trade.status == TradeGroupStatus.CLOSED.value  # sanity: the normal path closed it
    tg = await store.get_trade_group_by_id(pk)
    await store.upsert_trade_group(replace(tg, status="open", closed_at=None, realized_pnl=None))
    return pk


async def test_heals_open_group_with_fully_closed_content(store, accounts, resolver):
    pk = await _make_legacy_stuck_group(store, accounts, resolver)

    result = await reconcile(store, "main")

    assert result.healed_groups == 1
    tg = await store.get_trade_group_by_id(pk)
    assert tg.status == TradeGroupStatus.CLOSED.value
    assert tg.closed_at == CLOSED_AT  # the group's last activity
    assert tg.realized_pnl == Decimal("150")  # 250 credit - 100 debit, cash basis
    events = [ev for _, ev in store._events.all() if ev.trade_group_id == pk]
    heal_events = [ev for ev in events if ev.event_type == TradeGroupEventType.ADJUSTMENT.value]
    assert len(heal_events) == 1
    assert heal_events[0].event_at == CLOSED_AT
    assert "self-heal" in heal_events[0].notes

    rerun = await reconcile(store, "main")  # healed group left the open set
    assert rerun.healed_groups == 0
    assert len([ev for _, ev in store._events.all()
                if ev.trade_group_id == pk
                and ev.event_type == TradeGroupEventType.ADJUSTMENT.value]) == 1


async def test_open_content_is_left_alone(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="2",
           net_value="500", executed_at=T0)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Close", quantity="1",
           net_value="-100", executed_at=CLOSED_AT)  # partial: net 1 still open

    result = await _sync_and_reconcile(store, accounts, resolver, client)

    assert result.healed_groups == 0
    trade, _ = await _trade_and_pk(store)
    assert trade.status == TradeGroupStatus.OPEN.value


async def test_dry_run_counts_but_writes_nothing(store, accounts, resolver):
    pk = await _make_legacy_stuck_group(store, accounts, resolver)

    result = await reconcile(store, "main", dry_run=True)

    assert result.healed_groups == 1
    tg = await store.get_trade_group_by_id(pk)
    assert tg.status == TradeGroupStatus.OPEN.value
    assert tg.closed_at is None
    assert [ev for _, ev in store._events.all()
            if ev.trade_group_id == pk and ev.event_type == TradeGroupEventType.ADJUSTMENT.value] == []
