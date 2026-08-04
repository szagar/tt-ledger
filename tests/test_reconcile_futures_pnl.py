"""Bare-futures group ``realized_pnl`` is PRICE-based, never transaction-value based.

TastyTrade pays futures P/L through daily ``Money Movement / Mark to Market`` rows.
Those are position-level and never become trade-group members, so a futures group's
member transactions carry only settlement-relative fragments — a Trade's value is
(price − prior settlement) × multiplier, and a Receive Deliver delivery's value is
intrinsic vs that day's settle. Summing them (the pre-2026-08-04 semantics) booked
economically meaningless numbers on every futures group held across a 17:00 ET
settlement.

These fixtures replay the real ``individual`` groups that surfaced the defect:

* group 672 — a short call assignment delivered short /ESM6 @ 6600 (04-10); a long
  call exercise covered it @ 6625 (04-13). The cover's transaction value was
  +14,887.50 (settle 6922.75 vs 6625) for a trade that lost $1,250.
* group 745 — delivered long @ 7575, sold @ 7451 next morning: value +675 (vs the
  7437.50 settle) for a $6,200 loss.

Same-day round trips were never wrong (the settlement reference cancels), pinned
here so the fix provably changes nothing for them.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.enums import TradeGroupEventType, TradeGroupStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BrokerTransaction, PlacedLeg, PlacedOrder
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import reconcile, recompute_futures_group_pnl
from tt_ledger.rows import SecurityRow, TradeFilter
from tt_ledger.store.memory import InMemoryStore

T0 = datetime(2026, 4, 10, 21, 0, tzinfo=UTC)  # 17:00 ET settlement


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def resolver() -> PassthroughResolver:
    return PassthroughResolver()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


def _future_txn(client: MockTastyTradeClient, **kwargs) -> None:
    defaults = dict(
        account_number="ACCT1", symbol="/ESM6", instrument_type="Future",
        underlying_symbol="/ES", quantity=Decimal("1"),
    )
    executed_at = kwargs["executed_at"]
    client.add_transaction(
        BrokerTransaction(**{**defaults, **kwargs, "transaction_date": executed_at.date()})
    )


async def _sync_seed_reconcile(store, accounts, resolver, client):
    """Sync, then stamp the futures multiplier on the securities dimension (the
    passthrough resolver carries none), then reconcile — the order the live path
    produces (the zts resolver populates multiplier during sync)."""
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    await store.upsert_security(
        SecurityRow(security_id="/ESM6", product_type="F", underlying="/ES",
                    multiplier=50, tt_symbol="/ESM6")
    )
    return await reconcile(store, "main")


async def _trades(store) -> list:
    return await store.unified_trades(TradeFilter(account="main"))


def _delivered_short_covered_by_exercise(client: MockTastyTradeClient) -> None:
    """The group-672 rows verbatim: assignment delivers short @ 6600 (value 0), an
    exercise covers @ 6625 with the intrinsic-vs-settle credit on the transaction."""
    _future_txn(client, id="RD-OPEN", transaction_type="Receive Deliver",
                transaction_sub_type="Sell to Open", action="Sell to Open",
                price=Decimal("6600"), value=Decimal("0"),
                net_value=Decimal("1.77"), net_value_effect="Debit", executed_at=T0)
    _future_txn(client, id="RD-CLOSE", transaction_type="Receive Deliver",
                transaction_sub_type="Buy to Close", action="Buy to Close",
                price=Decimal("6625"), value=Decimal("14887.50"), value_effect="Credit",
                net_value=Decimal("14885.73"), net_value_effect="Credit",
                executed_at=T0 + timedelta(days=3))


async def test_delivery_round_trip_books_price_pnl_not_intrinsic_credit(store, accounts, resolver):
    """Short 6600 → cover 6625 is −$1,250 at 50×, no matter that the cover's
    transaction value was +14,887.50."""
    client = MockTastyTradeClient()
    _delivered_short_covered_by_exercise(client)

    await _sync_seed_reconcile(store, accounts, resolver, client)

    (trade,) = await _trades(store)
    assert trade.status == TradeGroupStatus.CLOSED.value
    assert trade.realized_pnl == Decimal("-1250")


async def test_overnight_long_sold_next_morning_books_price_pnl(store, accounts, resolver):
    """The group-745 shape: delivered long @ 7575, sold @ 7451 — the sell's value is
    +675 (vs the 7437.50 settle), the trade lost $6,200."""
    client = MockTastyTradeClient()
    _future_txn(client, id="RD-OPEN", transaction_type="Receive Deliver",
                transaction_sub_type="Buy to Open", action="Buy to Open",
                price=Decimal("7575"), value=Decimal("0"),
                net_value=Decimal("1.77"), net_value_effect="Debit", executed_at=T0)
    _future_txn(client, id="TXN-SELL", order_id="O-1", transaction_type="Trade",
                transaction_sub_type="Sell", action="Sell",
                price=Decimal("7451"), value=Decimal("675"), value_effect="Credit",
                net_value=Decimal("672.30"), net_value_effect="Credit",
                executed_at=T0 + timedelta(hours=14))
    client.add_order(PlacedOrder(
        id="O-1", account_number="ACCT1", received_at=T0 + timedelta(hours=14),
        underlying_symbol="/ES", status="Filled", terminal_at=T0 + timedelta(hours=14),
        legs=[PlacedLeg(instrument_type="Future", symbol="/ESM6", action="Sell",
                        quantity=Decimal("1"), remaining_quantity=Decimal("0"))],
    ))

    await _sync_seed_reconcile(store, accounts, resolver, client)

    (trade,) = await _trades(store)
    assert trade.status == TradeGroupStatus.CLOSED.value
    assert trade.realized_pnl == Decimal("-6200")


async def test_same_day_round_trip_unchanged_by_price_basis(store, accounts, resolver):
    """Intraday futures were never wrong — the two settlement-relative values sum to
    the price move. Bought @ 6800, sold @ 6810 with a 6795 prior settle: values −250
    and +750, price basis (6810 − 6800) × 50 — both +$500."""
    client = MockTastyTradeClient()
    _future_txn(client, id="TXN-BUY", order_id="O-1", transaction_type="Trade",
                transaction_sub_type="Buy to Open", action="Buy to Open",
                price=Decimal("6800"), value=Decimal("250"), value_effect="Debit",
                net_value=Decimal("250"), net_value_effect="Debit", executed_at=T0)
    _future_txn(client, id="TXN-SELL", order_id="O-2", transaction_type="Trade",
                transaction_sub_type="Sell to Close", action="Sell to Close",
                price=Decimal("6810"), value=Decimal("750"), value_effect="Credit",
                net_value=Decimal("750"), net_value_effect="Credit",
                executed_at=T0 + timedelta(hours=2))
    for order_id, action, at in (("O-1", "Buy to Open", T0), ("O-2", "Sell to Close", T0 + timedelta(hours=2))):
        client.add_order(PlacedOrder(
            id=order_id, account_number="ACCT1", received_at=at,
            underlying_symbol="/ES", status="Filled", terminal_at=at,
            legs=[PlacedLeg(instrument_type="Future", symbol="/ESM6", action=action,
                            quantity=Decimal("1"), remaining_quantity=Decimal("0"))],
        ))

    await _sync_seed_reconcile(store, accounts, resolver, client)

    (trade,) = await _trades(store)
    assert trade.status == TradeGroupStatus.CLOSED.value
    assert trade.realized_pnl == Decimal("500")


# ------------------------------------------------------------------- recompute backfill


async def test_recompute_repairs_pre_fix_stamps_and_is_idempotent(store, accounts, resolver):
    client = MockTastyTradeClient()
    _delivered_short_covered_by_exercise(client)
    await _sync_seed_reconcile(store, accounts, resolver, client)

    # simulate a group stamped under the old Σ-signed-gross semantics
    (trade,) = await _trades(store)
    pk = await store.get_trade_group_id(trade.group_id)
    tg = await store.get_trade_group_by_id(pk)
    await store.upsert_trade_group(replace(tg, realized_pnl=Decimal("14887.50")))

    previewed = await recompute_futures_group_pnl(store, "main", dry_run=True)
    assert [(c["group_pk"], c["old"], c["new"]) for c in previewed] == [
        (pk, Decimal("14887.50"), Decimal("-1250")),
    ]
    assert (await store.get_trade_group_by_id(pk)).realized_pnl == Decimal("14887.50")  # dry-run wrote nothing

    changed = await recompute_futures_group_pnl(store, "main")
    assert len(changed) == 1
    assert (await store.get_trade_group_by_id(pk)).realized_pnl == Decimal("-1250")
    events = [ev for _, ev in store._events.all() if ev.trade_group_id == pk]
    adjustments = [e for e in events if e.event_type == TradeGroupEventType.ADJUSTMENT.value]
    assert len(adjustments) == 1 and "14887.50 -> -1250" in adjustments[0].notes

    assert await recompute_futures_group_pnl(store, "main") == []  # second run: nothing left


async def test_recompute_never_touches_premium_settled_groups(store, accounts, resolver):
    """Surgical scope: a mis-stamped options group is NOT repaired by this pass —
    futures-containing groups only."""
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="SPY   260116P00580000",
        instrument_type="Equity Option", action="Sell to Open", quantity=Decimal("1"),
        fill_price=Decimal("1"), filled_at=T0, underlying_symbol="SPY",
    )
    client._transactions["ACCT1"][-1].net_value = Decimal("250")
    client.fill(
        account_number="ACCT1", order_id="O-2", symbol="SPY   260116P00580000",
        instrument_type="Equity Option", action="Buy to Close", quantity=Decimal("1"),
        fill_price=Decimal("1"), filled_at=T0 + timedelta(days=1), underlying_symbol="SPY",
    )
    client._transactions["ACCT1"][-1].net_value = Decimal("-100")
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    await reconcile(store, "main")

    (trade,) = await _trades(store)
    pk = await store.get_trade_group_id(trade.group_id)
    tg = await store.get_trade_group_by_id(pk)
    await store.upsert_trade_group(replace(tg, realized_pnl=Decimal("999")))

    assert await recompute_futures_group_pnl(store, "main") == []
    assert (await store.get_trade_group_by_id(pk)).realized_pnl == Decimal("999")
