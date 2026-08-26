"""ONE broker fill closing lots held by SEVERAL groups.

A transaction carries exactly one ``trade_group_id``, so a fill that flattens two groups
lands whole on the first. Two things used to go wrong at once, and this file pins both
ends of the fix plus the operator repair that finishes it.

The fixtures replay ``individual`` groups 4243 / 4727 verbatim (2026-08-26): two put
calendars had their short leg assigned on 08-18 and 08-20, each delivering a long /ESU6 —
at 7800 and at 7745 — into its own broker group. One ``Sell 2 @ 7676.50`` on 08-24
flattened the account.

* the RECEIVING group (4727) held 1 and was handed a close of 2. Unclamped it concluded it
  was SHORT 1 of a contract it had only ever bought, so it never read as fully closed:
  stuck ``open``, ``realized_pnl`` NULL.
* the COVERED sibling (4243) got no row at all and stayed open too.

Together that hid the whole trade. The two groups' true P&L is
``(7676.50 − 7800) × 50 + (7676.50 − 7745) × 50 = −9,600.00``, which is exactly the sum of
the daily ``Money Movement / Mark to Market`` cash TastyTrade actually paid
(−4,300 + 750 − 3,325 − 4,125 + 2,875 − 1,475). Meanwhile the two calendars whose
assignments started it read +4,737.50 and +2,162.50 on their option legs alone — so the
account's realized P&L came out POSITIVE for a losing trade.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.enums import ReviewStatus, TradeGroupStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BrokerTransaction, PlacedLeg, PlacedOrder
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import find_misattributed_open_groups, reconcile
from tt_ledger.ingest.remap import regroup_transactions
from tt_ledger.rows import SecurityRow, TradeFilter
from tt_ledger.store.memory import InMemoryStore

T0 = datetime(2026, 8, 18, 21, 0, tzinfo=UTC)  # 17:00 ET — the first assignment delivery


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
        account_number="ACCT1", symbol="/ESU6", instrument_type="Future",
        underlying_symbol="/ES", quantity=Decimal("1"),
    )
    executed_at = kwargs["executed_at"]
    client.add_transaction(
        BrokerTransaction(**{**defaults, **kwargs, "transaction_date": executed_at.date()})
    )


async def _sync_seed_reconcile(store, accounts, resolver, client):
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    await store.upsert_security(
        SecurityRow(security_id="/ESU6", product_type="F", underlying="/ES",
                    multiplier=50, tt_symbol="/ESU6")
    )
    return await reconcile(store, "main")


async def _trades(store) -> list:
    return sorted(
        await store.unified_trades(TradeFilter(account="main")), key=lambda t: t.executed_at
    )


def _two_deliveries_flattened_by_one_sell(client: MockTastyTradeClient) -> None:
    """Two assignment deliveries on different days, one Sell 2 covering both."""
    _future_txn(client, id="RD-7800", transaction_type="Receive Deliver",
                transaction_sub_type="Buy to Open", action="Buy to Open",
                price=Decimal("7800"), value=Decimal("0"),
                net_value=Decimal("1.76"), net_value_effect="Debit", executed_at=T0)
    _future_txn(client, id="RD-7745", transaction_type="Receive Deliver",
                transaction_sub_type="Buy to Open", action="Buy to Open",
                price=Decimal("7745"), value=Decimal("0"),
                net_value=Decimal("1.76"), net_value_effect="Debit",
                executed_at=T0 + timedelta(days=2))
    sold_at = T0 + timedelta(days=5, hours=18)
    _future_txn(client, id="TXN-SELL2", order_id="O-1", transaction_type="Trade",
                transaction_sub_type="Sell", action="Sell", quantity=Decimal("2"),
                price=Decimal("7676.50"), value=Decimal("1475"), value_effect="Debit",
                net_value=Decimal("1480.38"), net_value_effect="Debit", executed_at=sold_at)
    client.add_order(PlacedOrder(
        id="O-1", account_number="ACCT1", received_at=sold_at, underlying_symbol="/ES",
        status="Filled", terminal_at=sold_at,
        legs=[PlacedLeg(instrument_type="Future", symbol="/ESU6", action="Sell",
                        quantity=Decimal("2"), remaining_quantity=Decimal("0"))],
    ))


async def test_receiving_group_closes_at_what_it_held(store, accounts, resolver):
    """The group handed the whole Sell 2 held only 1: it closes that 1 and books its own
    price P&L. It must NOT read as short 1 — it never opened a short."""
    client = MockTastyTradeClient()
    _two_deliveries_flattened_by_one_sell(client)

    await _sync_seed_reconcile(store, accounts, resolver, client)

    first, second = await _trades(store)
    assert first.status == TradeGroupStatus.CLOSED.value
    # (7676.50 − 7800) × 1 × 50
    assert first.realized_pnl == Decimal("-6175.00")
    # the sibling the same fill also covered got no row, so it is still open
    assert second.status == TradeGroupStatus.OPEN.value
    assert second.realized_pnl is None


async def test_covered_sibling_is_reported_with_its_counterparty(store, accounts, resolver):
    """The residue is exactly what ``find_misattributed_open_groups`` is for — and the
    report names the over-closing group, so the regroup decision needs no hunting."""
    client = MockTastyTradeClient()
    _two_deliveries_flattened_by_one_sell(client)
    await _sync_seed_reconcile(store, accounts, resolver, client)

    first, second = await _trades(store)
    first_pk = await store.get_trade_group_id(first.group_id)
    second_pk = await store.get_trade_group_id(second.group_id)

    found = await find_misattributed_open_groups(store, "main")
    assert [f["group_pk"] for f in found] == [second_pk]
    assert found[0]["securities"] == ["/ESU6"]
    assert found[0]["covered_by"] == [first_pk]


async def test_regroup_merges_the_pair_and_books_the_real_loss(store, accounts, resolver):
    """The operator repair: move the stranded delivery onto the group holding the close.
    One group, both lots, the whole −$9,600 — which is the mark-to-market cash TT paid."""
    client = MockTastyTradeClient()
    _two_deliveries_flattened_by_one_sell(client)
    await _sync_seed_reconcile(store, accounts, resolver, client)

    first, second = await _trades(store)
    second_pk = await store.get_trade_group_id(second.group_id)
    stranded = [
        store._transactions.id_of(t.tt_transaction_id)  # the RD-7745 delivery
        for t in await store.get_group_transactions(second_pk)
    ]

    await regroup_transactions(store, stranded, target_group_id=first.group_id,
                               reviewed_by="test")

    merged = await store.get_trade_group(first.group_id)
    assert merged.status == TradeGroupStatus.CLOSED.value
    # weighted-average open (7800 + 7745)/2 = 7772.50, closed 7676.50, 2 × 50
    assert merged.realized_pnl == Decimal("-9600.00")
    # and the emptied source is no longer a 0-leg ghost in every portfolio view
    emptied = await store.get_trade_group(second.group_id)
    assert emptied.review_status == ReviewStatus.IGNORED
    # nothing left for an operator to untangle
    assert await find_misattributed_open_groups(store, "main") == []


async def test_a_genuine_partial_close_is_unaffected(store, accounts, resolver):
    """Guard on the clamp: a close SMALLER than the group's position still just reduces it.
    Bought 2 @ 7800, sold 1 @ 7676.50 — half off, group stays open."""
    client = MockTastyTradeClient()
    _future_txn(client, id="RD-2LOT", transaction_type="Receive Deliver",
                transaction_sub_type="Buy to Open", action="Buy to Open",
                quantity=Decimal("2"), price=Decimal("7800"), value=Decimal("0"),
                net_value=Decimal("3.52"), net_value_effect="Debit", executed_at=T0)
    sold_at = T0 + timedelta(days=5)
    _future_txn(client, id="TXN-SELL1", order_id="O-1", transaction_type="Trade",
                transaction_sub_type="Sell", action="Sell", quantity=Decimal("1"),
                price=Decimal("7676.50"), value=Decimal("737.50"), value_effect="Debit",
                net_value=Decimal("740.19"), net_value_effect="Debit", executed_at=sold_at)
    client.add_order(PlacedOrder(
        id="O-1", account_number="ACCT1", received_at=sold_at, underlying_symbol="/ES",
        status="Filled", terminal_at=sold_at,
        legs=[PlacedLeg(instrument_type="Future", symbol="/ESU6", action="Sell",
                        quantity=Decimal("1"), remaining_quantity=Decimal("0"))],
    ))

    await _sync_seed_reconcile(store, accounts, resolver, client)

    (trade,) = await _trades(store)
    assert trade.status == TradeGroupStatus.OPEN.value
    # still holding 1 — and NOT reported as misattributed, the account net is genuinely open
    assert await find_misattributed_open_groups(store, "main") == []
