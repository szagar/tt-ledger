"""``link_order_to_group`` — manual attachment of an unlinked (broker-entered) order to a
trade group, with fills following the order's group from then on.

The motivating case (zts cockpit, 2026-07-16): an operator manually enters a closing order
at the broker for a group the engine lost track of. The order syncs in with
``origin=broker`` and ``trade_group_id=None``; linking it must (a) survive broker resyncs
of the still-working order, and (b) route its eventual fills to the linked group via the
same pre-attributed-order path OMS-submitted orders use.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tt_ledger.enums import ReviewStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BrokerTransaction, PlacedOrder
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import reconcile
from tt_ledger.ingest.remap import link_order_to_group
from tt_ledger.rows import TradeFilter
from tt_ledger.store.memory import InMemoryStore

T0 = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1", "other": "ACCT2"})


@pytest.fixture
def resolver() -> PassthroughResolver:
    return PassthroughResolver()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


async def _sync(store, account, client, accounts, resolver):
    await sync_orders(store, account, client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, account, client=client, accounts=accounts, resolver=resolver)


def _working_order(order_id: str, symbol: str, *, account_number: str = "ACCT1") -> PlacedOrder:
    """A Live (unfilled) order as the broker order-history reports it."""
    return PlacedOrder(
        id=order_id, account_number=account_number, received_at=T0,
        underlying_symbol=symbol, order_type="Limit", price=Decimal("1.00"),
        price_effect="Debit", status="Live",
    )


async def _open_group(store, accounts, resolver, *, order_id="O-ENTRY", symbol="AAPL"):
    """Reconcile a broker entry fill into an OPEN group; returns the TradeRow."""
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id=order_id, symbol=symbol, instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=T0, status="Filled",
    )
    await _sync(store, "main", client, accounts, resolver)
    await reconcile(store, "main")
    return (await store.unified_trades(TradeFilter(account="main")))[-1]


# --- linking ---------------------------------------------------------------------------------


async def test_link_to_existing_group_stamps_order_and_events(store, accounts, resolver):
    trade = await _open_group(store, accounts, resolver)
    client = MockTastyTradeClient()
    client.add_order(_working_order("O-MANUAL", "AAPL"))
    await _sync(store, "main", client, accounts, resolver)

    result = await link_order_to_group(
        store, "O-MANUAL", target_group_id=trade.group_id, reviewed_by="alice"
    )

    order = await store.get_order("O-MANUAL")
    assert order.trade_group_id == store._trade_groups.id_of(trade.group_id)
    assert result.group_id == trade.group_id
    assert result.manually_attributed is True

    persisted = await store.get_trade_group(trade.group_id)
    assert persisted.reviewed_by == "alice"
    events = [
        ev for _, ev in store._events.all()
        if ev.trade_group_id == store._trade_groups.id_of(trade.group_id)
    ]
    assert any(ev.event_type == "adjustment" and "O-MANUAL" in (ev.notes or "") for ev in events)


async def test_link_to_new_group_creates_needs_review_group(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.add_order(_working_order("O-MANUAL", "MSFT"))
    await _sync(store, "main", client, accounts, resolver)

    result = await link_order_to_group(store, "O-MANUAL", target_group_id=None, reviewed_by="alice")

    assert result.account == "main"
    assert result.manually_attributed is True
    assert result.review_status is ReviewStatus.NEEDS_REVIEW
    order = await store.get_order("O-MANUAL")
    assert order.trade_group_id == store._trade_groups.id_of(result.group_id)


async def test_link_unknown_order_raises(store):
    with pytest.raises(ValueError, match="not found"):
        await link_order_to_group(store, "O-NOPE", target_group_id=None, reviewed_by="alice")


async def test_link_already_linked_order_raises(store, accounts, resolver):
    trade = await _open_group(store, accounts, resolver)
    with pytest.raises(ValueError, match="already linked"):
        await link_order_to_group(
            store, "O-ENTRY", target_group_id=trade.group_id, reviewed_by="alice"
        )


async def test_link_across_accounts_raises(store, accounts, resolver):
    trade = await _open_group(store, accounts, resolver)  # account "main"
    client = MockTastyTradeClient()
    client.add_order(_working_order("O-OTHER", "AAPL", account_number="ACCT2"))
    await _sync(store, "other", client, accounts, resolver)

    with pytest.raises(ValueError, match="belongs to account"):
        await link_order_to_group(
            store, "O-OTHER", target_group_id=trade.group_id, reviewed_by="alice"
        )


# --- fills follow the linked order -----------------------------------------------------------


async def test_later_fills_route_to_the_linked_group_not_a_new_one(store, accounts, resolver):
    """Link a Live order to a brand-new group; when its opening fill lands later, it must
    attach to THAT group via the pre-attributed-order path — heuristic reconcile would
    otherwise have created a fresh group for it (no open group on this symbol exists)."""
    client = MockTastyTradeClient()
    client.add_order(_working_order("O-MANUAL", "MSFT"))
    await _sync(store, "main", client, accounts, resolver)

    linked = await link_order_to_group(store, "O-MANUAL", target_group_id=None, reviewed_by="alice")
    linked_pk = store._trade_groups.id_of(linked.group_id)
    groups_before = len(list(store._trade_groups.all()))

    # The order fills at the broker; the next sync cycle picks it up.
    later = datetime(2026, 1, 5, 16, 0, tzinfo=UTC)
    client.fill(
        account_number="ACCT1", order_id="O-MANUAL", symbol="MSFT", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("400"),
        filled_at=later, status="Filled",
    )
    await _sync(store, "main", client, accounts, resolver)
    await reconcile(store, "main")

    txns = [t for _, t in store._transactions.all() if t.tt_order_id == "O-MANUAL"]
    assert txns and all(t.trade_group_id == linked_pk for t in txns)
    assert len(list(store._trade_groups.all())) == groups_before  # no heuristic group created


async def test_closing_fill_on_linked_order_closes_the_group(store, accounts, resolver):
    """The 2026-07-16 shape: an open group, a manual broker close linked to it — the close's
    fills must run the exit machinery against that group."""
    trade = await _open_group(store, accounts, resolver, symbol="XSP")
    client = MockTastyTradeClient()
    client.add_order(_working_order("O-CLOSE", "XSP"))
    await _sync(store, "main", client, accounts, resolver)
    await link_order_to_group(store, "O-CLOSE", target_group_id=trade.group_id, reviewed_by="alice")

    later = datetime(2026, 1, 5, 17, 0, tzinfo=UTC)
    client.fill(
        account_number="ACCT1", order_id="O-CLOSE", symbol="XSP", instrument_type="Equity",
        action="Sell to Close", quantity=Decimal("10"), fill_price=Decimal("160"),
        filled_at=later, status="Filled",
    )
    await _sync(store, "main", client, accounts, resolver)
    await reconcile(store, "main")

    tg = await store.get_trade_group(trade.group_id)
    assert tg.status == "closed"
    txns = [t for _, t in store._transactions.all() if t.tt_order_id == "O-CLOSE"]
    assert txns and all(t.trade_group_id == store._trade_groups.id_of(trade.group_id) for t in txns)


async def test_already_synced_ungrouped_fills_attach_on_link(store, accounts, resolver):
    """Fills that synced before the link (still ungrouped) attach during link_order's own
    reconcile pass — no waiting for the next sync."""
    client = MockTastyTradeClient()
    order = _working_order("O-MANUAL", "NVDA")
    order.status = "Filled"
    client.add_order(order)
    client.add_transaction(
        BrokerTransaction(
            id="TXN-O-MANUAL", account_number="ACCT1", order_id="O-MANUAL",
            underlying_symbol="NVDA", symbol="NVDA", instrument_type="Equity",
            transaction_type="Trade", action="Buy to Open", quantity=Decimal("5"),
            price=Decimal("800"), executed_at=T0, transaction_date=T0.date(),
        )
    )
    await _sync(store, "main", client, accounts, resolver)
    # NOTE: no reconcile between sync and link — the txn is still ungrouped.

    linked = await link_order_to_group(store, "O-MANUAL", target_group_id=None, reviewed_by="alice")

    txns = [t for _, t in store._transactions.all() if t.tt_order_id == "O-MANUAL"]
    assert txns and all(
        t.trade_group_id == store._trade_groups.id_of(linked.group_id) for t in txns
    )


# --- the link must survive broker resyncs ------------------------------------------------------


async def test_resync_of_working_order_preserves_the_link(store, accounts, resolver):
    """A Live order is re-upserted on every sync cycle with trade_group_id=None from the
    broker's perspective; the preserve-if-null upsert must keep the manual link."""
    client = MockTastyTradeClient()
    client.add_order(_working_order("O-MANUAL", "AMD"))
    await _sync(store, "main", client, accounts, resolver)
    linked = await link_order_to_group(store, "O-MANUAL", target_group_id=None, reviewed_by="alice")
    linked_pk = store._trade_groups.id_of(linked.group_id)

    await _sync(store, "main", client, accounts, resolver)  # broker resync, order still Live

    order = await store.get_order("O-MANUAL")
    assert order.trade_group_id == linked_pk


# --- the unlinked-orders read (cockpit queue) ---------------------------------------------------


async def test_unlinked_orders_returns_only_ungrouped_with_legs(store, accounts, resolver):
    await _open_group(store, accounts, resolver)  # linked via reconcile
    client = MockTastyTradeClient()
    order = _working_order("O-MANUAL", "XSP")
    from tt_ledger.ingest.broker import PlacedLeg

    order.legs = [
        PlacedLeg(instrument_type="Equity Option", symbol="XSP   260116P00753000",
                  action="Buy to Close", quantity=Decimal("1"), remaining_quantity=Decimal("1")),
    ]
    client.add_order(order)
    await _sync(store, "main", client, accounts, resolver)

    from tt_ledger.sdk import LedgerClient

    ledger = LedgerClient(store=store, accounts=accounts, resolver=resolver)
    details = await ledger.unlinked_orders(account="main")

    assert [d.order.tt_order_id for d in details] == ["O-MANUAL"]
    assert len(details[0].legs) == 1
    assert details[0].legs[0].action == "Buy to Close"
