"""Reconcile lapse synthesis (docs/ingestion.md → Reconcile, ``synthesize_lapsed_settlements``).

When an open lot is past expiry but the broker never sent a settlement row (futures options
that just vanish; corporate-action re-symbols), reconcile synthesizes the missing
``Receive Deliver / Expiration`` transaction so transaction-driven group accounting closes
the stuck group organically. Replay's ``_lapse_expired_lot`` stays as a harmless backstop.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.enums import TradeGroupEventType, TradeGroupStatus
from tt_ledger.identity import AccountMapper
from tt_ledger.identity.securities import ResolvedSecurity
from tt_ledger.ingest.broker import BrokerTransaction
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import reconcile
from tt_ledger.rows import ActivityFilter, TradeFilter
from tt_ledger.store.memory import InMemoryStore

T0 = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)

PUT_A = "SPY   260116P00580000"
EXPIRY = date(2026, 1, 16)
LAPSE_ID = f"lapse-main-{PUT_A}"
# the deterministic settlement timestamp: expiry 21:15Z
LAPSED_AT = datetime(2026, 1, 16, 21, 15, tzinfo=UTC)


class ExpiryResolver:
    """Passthrough ids, but options (symbols containing an option code) carry their expiry —
    lapse detection needs ``securities.expiry`` populated."""

    def resolve(self, vendor_symbol, instrument_type=None):  # noqa: ANN001, ANN201
        is_option = instrument_type == "Equity Option"
        return ResolvedSecurity(
            security_id=vendor_symbol,
            product_type="OS" if is_option else "S",
            underlying="SPY" if is_option else None,
            expiry=EXPIRY if is_option else None,
            multiplier=100 if is_option else 1,
        )


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def resolver() -> ExpiryResolver:
    return ExpiryResolver()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


def _trade(client: MockTastyTradeClient, *, order_id: str, symbol: str, action: str,
           quantity: str, net_value: str, executed_at: datetime, underlying: str = "SPY",
           instrument_type: str = "Equity Option") -> None:
    client.fill(
        account_number="ACCT1", order_id=order_id, symbol=symbol, instrument_type=instrument_type,
        action=action, quantity=Decimal(quantity), fill_price=Decimal("1"),
        filled_at=executed_at, underlying_symbol=underlying,
    )
    client._transactions["ACCT1"][-1].net_value = Decimal(net_value)  # fill() doesn't set cash fields


def _clock_trade(client: MockTastyTradeClient, *, executed_at: datetime) -> None:
    """Unrelated later activity that advances the account's own clock (never wall-clock)."""
    client.add_transaction(BrokerTransaction(
        id=f"T-CLOCK-{executed_at:%Y%m%d}", account_number="ACCT1", order_id=None,
        symbol="AAPL", instrument_type="Equity", transaction_type="Trade", action="Buy",
        quantity=Decimal("1"), price=Decimal("190"), net_value=Decimal("-190"),
        executed_at=executed_at, transaction_date=executed_at.date(),
    ))


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


async def _sync_and_reconcile(store, accounts, resolver, client, *, dry_run: bool = False):
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    return await reconcile(store, "main", dry_run=dry_run)


async def _lapse_rows(store) -> list:
    activity = await store.account_activity(ActivityFilter(account="main"))
    return [a for a in activity if a.tt_transaction_id.startswith("lapse-")]


async def _trades(store) -> list:
    return await store.unified_trades(TradeFilter(account="main"))


async def _events(store, group_id: str) -> list:
    pk = await store.get_trade_group_id(group_id)
    return [ev for _, ev in store._events.all() if ev.trade_group_id == pk]


def _entry_client() -> MockTastyTradeClient:
    """A short put entered at T0 whose contract expired with NO broker settlement row, plus a
    later unrelated trade that moves the account's clock a full day past the expiry."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _clock_trade(client, executed_at=datetime(2026, 2, 2, 15, 0, tzinfo=UTC))
    return client


async def test_lapsed_lot_synthesizes_settlement_and_expires_group_in_one_pass(store, accounts, resolver):
    result = await _sync_and_reconcile(store, accounts, resolver, _entry_client())

    lapses = await _lapse_rows(store)
    assert [r.tt_transaction_id for r in lapses] == [LAPSE_ID]
    lapse = lapses[0]
    assert lapse.transaction_type == "Receive Deliver"
    assert lapse.transaction_sub_type == "Expiration"
    assert lapse.quantity == Decimal("1")
    assert lapse.price == Decimal("0")
    assert lapse.executed_at == LAPSED_AT
    assert lapse.underlying == "SPY"
    assert result.transactions >= 1  # the synthesized row is reported

    # the entry group closed as EXPIRED in the SAME reconcile pass
    trade = next(t for t in await _trades(store) if t.status != TradeGroupStatus.OPEN.value)
    assert trade.status == TradeGroupStatus.EXPIRED.value
    assert trade.realized_pnl == Decimal("250")  # full credit kept
    assert trade.closed_at == LAPSED_AT
    events = await _events(store, trade.group_id)
    assert [e.event_type for e in events] == [
        TradeGroupEventType.ENTRY.value, TradeGroupEventType.EXPIRATION.value,
    ]

    # and the synthetic row is attributed to that group
    assert (await _lapse_rows(store))[0].trade_group_id is not None


async def test_rerun_is_idempotent(store, accounts, resolver):
    await _sync_and_reconcile(store, accounts, resolver, _entry_client())
    first = await _lapse_rows(store)

    result = await reconcile(store, "main")

    again = await _lapse_rows(store)
    assert [r.tt_transaction_id for r in again] == [r.tt_transaction_id for r in first] == [LAPSE_ID]
    assert result.trade_groups == 0  # nothing new to group
    trade = next(t for t in await _trades(store) if t.status != TradeGroupStatus.OPEN.value)
    assert trade.status == TradeGroupStatus.EXPIRED.value


async def test_real_settlement_present_skips_synthesis(store, accounts, resolver):
    client = _entry_client()
    _receive_deliver(client, txn_id="RD-1", symbol=PUT_A, sub_type="Expiration", quantity="1",
                     net_value="0", executed_at=LAPSED_AT)

    await _sync_and_reconcile(store, accounts, resolver, client)

    assert await _lapse_rows(store) == []  # broker truth already nets the lot to zero
    trade = next(t for t in await _trades(store) if t.status != TradeGroupStatus.OPEN.value)
    assert trade.status == TradeGroupStatus.EXPIRED.value  # closed by the REAL row


async def test_not_yet_lapsed_lot_is_untouched(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    # account clock stops ON the expiry day -- not a full day past it
    _clock_trade(client, executed_at=datetime.combine(EXPIRY, datetime.min.time(), tzinfo=UTC))

    await _sync_and_reconcile(store, accounts, resolver, client)

    assert await _lapse_rows(store) == []
    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.OPEN.value


async def test_dry_run_counts_but_writes_nothing(store, accounts, resolver):
    client = _entry_client()
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)

    result = await reconcile(store, "main", dry_run=True)

    assert result.transactions == 1  # the would-be synthesized settlement is previewed
    assert await _lapse_rows(store) == []  # ...but never written
    assert await _trades(store) == []  # no groups created either
