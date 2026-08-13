"""Reconcile lapse synthesis (docs/ingestion.md → Reconcile, ``synthesize_lapsed_settlements``).

When an OPEN group holds a lot past expiry and the broker never sent a settlement row (futures
options that just vanish), reconcile synthesizes the missing ``Receive Deliver / Expiration``
transaction so transaction-driven group accounting closes the stuck group organically. Replay's
``_lapse_expired_lot`` stays as a harmless position-level backstop.

Synthesis runs before grouping, so a lot whose entry hasn't been grouped yet synthesizes on the
NEXT pass (once its group exists and is seen open) — fresh-history tests reconcile twice.
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
# per-group deterministic id: lapse-<account>-<group_pk>-<security_id>
def _lapse_id(group_pk: int) -> str:
    return f"lapse-main-{group_pk}-{PUT_A}"
# the deterministic settlement timestamp: expiry 21:15Z
LAPSED_AT = datetime(2026, 1, 16, 21, 15, tzinfo=UTC)


class ExpiryResolver:
    """Passthrough ids, but options carry their expiry — lapse detection needs
    ``securities.expiry`` populated."""

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
                     quantity: str, net_value: str, executed_at: datetime, action: str | None = None,
                     underlying: str = "SPY") -> None:
    client.add_transaction(
        BrokerTransaction(
            id=txn_id, account_number="ACCT1", order_id=None, underlying_symbol=underlying,
            symbol=symbol, instrument_type="Equity Option", transaction_type="Receive Deliver",
            transaction_sub_type=sub_type, action=action, quantity=Decimal(quantity),
            net_value=Decimal(net_value), executed_at=executed_at, transaction_date=executed_at.date(),
        )
    )


async def _sync(store, accounts, resolver, client) -> None:
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)


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


async def test_lapsed_lot_synthesizes_settlement_and_expires_the_stuck_group(store, accounts, resolver):
    await _sync(store, accounts, resolver, _entry_client())
    first = await reconcile(store, "main")
    assert first.trade_groups == 1  # pass 1 groups the entry; synthesis waits for the open group
    assert (await _trades(store))[0].status == TradeGroupStatus.OPEN.value

    result = await reconcile(store, "main")  # pass 2: the stuck group closes organically

    pk = await store.get_trade_group_id((await _trades(store))[0].group_id)
    lapses = await _lapse_rows(store)
    assert [r.tt_transaction_id for r in lapses] == [_lapse_id(pk)]
    lapse = lapses[0]
    assert lapse.transaction_type == "Receive Deliver"
    assert lapse.transaction_sub_type == "Expiration"
    assert lapse.quantity == Decimal("1")
    assert lapse.price == Decimal("0")
    assert lapse.executed_at == LAPSED_AT
    assert lapse.underlying == "SPY"
    assert lapse.trade_group_id == pk  # pre-attributed to the group it closed
    assert result.transactions >= 1  # the synthesized row is reported
    assert result.trade_groups == 0  # ...and did NOT orphan into a new group

    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.EXPIRED.value
    assert trade.realized_pnl == Decimal("250")  # full credit kept
    assert trade.closed_at == LAPSED_AT
    events = await _events(store, trade.group_id)
    assert [e.event_type for e in events] == [
        TradeGroupEventType.ENTRY.value, TradeGroupEventType.EXPIRATION.value,
    ]


async def test_rerun_is_idempotent(store, accounts, resolver):
    await _sync(store, accounts, resolver, _entry_client())
    await reconcile(store, "main")
    await reconcile(store, "main")  # closes the group via synthesis

    result = await reconcile(store, "main")

    trade = (await _trades(store))[0]
    pk = await store.get_trade_group_id(trade.group_id)
    assert [r.tt_transaction_id for r in await _lapse_rows(store)] == [_lapse_id(pk)]  # no second row
    assert result.transactions == 0
    assert result.trade_groups == 0
    assert trade.status == TradeGroupStatus.EXPIRED.value


async def test_lot_split_across_groups_settles_each_group(store, accounts, resolver):
    """The same contract held open by SEVERAL groups (parallel strategies on one chain) gets one
    settlement per group, sized to that group's net — first-match whole-quantity routing left
    every group but the first stuck (the kaity_paper/tommyboy_paper SPXW 2026-07-08 incident)."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Sell to Open", quantity="2",
           net_value="500", executed_at=T0 + timedelta(minutes=30))  # own cluster -> own group
    _clock_trade(client, executed_at=datetime(2026, 2, 2, 15, 0, tzinfo=UTC))
    await _sync(store, accounts, resolver, client)
    first = await reconcile(store, "main")
    assert first.trade_groups == 2

    result = await reconcile(store, "main")

    assert result.transactions == 2  # one settlement PER group
    trades = await _trades(store)
    assert [t.status for t in trades] == [TradeGroupStatus.EXPIRED.value] * 2
    quantities = sorted(r.quantity for r in await _lapse_rows(store))
    assert quantities == [Decimal("1"), Decimal("2")]  # each sized to its group's net
    for trade in trades:
        pk = await store.get_trade_group_id(trade.group_id)
        events = await _events(store, trade.group_id)
        assert events[-1].event_type == TradeGroupEventType.EXPIRATION.value
        lapse = next(r for r in await _lapse_rows(store) if r.trade_group_id == pk)
        assert lapse.tt_transaction_id == _lapse_id(pk)


async def test_real_settlement_present_skips_synthesis(store, accounts, resolver):
    client = _entry_client()
    _receive_deliver(client, txn_id="RD-1", symbol=PUT_A, sub_type="Expiration", quantity="1",
                     net_value="0", executed_at=LAPSED_AT)
    await _sync(store, accounts, resolver, client)
    await reconcile(store, "main")

    await reconcile(store, "main")

    assert await _lapse_rows(store) == []  # broker truth already nets the lot to zero
    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.EXPIRED.value  # closed by the REAL row


async def test_priceless_corporate_action_close_skips_synthesis(store, accounts, resolver):
    """Receive Deliver / Special Dividend closes carry action + quantity but NO price. Group
    accounting counts them (the group closes), and synthesis must agree — replay's price-gated
    cost-basis walk would call the lot open and fabricate a double-closing settlement."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _receive_deliver(client, txn_id="RD-CA", symbol=PUT_A, sub_type="Special Dividend",
                     action="Buy to Close", quantity="1", net_value="0",
                     executed_at=datetime(2026, 1, 7, 12, 0, tzinfo=UTC))
    _clock_trade(client, executed_at=datetime(2026, 2, 2, 15, 0, tzinfo=UTC))
    await _sync(store, accounts, resolver, client)
    await reconcile(store, "main")

    result = await reconcile(store, "main")

    assert await _lapse_rows(store) == []
    assert result.transactions == 0
    assert result.trade_groups == 0


async def test_lot_with_no_open_group_does_not_synthesize(store, accounts, resolver):
    """An expired lot none of the OPEN groups holds must not synthesize — the settlement would
    orphan into a junk NEEDS_REVIEW group. Position-level flattening is replay's backstop."""
    client = _entry_client()
    await _sync(store, accounts, resolver, client)
    # no reconcile yet -> the entry is ungrouped, so no open group holds the lot

    result = await reconcile(store, "main")

    assert result.transactions == 0  # pass 1 synthesized nothing (group didn't exist yet)
    assert result.trade_groups == 1


async def test_not_yet_lapsed_lot_is_untouched(store, accounts, resolver):
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    # account clock stops ON the expiry day -- not a full day past it
    _clock_trade(client, executed_at=datetime.combine(EXPIRY, datetime.min.time(), tzinfo=UTC))
    await _sync(store, accounts, resolver, client)
    await reconcile(store, "main")

    await reconcile(store, "main")

    assert await _lapse_rows(store) == []
    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.OPEN.value


async def test_dry_run_counts_but_writes_nothing(store, accounts, resolver):
    await _sync(store, accounts, resolver, _entry_client())
    await reconcile(store, "main")  # real pass 1: the entry group exists and is open

    result = await reconcile(store, "main", dry_run=True)

    assert result.transactions == 1  # the would-be synthesized settlement is previewed
    assert await _lapse_rows(store) == []  # ...but never written
    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.OPEN.value  # and the group didn't flip


# ── intrinsic settlement (the 2026-08-07 phantom-full-credit fix) ────────────
#
# An option lot with strike metadata must settle at INTRINSIC against the
# injected settlement_price resolver, not lapse at zero: a zero-price lapse on
# an ITM short fabricated a full-credit profit (six /ES paper iron flies).
# Without a resolvable price the leg is refused — stuck-open beats
# wrong-forever. Lots without strike metadata keep the legacy zero-lapse
# (covered by the tests above).


class StrikeAwareResolver:
    """ExpiryResolver plus the option-metadata fields intrinsic needs."""

    def resolve(self, vendor_symbol, instrument_type=None):  # noqa: ANN001, ANN201
        is_option = instrument_type == "Equity Option"
        return ResolvedSecurity(
            security_id=vendor_symbol,
            product_type="OS" if is_option else "S",
            underlying="SPY" if is_option else None,
            expiry=EXPIRY if is_option else None,
            strike=Decimal("580") if is_option else None,
            option_type="P" if is_option else None,
            multiplier=100 if is_option else 1,
        )


def _settle_at(price: str | None):
    async def resolver(security_id, expiry):  # noqa: ANN001, ANN202
        return Decimal(price) if price is not None else None

    return resolver


@pytest.fixture
def strike_resolver() -> StrikeAwareResolver:
    return StrikeAwareResolver()


async def test_itm_lapse_settles_at_intrinsic(store, accounts, strike_resolver):
    """Short 580P, settle 550 → intrinsic 30: the lapse row carries price 30 /
    net −3000 (short pays), and realized = 250 credit − 3000 = −2750 — never
    the fabricated full credit."""
    await _sync(store, accounts, strike_resolver, _entry_client())
    await reconcile(store, "main")
    await reconcile(store, "main", settlement_price=_settle_at("550"))

    lapse = (await _lapse_rows(store))[0]
    assert lapse.price == Decimal("30")
    assert lapse.net_value == Decimal("-3000")
    trade = (await _trades(store))[0]
    assert trade.status == TradeGroupStatus.EXPIRED.value
    assert trade.realized_pnl == Decimal("-2750")


async def test_otm_lapse_keeps_full_credit(store, accounts, strike_resolver):
    """Short 580P, settle 600 → intrinsic 0: legacy economics, full credit."""
    await _sync(store, accounts, strike_resolver, _entry_client())
    await reconcile(store, "main")
    await reconcile(store, "main", settlement_price=_settle_at("600"))

    lapse = (await _lapse_rows(store))[0]
    assert lapse.price == Decimal("0")
    assert lapse.net_value == Decimal("0")
    assert (await _trades(store))[0].realized_pnl == Decimal("250")


async def test_unresolvable_price_refuses_to_fabricate(store, accounts, strike_resolver):
    """Strike metadata present but no price resolvable (resolver absent OR
    returning None) → NO lapse row, group stays open for a later pass."""
    await _sync(store, accounts, strike_resolver, _entry_client())
    await reconcile(store, "main")

    await reconcile(store, "main")  # no resolver at all
    assert await _lapse_rows(store) == []
    assert (await _trades(store))[0].status == TradeGroupStatus.OPEN.value

    await reconcile(store, "main", settlement_price=_settle_at(None))
    assert await _lapse_rows(store) == []
    assert (await _trades(store))[0].status == TradeGroupStatus.OPEN.value

    # a price arriving later un-sticks it
    await reconcile(store, "main", settlement_price=_settle_at("550"))
    assert (await _trades(store))[0].status == TradeGroupStatus.EXPIRED.value


async def test_long_itm_lapse_receives_intrinsic(store, accounts, strike_resolver):
    """A LONG 580P bought for 250, settle 550 → receives +3000: realized
    −250 − fees? no — gross realized = 3000 − 250 = +2750, net_value +3000."""
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Buy to Open", quantity="1",
           net_value="-250", executed_at=T0)
    _clock_trade(client, executed_at=datetime(2026, 2, 2, 15, 0, tzinfo=UTC))
    await _sync(store, accounts, strike_resolver, client)
    await reconcile(store, "main")
    await reconcile(store, "main", settlement_price=_settle_at("550"))

    lapse = (await _lapse_rows(store))[0]
    assert lapse.price == Decimal("30")
    assert lapse.net_value == Decimal("3000")
    assert (await _trades(store))[0].realized_pnl == Decimal("2750")


async def test_offsetting_sibling_groups_each_settle(store, accounts, strike_resolver):
    """Two open groups holding the SAME contract in OPPOSITE directions both settle.

    A laddered multi-entry strategy reuses strikes across rungs: the strike one
    rung sells short is the next rung's long wing. That nets the ACCOUNT to zero
    while both groups sit open, so discovery driven off the account-level net
    could not see the contract at all and left every such group stuck open
    forever — 68 MEIC groups across three paper accounts over ten sessions
    (2026-08-03..08-12), invisible because 134 expired contracts netted to zero.

    Each group must settle its own leg at its own sign: short pays intrinsic,
    long receives it, and the two cancel at the account level exactly as they
    should.
    """
    client = MockTastyTradeClient()
    _trade(client, order_id="O-1", symbol=PUT_A, action="Sell to Open", quantity="1",
           net_value="250", executed_at=T0)
    _trade(client, order_id="O-2", symbol=PUT_A, action="Buy to Open", quantity="1",
           net_value="-200", executed_at=T0 + timedelta(minutes=30))  # own cluster -> own group
    _clock_trade(client, executed_at=datetime(2026, 2, 2, 15, 0, tzinfo=UTC))
    await _sync(store, accounts, strike_resolver, client)
    first = await reconcile(store, "main")
    assert first.trade_groups == 2

    # Settle 550 against the 580 put -> intrinsic 30 (x100 = 3000 per contract).
    result = await reconcile(store, "main", settlement_price=_settle_at("550"))

    assert result.transactions == 2  # one settlement PER group, not zero
    lapses = await _lapse_rows(store)
    assert sorted(r.net_value for r in lapses) == [Decimal("-3000"), Decimal("3000")]
    assert {r.price for r in lapses} == {Decimal("30")}
    # Both groups closed, and the offsetting settlements cancel account-wide.
    trades = await _trades(store)
    assert [t.status for t in trades] == [TradeGroupStatus.EXPIRED.value] * 2
    assert sum(r.net_value for r in lapses) == Decimal("0")
    # Short group: 250 credit - 3000 = -2750. Long group: -200 debit + 3000 = 2800.
    assert sorted(t.realized_pnl for t in trades) == [Decimal("-2750"), Decimal("2800")]
