"""``reconcile`` / ``detect_strategy_type`` (docs/ingestion.md -> Reconcile).

Classification tests use ``CanonicalSymbolResolver`` (not the zero-config ``PassthroughResolver``)
because strategy classification depends on option_type/strike/expiry actually being decomposed
onto the securities dimension -- the default passthrough resolver never populates those, so
multi-leg classification degrades toward "custom"/"condor" with it. That's a real, worth-knowing
limitation of running reconcile with zero symbology config.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, UTC
from decimal import Decimal

import pytest

from tt_ledger.enums import Origin, ReviewStatus
from tt_ledger.identity import AccountMapper
from tt_ledger.identity.canonical import CanonicalSymbolResolver
from tt_ledger.ingest.broker import BrokerTransaction, PlacedFill, PlacedLeg, PlacedOrder
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import LegInfo, detect_strategy_type, reconcile
from tt_ledger.rows import TradeFilter
from tt_ledger.schema import metadata, models
from tt_ledger.store.memory import InMemoryStore
from tt_ledger.store.sql import SqlLedgerStore

# --- detect_strategy_type: pure unit tests --------------------------------------------------

_EXP = date(2026, 1, 17)
_EXP2 = date(2026, 2, 21)


def _leg(product_type="OS", option_type=None, expiry=_EXP, strike=None, quantity=Decimal("1")):
    return LegInfo(product_type=product_type, option_type=option_type, expiry=expiry, strike=strike, quantity=quantity)


def test_no_legs_is_custom():
    assert detect_strategy_type([]) == "custom"


def test_single_option_leg():
    assert detect_strategy_type([_leg(option_type="C", strike=Decimal("100"))]) == "single"


def test_single_future_leg():
    assert detect_strategy_type([_leg(product_type="F")]) == "future"


def test_vertical_spread():
    legs = [_leg(option_type="C", strike=Decimal("100")), _leg(option_type="C", strike=Decimal("105"))]
    assert detect_strategy_type(legs) == "vertical"


def test_ratio_spread():
    legs = [
        _leg(option_type="C", strike=Decimal("100"), quantity=Decimal("1")),
        _leg(option_type="C", strike=Decimal("105"), quantity=Decimal("2")),
    ]
    assert detect_strategy_type(legs) == "ratio"


def test_calendar_spread():
    legs = [_leg(option_type="C", strike=Decimal("100"), expiry=_EXP), _leg(option_type="C", strike=Decimal("100"), expiry=_EXP2)]
    assert detect_strategy_type(legs) == "calendar"


def test_diagonal_spread():
    legs = [_leg(option_type="C", strike=Decimal("100"), expiry=_EXP), _leg(option_type="C", strike=Decimal("105"), expiry=_EXP2)]
    assert detect_strategy_type(legs) == "diagonal"


def test_straddle():
    legs = [_leg(option_type="C", strike=Decimal("100")), _leg(option_type="P", strike=Decimal("100"))]
    assert detect_strategy_type(legs) == "straddle"


def test_strangle():
    legs = [_leg(option_type="C", strike=Decimal("105")), _leg(option_type="P", strike=Decimal("95"))]
    assert detect_strategy_type(legs) == "strangle"


def test_butterfly():
    legs = [_leg(option_type="C", strike=Decimal("95")), _leg(option_type="C", strike=Decimal("100")), _leg(option_type="C", strike=Decimal("105"))]
    assert detect_strategy_type(legs) == "butterfly"


def test_iron_condor():
    legs = [
        _leg(option_type="P", strike=Decimal("95")), _leg(option_type="P", strike=Decimal("100")),
        _leg(option_type="C", strike=Decimal("110")), _leg(option_type="C", strike=Decimal("115")),
    ]
    assert detect_strategy_type(legs) == "iron_condor"


def test_iron_butterfly():
    legs = [
        _leg(option_type="P", strike=Decimal("95")), _leg(option_type="P", strike=Decimal("100")),
        _leg(option_type="C", strike=Decimal("100")), _leg(option_type="C", strike=Decimal("105")),
    ]
    assert detect_strategy_type(legs) == "iron_butterfly"


def test_condor_all_calls():
    legs = [_leg(option_type="C", strike=s) for s in (Decimal("95"), Decimal("100"), Decimal("110"), Decimal("115"))]
    assert detect_strategy_type(legs) == "condor"


def test_covered():
    legs = [_leg(product_type="S", option_type=None, strike=None), _leg(option_type="C", strike=Decimal("100"))]
    assert detect_strategy_type(legs) == "covered"


def test_collar():
    legs = [
        _leg(product_type="S", option_type=None, strike=None),
        _leg(option_type="C", strike=Decimal("105")),
        _leg(option_type="P", strike=Decimal("95")),
    ]
    assert detect_strategy_type(legs) == "collar"


def test_future_spread():
    legs = [_leg(product_type="F", option_type=None, strike=None), _leg(product_type="F", option_type=None, strike=None)]
    assert detect_strategy_type(legs) == "future_spread"


def test_five_legs_is_custom():
    legs = [_leg(option_type="C", strike=Decimal(str(100 + i))) for i in range(5)]
    assert detect_strategy_type(legs) == "custom"


def test_mixed_stock_and_multiple_options_is_custom():
    legs = [_leg(product_type="S", option_type=None, strike=None), _leg(option_type="C", strike=Decimal("100")), _leg(option_type="C", strike=Decimal("105"))]
    assert detect_strategy_type(legs) == "custom"


# --- reconcile: integration tests (InMemoryStore) -------------------------------------------


def _occ(root: str, expiry: date, right: str, strike: str) -> str:
    padded_root = root.ljust(6)
    return f"{padded_root}{expiry.strftime('%y%m%d')}{right}{strike}"


IC_EXPIRY = date(2026, 1, 17)
IC_LEGS = [
    # (symbol, action, fill_price, net_value)
    (_occ("SPXW", IC_EXPIRY, "P", "05000000"), "Sell to Open", Decimal("2.50"), Decimal("250")),
    (_occ("SPXW", IC_EXPIRY, "P", "04950000"), "Buy to Open", Decimal("0.50"), Decimal("-50")),
    (_occ("SPXW", IC_EXPIRY, "C", "05500000"), "Sell to Open", Decimal("2.50"), Decimal("250")),
    (_occ("SPXW", IC_EXPIRY, "C", "05550000"), "Buy to Open", Decimal("0.50"), Decimal("-50")),
]


def _iron_condor_client(order_id: str = "O-IC", account_number: str = "ACCT1", executed_at: datetime | None = None) -> MockTastyTradeClient:
    executed_at = executed_at or datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
    client = MockTastyTradeClient()
    legs = []
    for i, (symbol, action, fill_price, _net_value) in enumerate(IC_LEGS):
        legs.append(
            PlacedLeg(
                instrument_type="Equity Option", symbol=symbol, action=action,
                quantity=Decimal("1"), remaining_quantity=Decimal("0"),
                fills=[PlacedFill(fill_id=f"{order_id}-F{i}", quantity=Decimal("1"), fill_price=fill_price, filled_at=executed_at)],
            )
        )
    client.add_order(
        PlacedOrder(
            id=order_id, account_number=account_number, received_at=executed_at, underlying_symbol="SPX",
            complex_order_id="CO-1", complex_order_tag="Iron Condor", status="Filled", terminal_at=executed_at, legs=legs,
        )
    )
    for i, (symbol, action, _fill_price, net_value) in enumerate(IC_LEGS):
        client.add_transaction(
            BrokerTransaction(
                id=f"TXN-{order_id}-{i}", account_number=account_number, order_id=order_id,
                underlying_symbol="SPX", symbol=symbol, instrument_type="Equity Option",
                transaction_type="Trade", action=action, quantity=Decimal("1"), net_value=net_value,
                executed_at=executed_at, transaction_date=executed_at.date(),
            )
        )
    return client


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1", "second": "ACCT2"})


@pytest.fixture
def resolver() -> CanonicalSymbolResolver:
    return CanonicalSymbolResolver()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


async def _sync(store, account, client, accounts, resolver):
    await sync_orders(store, account, client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, account, client=client, accounts=accounts, resolver=resolver)


async def test_reconcile_iron_condor_fixture(store, accounts, resolver):
    client = _iron_condor_client()
    await _sync(store, "main", client, accounts, resolver)

    result = await reconcile(store, "main")
    assert result.trade_groups == 1
    assert result.errors == []

    trades = await store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 1
    trade = trades[0]
    assert trade.origin is Origin.BROKER
    assert trade.review_status is ReviewStatus.NEEDS_REVIEW
    assert trade.strategy_type == "iron_condor"
    assert trade.leg_count == 4
    assert trade.total_premium == Decimal("400")  # 250 - 50 + 250 - 50
    assert trade.status == "open"

    # legs correctly VWAP'd (single fill each here, so VWAP == the fill price)
    order_id = store._orders.id_of("O-IC")
    legs = {row.leg_index: row for _, row in store._legs.all() if row.order_id == order_id}
    assert legs[0].fill_price == Decimal("2.50")
    assert legs[1].fill_price == Decimal("0.50")

    # both orders.trade_group_id and transactions.trade_group_id are set
    order = await store.get_order("O-IC")
    assert order.trade_group_id is not None
    for i in range(4):
        txn = store._transactions.get_by_key(f"TXN-O-IC-{i}")
        assert txn.trade_group_id is not None

    # re-running is idempotent: no new group
    result2 = await reconcile(store, "main")
    assert result2.trade_groups == 0
    assert len(await store.unified_trades(TradeFilter(account="main"))) == 1


async def test_reconcile_never_touches_a_manually_attributed_group(store, accounts, resolver):
    client = _iron_condor_client()
    await _sync(store, "main", client, accounts, resolver)
    await reconcile(store, "main")

    trade = (await store.unified_trades(TradeFilter(account="main")))[0]
    tg = await store.get_trade_group(trade.group_id)
    manually_edited = replace(
        tg, manually_attributed=True, review_status=ReviewStatus.CONFIRMED, strategy_type="custom", bot_name="my-bot",
    )
    await store.upsert_trade_group(manually_edited)

    result = await reconcile(store, "main")
    assert result.trade_groups == 0  # nothing new -- the group's transactions are already claimed

    after = await store.get_trade_group(trade.group_id)
    assert after.manually_attributed is True
    assert after.review_status is ReviewStatus.CONFIRMED
    assert after.strategy_type == "custom"
    assert after.bot_name == "my-bot"


async def test_reconcile_single_leg_order(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await _sync(store, "main", client, accounts, resolver)

    result = await reconcile(store, "main")
    assert result.trade_groups == 1
    trade = (await store.unified_trades(TradeFilter(account="main")))[0]
    assert trade.strategy_type == "single"
    assert trade.leg_count == 1


async def test_reconcile_groups_two_orders_executed_within_the_tolerance_window(store, accounts, resolver):
    t0 = datetime(2026, 1, 5, 10, 0, 0, tzinfo=UTC)
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-A", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=t0, status="Filled",
    )
    client.fill(
        account_number="ACCT1", order_id="O-B", symbol="MSFT", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("300"),
        filled_at=t0 + timedelta(seconds=2), status="Filled",
    )
    await _sync(store, "main", client, accounts, resolver)

    result = await reconcile(store, "main")
    assert result.trade_groups == 1
    trade = (await store.unified_trades(TradeFilter(account="main")))[0]
    assert trade.leg_count == 2


async def test_reconcile_does_not_group_orders_far_apart_in_time(store, accounts, resolver):
    t0 = datetime(2026, 1, 5, 10, 0, 0, tzinfo=UTC)
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-A", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=t0, status="Filled",
    )
    client.fill(
        account_number="ACCT1", order_id="O-B", symbol="MSFT", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("300"),
        filled_at=t0 + timedelta(hours=3), status="Filled",
    )
    await _sync(store, "main", client, accounts, resolver)

    result = await reconcile(store, "main")
    assert result.trade_groups == 2


async def test_reconcile_dry_run_does_not_persist(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await _sync(store, "main", client, accounts, resolver)

    result = await reconcile(store, "main", dry_run=True)
    assert result.trade_groups == 1
    assert await store.unified_trades(TradeFilter(account="main")) == []

    # the link step still ran (deterministic exact-match linking is safe even in dry_run)
    order_id = store._orders.id_of("O-1")
    txn = next(row for _, row in store._transactions.all() if row.tt_order_id == "O-1")
    assert txn.order_id == order_id
    assert txn.trade_group_id is None


async def test_reconcile_with_no_account_covers_every_account_with_activity(store, accounts, resolver):
    client_main = MockTastyTradeClient()
    client_main.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await _sync(store, "main", client_main, accounts, resolver)

    client_second = MockTastyTradeClient()
    client_second.fill(
        account_number="ACCT2", order_id="O-2", symbol="MSFT", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("300"), filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await _sync(store, "second", client_second, accounts, resolver)

    result = await reconcile(store)  # account=None -> all accounts
    assert result.trade_groups == 2
    assert len(await store.unified_trades(TradeFilter(account="main"))) == 1
    assert len(await store.unified_trades(TradeFilter(account="second"))) == 1


# --- confirmatory test against the real SQL store -------------------------------------------


@pytest.fixture
async def sql_store(store_url):
    s = SqlLedgerStore(store_url)
    async with s._engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await s.create_all()
    async with s._sessionmaker() as session, session.begin():
        await session.execute(
            models.Account.__table__.insert().values(nickname="main", account_number="ACCT1", login="user1")
        )
    yield s
    await s.dispose()


async def test_reconcile_iron_condor_against_sql_store(sql_store, accounts, resolver):
    client = _iron_condor_client()
    await _sync(sql_store, "main", client, accounts, resolver)

    result = await reconcile(sql_store, "main")
    assert result.trade_groups == 1

    trades = await sql_store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 1
    assert trades[0].strategy_type == "iron_condor"
    assert trades[0].total_premium == Decimal("400")

    result2 = await reconcile(sql_store, "main")
    assert result2.trade_groups == 0


# --- compute_group_fields: legs are per SECURITY at peak exposure (2026-07-12 fix) ----------


def _act(security_id, action, qty, at, *, txn_type="Trade", sub_type=None):
    from tt_ledger.rows import ActivityRow
    return ActivityRow(
        tt_transaction_id=f"T-{security_id}-{action}-{at:%H%M%S}", account="main",
        transaction_type=txn_type, transaction_sub_type=sub_type, action=action,
        security_id=security_id, underlying="SPY", quantity=Decimal(qty),
        net_value=Decimal("1"), executed_at=at,
    )


async def _fields_store(securities):
    from tt_ledger.rows import SecurityRow
    from tt_ledger.store.memory import InMemoryStore
    store = InMemoryStore()
    for sec in securities:
        await store.upsert_security(SecurityRow(**sec))
    return store


_T = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)
_OPT = {"security_id": "OPT-P580", "product_type": "OS", "option_type": "P",
        "expiry": date(2026, 1, 16), "strike": Decimal("580"), "multiplier": 100}


async def test_fields_closed_round_trip_stays_single():
    """Open + close of one option = ONE leg (single), not a two-leg 'spread' — the wart that
    degraded every regroup recompute of a closed group."""
    from tt_ledger.ingest.reconcile import compute_group_fields
    store = await _fields_store([_OPT])
    cluster = [
        _act("OPT-P580", "Sell to Open", "1", _T),
        _act("OPT-P580", "Buy to Close", "1", _T + timedelta(hours=2)),
    ]
    fields = await compute_group_fields(store, cluster)
    assert fields["strategy_type"] == "single"
    assert fields["leg_count"] == 1
    assert fields["quantity"] == Decimal("1")


async def test_fields_scale_in_classifies_at_peak():
    """1-lot entry + 3-lot add + 4-lot close per leg = a 4-lot condor, not a 12-leg custom."""
    from tt_ledger.ingest.reconcile import compute_group_fields
    store = await _fields_store([
        {"security_id": f"OPT-{name}", "product_type": "OS", "option_type": ot,
         "expiry": date(2026, 1, 16), "strike": Decimal(strike), "multiplier": 100}
        for name, ot, strike in (("P95", "P", "95"), ("P100", "P", "100"),
                                 ("C110", "C", "110"), ("C115", "C", "115"))
    ])
    cluster = []
    for sid, open_action, close_action in (
        ("OPT-P95", "Buy to Open", "Sell to Close"), ("OPT-P100", "Sell to Open", "Buy to Close"),
        ("OPT-C110", "Sell to Open", "Buy to Close"), ("OPT-C115", "Buy to Open", "Sell to Close"),
    ):
        cluster.append(_act(sid, open_action, "1", _T))
        cluster.append(_act(sid, open_action, "3", _T + timedelta(hours=3)))
        cluster.append(_act(sid, close_action, "4", _T + timedelta(days=4)))
    fields = await compute_group_fields(store, cluster)
    assert fields["strategy_type"] == "iron_condor"
    assert fields["leg_count"] == 4
    assert fields["quantity"] == Decimal("4")


async def test_fields_partial_fill_is_not_a_ratio():
    """A 2-lot leg filled as 1+1 is one 2-lot leg, not a 1:1 'ratio' of two legs."""
    from tt_ledger.ingest.reconcile import compute_group_fields
    store = await _fields_store([_OPT])
    cluster = [
        _act("OPT-P580", "Sell to Open", "1", _T),
        _act("OPT-P580", "Sell to Open", "1", _T),
    ]
    fields = await compute_group_fields(store, cluster)
    assert fields["strategy_type"] == "single"
    assert fields["quantity"] == Decimal("2")
