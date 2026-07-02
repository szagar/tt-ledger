"""``rebuild_positions_from_transactions`` (docs/ingestion.md -> Replay).

Pure unit tests drive ``_replay_security`` directly with hand-built ``ActivityRow``s (fast, no
store); integration tests run the whole thing through ``InMemoryStore``/``SqlLedgerStore`` via
``MockTastyTradeClient`` + ``sync_transactions``, matching the convention in
``test_ingest_reconcile.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BrokerPosition, BrokerTransaction
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_positions, sync_transactions
from tt_ledger.ingest.replay import _replay_security, rebuild_positions_from_transactions
from tt_ledger.rows import ActivityRow
from tt_ledger.schema import metadata, models
from tt_ledger.store.memory import InMemoryStore
from tt_ledger.store.sql import SqlLedgerStore

T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)


def _row(tt_transaction_id, *, quantity, action, price, executed_at=T0, order_id=None, security_id="AAPL"):
    return ActivityRow(
        tt_transaction_id=tt_transaction_id, account="main", security_id=security_id,
        quantity=quantity, action=action, price=price, executed_at=executed_at, order_id=order_id,
    )


# --- _replay_security: pure unit tests -------------------------------------------------------


def test_single_open_leaves_position_open_no_closes():
    rows = [_row("T1", quantity=Decimal("10"), action="Buy to Open", price=Decimal("100"), order_id=1)]
    position, plan = _replay_security("main", "AAPL", rows, 1, None)

    assert position.quantity == Decimal("10")
    assert position.quantity_direction == "Long"
    assert position.average_open_price == Decimal("100")
    assert position.opening_order_id == 1
    assert position.position_opened_at == T0
    assert plan == [("T1", True, None)]


def test_adding_to_a_long_position_recomputes_weighted_average():
    rows = [
        _row("T1", quantity=Decimal("10"), action="Buy to Open", price=Decimal("100")),
        _row("T2", quantity=Decimal("5"), action="Buy to Open", price=Decimal("130"), executed_at=T0 + timedelta(days=1)),
    ]
    position, plan = _replay_security("main", "AAPL", rows, 1, None)

    assert position.quantity == Decimal("15")
    assert position.average_open_price == Decimal("110")  # (10*100 + 5*130) / 15
    assert plan == [("T1", True, None), ("T2", True, None)]


def test_partial_close_leaves_average_open_price_unchanged_and_no_closed_row():
    rows = [
        _row("T1", quantity=Decimal("10"), action="Buy to Open", price=Decimal("100")),
        _row("T2", quantity=Decimal("4"), action="Sell to Close", price=Decimal("120"), executed_at=T0 + timedelta(days=1)),
    ]
    position, plan = _replay_security("main", "AAPL", rows, 1, None)

    assert position.quantity == Decimal("6")
    assert position.quantity_direction == "Long"
    assert position.average_open_price == Decimal("100")  # unchanged by a partial close
    assert plan[1] == ("T2", True, None)  # still contributing to the (still open) current lot


def test_full_close_creates_a_closed_position_and_resets_the_lot():
    opened_at = T0
    closed_at = T0 + timedelta(days=10)
    rows = [
        _row("T1", quantity=Decimal("10"), action="Buy to Open", price=Decimal("100"), executed_at=opened_at, order_id=1),
        _row("T2", quantity=Decimal("10"), action="Sell to Close", price=Decimal("120"), executed_at=closed_at, order_id=2),
    ]
    position, plan = _replay_security("main", "AAPL", rows, 1, None)

    assert position.quantity == Decimal("0")
    assert position.average_open_price is None
    assert position.opening_order_id is None
    assert position.position_opened_at is None

    tt_id, marks_open, closed = plan[1]
    assert tt_id == "T2"
    assert marks_open is False
    assert closed.quantity == Decimal("10")
    assert closed.quantity_direction == "Long"
    assert closed.average_open_price == Decimal("100")
    assert closed.average_close_price == Decimal("120")
    assert closed.realized_pnl == Decimal("200")  # (120-100)*10
    assert closed.opening_order_id == 1
    assert closed.closing_order_id == 2
    assert closed.opened_at == opened_at
    assert closed.closed_at == closed_at
    assert closed.holding_period_days == 10


def test_multiplier_scales_realized_pnl_for_options():
    rows = [
        _row("T1", quantity=Decimal("1"), action="Buy to Open", price=Decimal("2.00")),
        _row("T2", quantity=Decimal("1"), action="Sell to Close", price=Decimal("3.50"), executed_at=T0 + timedelta(days=1)),
    ]
    _, plan = _replay_security("main", "AAPL  260117C00150000", rows, 100, None)
    assert plan[1][2].realized_pnl == Decimal("150.00")  # (3.50-2.00)*1*100


def test_shorting_and_covering_at_a_lower_price_is_a_profit():
    rows = [
        _row("T1", quantity=Decimal("10"), action="Sell to Open", price=Decimal("50")),
        _row("T2", quantity=Decimal("10"), action="Buy to Close", price=Decimal("40"), executed_at=T0 + timedelta(days=1)),
    ]
    position, plan = _replay_security("main", "AAPL", rows, 1, None)
    assert position.quantity_direction == "Short"  # last known direction, now flat
    closed = plan[1][2]
    assert closed.quantity_direction == "Short"
    assert closed.realized_pnl == Decimal("100")  # (50-40)*10


def test_direction_flip_closes_the_old_lot_and_opens_a_new_one_in_the_same_transaction():
    rows = [
        _row("T1", quantity=Decimal("10"), action="Buy to Open", price=Decimal("100"), order_id=1),
        _row("T2", quantity=Decimal("15"), action="Sell to Close", price=Decimal("110"), order_id=2, executed_at=T0 + timedelta(days=1)),
    ]
    position, plan = _replay_security("main", "AAPL", rows, 1, None)

    assert position.quantity == Decimal("5")
    assert position.quantity_direction == "Short"
    assert position.average_open_price == Decimal("110")  # the flip trade's own price
    assert position.opening_order_id == 2

    tt_id, marks_open, closed = plan[1]
    assert tt_id == "T2"
    assert marks_open is True  # this row ALSO opens the new short lot
    assert closed is not None  # ...while closing the old long lot
    assert closed.quantity == Decimal("10")
    assert closed.quantity_direction == "Long"
    assert closed.realized_pnl == Decimal("100")  # (110-100)*10 -- only the closed portion


def test_zero_delta_or_missing_price_is_ignored():
    rows = [
        _row("T1", quantity=Decimal("0"), action="Buy to Open", price=Decimal("100")),
        _row("T2", quantity=Decimal("5"), action="Buy to Open", price=None),
    ]
    position, plan = _replay_security("main", "AAPL", rows, 1, None)
    assert position.quantity == Decimal("0")
    assert plan == [("T1", False, None), ("T2", False, None)]


def test_dividend_reinvestment_with_an_action_opens_like_an_ordinary_buy():
    rows = [_row("T1", quantity=Decimal("1.68074"), action="Buy to Open", price=Decimal("16.46"))]
    position, _ = _replay_security("main", "KBWD", rows, 1, None)
    assert position.quantity == Decimal("1.68074")
    assert position.average_open_price == Decimal("16.46")


def test_no_action_falls_back_to_the_transactions_own_quantity_sign():
    """Corporate actions (splits, symbol changes) sometimes carry no ``action`` -- best-effort:
    apply the transaction's own (signed) quantity directly rather than guessing a direction."""
    rows = [_row("T1", quantity=Decimal("-5"), action=None, price=Decimal("100"))]
    position, plan = _replay_security("main", "AAPL", rows, 1, None)
    assert position.quantity == Decimal("5")
    assert position.quantity_direction == "Short"
    assert plan == [("T1", True, None)]


def test_existing_position_market_data_fields_are_preserved():
    from tt_ledger.rows import PositionRow

    existing = PositionRow(
        account="main", security_id="AAPL", quantity=Decimal("999"), quantity_direction="Long",
        mark_price=Decimal("155.5"), close_price=Decimal("154"), unrealized_pnl=Decimal("42"),
        realized_day_gain=Decimal("1"), strategy_id=7, trade_group_id=3,
    )
    rows = [_row("T1", quantity=Decimal("10"), action="Buy to Open", price=Decimal("100"))]
    position, _ = _replay_security("main", "AAPL", rows, 1, existing)

    assert position.quantity == Decimal("10")  # replay owns quantity -- not preserved
    assert position.mark_price == Decimal("155.5")
    assert position.close_price == Decimal("154")
    assert position.unrealized_pnl == Decimal("42")
    assert position.realized_day_gain == Decimal("1")
    assert position.strategy_id == 7
    assert position.trade_group_id == 3


# --- rebuild_positions_from_transactions: integration tests ----------------------------------


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def resolver() -> PassthroughResolver:
    return PassthroughResolver()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


async def test_rebuild_open_position(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=T0,
    )
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)

    result = await rebuild_positions_from_transactions(store, "main")
    assert result.errors == []
    assert result.positions == 1

    position = await store.get_position("main", "AAPL")
    assert position.quantity == Decimal("10")
    assert position.quantity_direction == "Long"
    assert position.average_open_price == Decimal("150")
    assert await store.get_closed_positions("main") == []


async def test_rebuild_open_and_close_creates_a_closed_position_and_links_transactions(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=T0,
    )
    client.fill(
        account_number="ACCT1", order_id="O-2", symbol="AAPL", instrument_type="Equity",
        action="Sell to Close", quantity=Decimal("10"), fill_price=Decimal("170"), filled_at=T0 + timedelta(days=3),
    )
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)

    result = await rebuild_positions_from_transactions(store, "main")
    assert result.positions == 1

    position = await store.get_position("main", "AAPL")
    assert position.quantity == Decimal("0")

    closed = await store.get_closed_positions("main")
    assert len(closed) == 1
    assert closed[0].realized_pnl == Decimal("200")  # (170-150)*10
    assert closed[0].holding_period_days == 3

    opening_txn = store._transactions.get_by_key("TXN-O-1")
    closing_txn = store._transactions.get_by_key("TXN-O-2")
    position_id = await store.get_position_id("main", "AAPL")
    assert opening_txn.position_id == position_id
    assert opening_txn.closed_position_id is None
    assert closing_txn.position_id is None
    assert closing_txn.closed_position_id is not None


async def test_rebuild_is_idempotent(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=T0,
    )
    client.fill(
        account_number="ACCT1", order_id="O-2", symbol="AAPL", instrument_type="Equity",
        action="Sell to Close", quantity=Decimal("10"), fill_price=Decimal("170"), filled_at=T0 + timedelta(days=3),
    )
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)

    await rebuild_positions_from_transactions(store, "main")
    first_closed_id = (await store.get_closed_positions("main"))[0]
    await rebuild_positions_from_transactions(store, "main")
    closed = await store.get_closed_positions("main")

    assert len(closed) == 1  # not duplicated
    assert closed[0] == first_closed_id


async def test_rebuild_covers_multiple_securities(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=T0,
    )
    client.fill(
        account_number="ACCT1", order_id="O-2", symbol="MSFT", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("300"), filled_at=T0,
    )
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)

    result = await rebuild_positions_from_transactions(store, "main")
    assert result.positions == 2


async def test_rebuild_with_no_account_covers_every_account_with_activity(store, resolver):
    accounts = AccountMapper({"main": "ACCT1", "second": "ACCT2"})
    client_main = MockTastyTradeClient()
    client_main.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=T0,
    )
    await sync_transactions(store, "main", client=client_main, accounts=accounts, resolver=resolver)

    client_second = MockTastyTradeClient()
    client_second.fill(
        account_number="ACCT2", order_id="O-2", symbol="MSFT", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("300"), filled_at=T0,
    )
    await sync_transactions(store, "second", client=client_second, accounts=accounts, resolver=resolver)

    result = await rebuild_positions_from_transactions(store)  # account=None -> all accounts
    assert result.positions == 2
    assert (await store.get_position("main", "AAPL")).quantity == Decimal("10")
    assert (await store.get_position("second", "MSFT")).quantity == Decimal("5")


async def test_rebuild_ignores_cash_only_transactions(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.add_transaction(
        BrokerTransaction(
            id="TXN-DIV", account_number="ACCT1", transaction_type="Money Movement",
            transaction_sub_type="Dividend", executed_at=T0, transaction_date=T0.date(),
            net_value=Decimal("27.74"), net_value_effect="Credit",
        )
    )
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)

    result = await rebuild_positions_from_transactions(store, "main")
    assert result.positions == 0
    assert result.errors == []


async def test_rebuild_preserves_broker_market_data_after_a_prior_sync_positions(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=T0,
    )
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    client.set_positions("ACCT1", [
        BrokerPosition(
            account_number="ACCT1", symbol="AAPL", instrument_type="Equity", quantity=Decimal("10"),
            quantity_direction="Long", average_open_price=Decimal("150"), mark_price=Decimal("162"),
        )
    ])
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)

    await rebuild_positions_from_transactions(store, "main")
    position = await store.get_position("main", "AAPL")
    assert position.mark_price == Decimal("162")  # broker-owned, untouched by replay
    assert position.quantity == Decimal("10")  # replay-owned, recomputed from transactions


# --- confirmatory test against the real SQL store --------------------------------------------


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


async def test_rebuild_open_and_close_against_sql_store(sql_store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=T0,
    )
    client.fill(
        account_number="ACCT1", order_id="O-2", symbol="AAPL", instrument_type="Equity",
        action="Sell to Close", quantity=Decimal("10"), fill_price=Decimal("170"), filled_at=T0 + timedelta(days=3),
    )
    await sync_transactions(sql_store, "main", client=client, accounts=accounts, resolver=resolver)

    result = await rebuild_positions_from_transactions(sql_store, "main")
    assert result.positions == 1

    position = await sql_store.get_position("main", "AAPL")
    assert position.quantity == Decimal("0")

    closed = await sql_store.get_closed_positions("main")
    assert len(closed) == 1
    assert closed[0].realized_pnl == Decimal("200")

    # idempotent re-run against the real SQL store too
    result2 = await rebuild_positions_from_transactions(sql_store, "main")
    assert result2.positions == 1
    assert len(await sql_store.get_closed_positions("main")) == 1
