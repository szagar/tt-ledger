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


# --- net P&L (fees + pnl_net) ------------------------------------------------------------------


def _fee_row(tt_transaction_id, *, quantity, action, price, executed_at=T0,
             commission="1", clearing="0.1", regulatory="0.05"):
    return ActivityRow(
        tt_transaction_id=tt_transaction_id, account="main", security_id="AAPL",
        quantity=quantity, action=action, price=price, executed_at=executed_at,
        commission=Decimal(commission), clearing_fees=Decimal(clearing),
        regulatory_fees=Decimal(regulatory),
    )


def test_closed_row_nets_fees_across_the_whole_lifecycle():
    rows = [
        _fee_row("T1", quantity=Decimal("10"), action="Buy to Open", price=Decimal("100")),
        _fee_row("T2", quantity=Decimal("10"), action="Sell to Close", price=Decimal("110"),
                 executed_at=T0 + timedelta(days=1)),
    ]
    _, plan = _replay_security("main", "AAPL", rows, 1, None)

    closed = plan[-1][2]
    assert closed.realized_pnl == Decimal("100")          # gross: (110-100) * 10
    assert closed.fees == Decimal("2.30")                 # both legs: 2 * (1 + 0.1 + 0.05)
    assert closed.pnl_net == Decimal("97.70")


def test_flip_fees_attach_to_the_closing_lifecycle_not_the_new_lot():
    rows = [
        _fee_row("T1", quantity=Decimal("10"), action="Buy to Open", price=Decimal("100")),
        # sell 15: closes the 10-lot AND opens a short 5 -- its fees belong to the closed lifecycle
        _fee_row("T2", quantity=Decimal("15"), action="Sell to Open", price=Decimal("110"),
                 executed_at=T0 + timedelta(days=1)),
        _fee_row("T3", quantity=Decimal("5"), action="Buy to Close", price=Decimal("105"),
                 executed_at=T0 + timedelta(days=2)),
    ]
    _, plan = _replay_security("main", "AAPL", rows, 1, None)

    first_closed = plan[1][2]
    assert first_closed.fees == Decimal("2.30")           # T1 + T2 fees
    second_closed = plan[2][2]
    assert second_closed.realized_pnl == Decimal("25")    # short 5 @110 covered @105
    assert second_closed.fees == Decimal("1.15")          # T3 only -- T2's went to the first lifecycle
    assert second_closed.pnl_net == Decimal("23.85")


# --- Receive Deliver settlements (expiration / assignment / exercise) ---------------------------


def _settlement_row(tt_transaction_id, *, quantity, sub_type="Expiration", price=None,
                    executed_at=T0 + timedelta(days=30), security_id="AAPL"):
    return ActivityRow(
        tt_transaction_id=tt_transaction_id, account="main", security_id=security_id,
        quantity=quantity, action=None, price=price, executed_at=executed_at,
        transaction_type="Receive Deliver", transaction_sub_type=sub_type,
    )


def test_expiration_closes_a_long_lot_at_zero():
    """Regression: settlement rows carry no action and no price, so replay used to skip them
    entirely -- expired lots stayed open forever (196 phantom open positions in the first live
    backfill rehearsal)."""
    rows = [
        _row("T1", quantity=Decimal("2"), action="Buy to Open", price=Decimal("1.50")),
        _settlement_row("RD1", quantity=Decimal("2")),
    ]
    position, plan = _replay_security("main", "AAPL", rows, 100, None)

    assert position.quantity == Decimal("0")
    closed = plan[-1][2]
    assert closed is not None
    assert closed.average_close_price == Decimal("0")
    assert closed.realized_pnl == Decimal("-300")  # paid 1.50 x 2 x 100, expired worthless


def test_expiration_closes_a_short_lot_keeping_the_credit():
    rows = [
        _row("T1", quantity=Decimal("1"), action="Sell to Open", price=Decimal("2.50")),
        _settlement_row("RD1", quantity=Decimal("1")),
    ]
    position, plan = _replay_security("main", "AAPL", rows, 100, None)

    assert position.quantity == Decimal("0")
    closed = plan[-1][2]
    assert closed.realized_pnl == Decimal("250")  # sold 2.50, expired worthless


def test_cash_settled_exercise_uses_the_rows_price_when_present():
    rows = [
        _row("T1", quantity=Decimal("1"), action="Buy to Open", price=Decimal("3.00")),
        _settlement_row("RD1", quantity=Decimal("1"), sub_type="Cash Settled Exercise", price=Decimal("8.00")),
    ]
    _, plan = _replay_security("main", "AAPL", rows, 100, None)

    closed = plan[-1][2]
    assert closed.realized_pnl == Decimal("500")  # (8 - 3) x 1 x 100


def test_settlement_against_a_flat_lot_is_a_noop():
    rows = [_settlement_row("RD1", quantity=Decimal("1"))]
    position, plan = _replay_security("main", "AAPL", rows, 100, None)
    assert position.quantity == Decimal("0")
    assert plan == [("RD1", False, None)]


def test_mark_to_market_money_movements_do_not_change_quantity():
    """Regression: futures daily Mark to Market rows carry quantity + price but are cash-only —
    counting them as fills inflated futures lots by a contract per settlement day."""
    rows = [
        _row("T1", quantity=Decimal("1"), action="Buy", price=Decimal("6900")),
        ActivityRow(
            tt_transaction_id="MM1", account="main", security_id="AAPL",
            quantity=Decimal("1"), action=None, price=Decimal("6963.25"),
            executed_at=T0 + timedelta(days=1),
            transaction_type="Money Movement", transaction_sub_type="Mark to Market",
        ),
        _row("T2", quantity=Decimal("1"), action="Sell", price=Decimal("7000"), executed_at=T0 + timedelta(days=2)),
    ]
    position, plan = _replay_security("main", "AAPL", rows, 50, None)

    assert position.quantity == Decimal("0")
    assert plan[1] == ("MM1", False, None)
    closed = plan[-1][2]
    assert closed.realized_pnl == Decimal("5000")  # (7000-6900) x 1 x 50, MTM untouched


def test_close_against_a_flat_lot_is_a_window_artifact_noop():
    """Regression: a '* to Close' whose open predates the sync window fabricated a phantom
    fresh lot (196 phantom open positions in the first live rehearsal)."""
    rows = [_row("T1", quantity=Decimal("1"), action="Buy to Close", price=Decimal("5.00"))]
    position, plan = _replay_security("main", "AAPL", rows, 100, None)
    assert position.quantity == Decimal("0")
    assert plan == [("T1", False, None)]


async def test_same_timestamp_delivery_batch_applies_opens_before_closes(store_url):
    """An option-exercise delivery books the delivered future's open and its offsetting close
    at the same instant; close-first would hit the close-on-flat no-op and strand the open."""
    from tt_ledger.identity import AccountMapper, PassthroughResolver
    from tt_ledger.ingest.mock_broker import MockTastyTradeClient
    from tt_ledger.ingest.broker import BrokerTransaction
    from tt_ledger.ingest.pull import sync_transactions
    from tt_ledger.ingest.replay import rebuild_positions_from_transactions
    from tt_ledger.rows import AccountRow
    from tt_ledger.schema import metadata
    from tt_ledger.store.sql import SqlLedgerStore

    store = SqlLedgerStore(store_url)
    async with store._engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await store.create_all()
    try:
        await store.upsert_account(AccountRow(nickname="main", account_number="ACCT1", login="u"))
        accounts = AccountMapper({"main": "ACCT1"})
        client = MockTastyTradeClient()
        ts = T0
        # deliberately add the close FIRST -- ordering must not matter
        client.add_transaction(BrokerTransaction(
            id="RD-close", account_number="ACCT1", symbol="/ESH6", instrument_type="Future",
            transaction_type="Receive Deliver", transaction_sub_type="Sell to Close",
            action="Sell to Close", quantity=Decimal("1"), price=Decimal("6650"),
            executed_at=ts, transaction_date=ts.date(),
        ))
        client.add_transaction(BrokerTransaction(
            id="RD-open", account_number="ACCT1", symbol="/ESH6", instrument_type="Future",
            transaction_type="Receive Deliver", transaction_sub_type="Buy to Open",
            action="Buy to Open", quantity=Decimal("1"), price=Decimal("6700"),
            executed_at=ts, transaction_date=ts.date(),
        ))
        await sync_transactions(store, "main", client=client, accounts=accounts, resolver=PassthroughResolver())
        await rebuild_positions_from_transactions(store, "main")

        pos = await store.get_position("main", "/ESH6")
        assert pos is not None and pos.quantity == Decimal("0")
    finally:
        await store.dispose()


async def test_resolver_multiplier_reaches_replay_pnl(store_url):
    """A resolver-supplied contract multiplier lands on the securities dimension and scales
    replay P&L into dollars (without it, futures/options closed_positions were per-unit)."""
    from tt_ledger.identity import AccountMapper
    from tt_ledger.identity.securities import ResolvedSecurity
    from tt_ledger.ingest.broker import BrokerTransaction
    from tt_ledger.ingest.mock_broker import MockTastyTradeClient
    from tt_ledger.ingest.pull import sync_transactions
    from tt_ledger.ingest.replay import rebuild_positions_from_transactions
    from tt_ledger.rows import AccountRow
    from tt_ledger.schema import metadata
    from tt_ledger.store.sql import SqlLedgerStore

    class FiftyXResolver:
        def resolve(self, vendor_symbol, instrument_type=None):
            return ResolvedSecurity(security_id=vendor_symbol, product_type="F", multiplier=50)

    store = SqlLedgerStore(store_url)
    async with store._engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await store.create_all()
    try:
        await store.upsert_account(AccountRow(nickname="main", account_number="ACCT1", login="u"))
        accounts = AccountMapper({"main": "ACCT1"})
        client = MockTastyTradeClient()
        client.add_transaction(BrokerTransaction(
            id="T1", account_number="ACCT1", symbol="/ESM6", instrument_type="Future",
            transaction_type="Trade", action="Sell to Open", quantity=Decimal("1"),
            price=Decimal("7350"), executed_at=T0, transaction_date=T0.date(),
        ))
        client.add_transaction(BrokerTransaction(
            id="T2", account_number="ACCT1", symbol="/ESM6", instrument_type="Future",
            transaction_type="Trade", action="Buy to Close", quantity=Decimal("1"),
            price=Decimal("7421.25"), executed_at=T0 + timedelta(days=1),
            transaction_date=(T0 + timedelta(days=1)).date(),
        ))
        await sync_transactions(store, "main", client=client, accounts=accounts, resolver=FiftyXResolver())
        await rebuild_positions_from_transactions(store, "main")

        closed = await store.get_closed_positions("main", "/ESM6")
        assert len(closed) == 1
        assert closed[0].realized_pnl == Decimal("-3562.50")  # (7350 - 7421.25) x 1 x 50
    finally:
        await store.dispose()
