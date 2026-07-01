"""``sync_orders`` / ``sync_transactions`` (docs/ingestion.md -> Pull).

The bulk of the logic is exercised against ``InMemoryStore`` (fast, no FK ceremony); a couple of
confirmatory tests run against the real ``SqlLedgerStore`` (parametrized over SQLite always,
Postgres when ``TT_LEDGER_TEST_PG`` is set) to prove the end-to-end wiring against a real DB too.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, UTC
from decimal import Decimal

import pytest
from sqlalchemy import select

from tt_ledger.enums import Ingest, Origin, OrderStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BrokerPosition, BrokerTransaction, PlacedFill, PlacedLeg, PlacedOrder
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_all, sync_orders, sync_positions, sync_transactions
from tt_ledger.repositories import TransactionRepository, map_order_status
from tt_ledger.rows import ActivityFilter, OrderFilter, OrderRow, TradeGroupRow
from tt_ledger.schema import metadata, models
from tt_ledger.store.memory import InMemoryStore
from tt_ledger.store.sql import SqlLedgerStore


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def resolver() -> PassthroughResolver:
    return PassthroughResolver()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


async def test_sync_orders_creates_a_broker_order_with_legs_and_fills(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150.25"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), price=Decimal("150.25"), price_effect="Debit",
        status="Filled",
    )

    count = await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert count == 1

    orders = await store.query_orders(OrderFilter(account="main"))
    assert len(orders) == 1
    order = orders[0]
    assert order.tt_order_id == "O-1"
    assert order.origin is Origin.BROKER
    assert order.ingest is Ingest.ORDER_HISTORY
    assert order.security_id == "AAPL"  # single-leg -> order-level security_id set
    assert order.oms_status == "filled"
    assert order.tt_status == "Filled"
    assert order.average_fill_price == Decimal("150.25")  # derived from the leg's single fill
    assert order.filled_quantity == Decimal("10")
    assert order.remaining_quantity == Decimal("0")
    assert order.filled_at is not None
    assert order.is_complex is False
    assert order.complex_order_type is None

    order_id = store._orders.id_of("O-1")
    legs = [row for _, row in store._legs.all() if row.order_id == order_id]
    assert len(legs) == 1
    assert legs[0].security_id == "AAPL"
    assert legs[0].action == "Buy to Open"

    fills = [row for _, row in store._fills.all() if row.tt_order_id == "O-1"]
    assert len(fills) == 1
    assert fills[0].fill_price == Decimal("150.25")
    assert fills[0].order_leg_id == store._legs.id_of((order_id, 0))

    security = store._securities.get_by_key("AAPL")
    assert security is not None
    assert security.tt_symbol == "AAPL"


async def test_sync_orders_is_idempotent(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150.25"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC),
    )
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)

    assert len(await store.query_orders(OrderFilter(account="main"))) == 1
    order_id = store._orders.id_of("O-1")
    assert len([row for _, row in store._legs.all() if row.order_id == order_id]) == 1
    assert len([row for _, row in store._fills.all() if row.tt_order_id == "O-1"]) == 1


async def test_sync_orders_sets_leg_fill_price_to_the_quantity_weighted_vwap(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.add_order(
        PlacedOrder(
            id="O-partial", account_number="ACCT1", received_at=datetime(2026, 1, 5, tzinfo=UTC),
            status="Filled", terminal_at=datetime(2026, 1, 5, tzinfo=UTC),
            legs=[
                PlacedLeg(
                    instrument_type="Equity", symbol="AAPL", action="Buy to Open",
                    quantity=Decimal("30"), remaining_quantity=Decimal("0"),
                    fills=[
                        PlacedFill(fill_id="F-1", quantity=Decimal("10"), fill_price=Decimal("100"), filled_at=datetime(2026, 1, 5, tzinfo=UTC)),
                        PlacedFill(fill_id="F-2", quantity=Decimal("20"), fill_price=Decimal("103"), filled_at=datetime(2026, 1, 5, tzinfo=UTC)),
                    ],
                ),
            ],
        )
    )
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)

    order_id = store._orders.id_of("O-partial")
    leg = next(row for _, row in store._legs.all() if row.order_id == order_id)
    # VWAP = (10*100 + 20*103) / 30 = 102
    assert leg.fill_price == Decimal("102")


async def test_sync_orders_multi_leg_iron_condor(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.add_order(
        PlacedOrder(
            id="O-IC", account_number="ACCT1", received_at=datetime(2026, 1, 5, tzinfo=UTC),
            underlying_symbol="SPX", complex_order_id="CO-1", complex_order_tag="Iron Condor",
            status="Filled", terminal_at=datetime(2026, 1, 5, 10, tzinfo=UTC),
            legs=[
                PlacedLeg(
                    instrument_type="Equity Option", symbol="SPXW  260117P05000000", action="Sell to Open",
                    quantity=Decimal("1"), remaining_quantity=Decimal("0"),
                    fills=[PlacedFill(fill_id="F-1", quantity=Decimal("1"), fill_price=Decimal("2.5"), filled_at=datetime(2026, 1, 5, tzinfo=UTC))],
                ),
                PlacedLeg(
                    instrument_type="Equity Option", symbol="SPXW  260117C05500000", action="Sell to Open",
                    quantity=Decimal("1"), remaining_quantity=Decimal("0"),
                    fills=[PlacedFill(fill_id="F-2", quantity=Decimal("1"), fill_price=Decimal("2.5"), filled_at=datetime(2026, 1, 5, tzinfo=UTC))],
                ),
            ],
        )
    )

    count = await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert count == 1

    orders = await store.query_orders(OrderFilter(account="main"))
    assert orders[0].security_id is None  # ambiguous across 2 legs -> left unset
    assert orders[0].underlying == "SPX"
    # a 2-leg spread's net price isn't a plain average of its legs' fill prices, and TastyTrade's
    # real Order object has no order-level fill fields anyway (verified against their OpenAPI
    # spec) -- these stay unset for anything but a single-leg order.
    assert orders[0].average_fill_price is None
    assert orders[0].filled_quantity is None
    assert orders[0].remaining_quantity is None
    assert orders[0].is_complex is True
    assert orders[0].complex_order_type == "Iron Condor"

    order_id = store._orders.id_of("O-IC")
    legs = sorted((row for _, row in store._legs.all() if row.order_id == order_id), key=lambda leg: leg.leg_index)
    assert [leg.security_id for leg in legs] == ["SPXW  260117P05000000", "SPXW  260117C05500000"]

    fills = {f.fill_id: f for f in (row for _, row in store._fills.all() if row.tt_order_id == "O-IC")}
    assert fills["F-1"].order_leg_id == store._legs.id_of((order_id, 0))
    assert fills["F-2"].order_leg_id == store._legs.id_of((order_id, 1))


async def test_sync_orders_enriches_existing_zts_row_without_touching_attribution(store, accounts, resolver):
    # seed a ZTS-origin order the way the (not-yet-implemented) push path would: tt_order_id
    # already known, attribution fields set.
    await store.upsert_orders(
        [
            OrderRow(
                tt_order_id="O-1", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT,
                signal_id="SIG-1", trace_id="TRACE-1", strategy_id=42, security_id="AAPL",
                oms_status="submitted",
            )
        ]
    )

    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150.25"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )

    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)

    order = (await store.query_orders(OrderFilter(account="main")))[0]
    assert order.origin is Origin.ZTS
    assert order.ingest is Ingest.OMS_SUBMIT
    assert order.signal_id == "SIG-1"
    assert order.trace_id == "TRACE-1"
    assert order.strategy_id == 42
    assert order.oms_status == "filled"  # enriched
    assert order.average_fill_price == Decimal("150.25")  # enriched
    assert order.filled_at is not None  # enriched

    # legs/fills are still created for the enriched row -- structure isn't origin-specific
    order_id = store._orders.id_of("O-1")
    assert len([row for _, row in store._legs.all() if row.order_id == order_id]) == 1


async def test_sync_orders_unrecognized_status_keeps_raw_tt_status(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("1"), fill_price=Decimal("1"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Some Future TastyTrade Status",
    )
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    order = (await store.query_orders(OrderFilter(account="main")))[0]
    assert order.oms_status is None
    assert order.tt_status == "Some Future TastyTrade Status"


async def test_sync_orders_captures_reject_reason_as_status_message(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.add_order(
        PlacedOrder(
            id="O-1", account_number="ACCT1", received_at=datetime(2026, 1, 5, tzinfo=UTC),
            status="Rejected", reject_reason="Insufficient buying power",
            terminal_at=datetime(2026, 1, 5, tzinfo=UTC),
        )
    )
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)

    order = (await store.query_orders(OrderFilter(account="main")))[0]
    assert order.oms_status == "rejected"
    assert order.status_message == "Insufficient buying power"


async def test_sync_orders_enrich_updates_status_message_from_reject_reason(store, accounts, resolver):
    # seed a ZTS-origin order the way the (not-yet-implemented) push path would.
    await store.upsert_orders(
        [OrderRow(tt_order_id="O-1", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT, oms_status="submitted")]
    )
    client = MockTastyTradeClient()
    client.add_order(
        PlacedOrder(
            id="O-1", account_number="ACCT1", received_at=datetime(2026, 1, 5, tzinfo=UTC),
            status="Rejected", reject_reason="Insufficient buying power",
            terminal_at=datetime(2026, 1, 5, tzinfo=UTC),
        )
    )
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)

    order = await store.get_order("O-1")
    assert order.origin is Origin.ZTS  # untouched
    assert order.status_message == "Insufficient buying power"  # enriched


async def test_sync_orders_uses_since_as_the_lower_bound(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-old", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("1"), fill_price=Decimal("1"),
        filled_at=datetime(2020, 1, 1, tzinfo=UTC),
    )
    client.fill(
        account_number="ACCT1", order_id="O-new", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("1"), fill_price=Decimal("1"),
        filled_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    count = await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver, since=date(2025, 1, 1))
    assert count == 1
    orders = await store.query_orders(OrderFilter(account="main"))
    assert [o.tt_order_id for o in orders] == ["O-new"]


async def test_sync_orders_returns_zero_for_no_orders(store, accounts, resolver):
    count = await sync_orders(store, "main", client=MockTastyTradeClient(), accounts=accounts, resolver=resolver)
    assert count == 0


# --- confirmatory tests against the real SQL store ---------------------------------------


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


async def test_sync_orders_against_sql_store_fresh_and_idempotent(sql_store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150.25"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )

    count1 = await sync_orders(sql_store, "main", client=client, accounts=accounts, resolver=resolver)
    count2 = await sync_orders(sql_store, "main", client=client, accounts=accounts, resolver=resolver)
    assert count1 == count2 == 1

    orders = await sql_store.query_orders(OrderFilter(account="main"))
    assert len(orders) == 1
    assert orders[0].oms_status == "filled"
    assert orders[0].average_fill_price == Decimal("150.25")

    async with sql_store._sessionmaker() as session:
        legs = (await session.execute(select(models.OrderLeg.__table__))).all()
        fills = (await session.execute(select(models.OrderFill.__table__))).all()
        securities = (await session.execute(select(models.Security.__table__))).all()
    assert len(legs) == 1
    assert len(fills) == 1
    assert len(securities) == 1


async def test_sync_orders_against_sql_store_enriches_zts_row(sql_store, accounts, resolver):
    async with sql_store._sessionmaker() as session, session.begin():
        await session.execute(models.Security.__table__.insert().values(security_id="AAPL", product_type="S"))
    await sql_store.upsert_orders(
        [
            OrderRow(
                tt_order_id="O-1", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT,
                signal_id="SIG-1", strategy_id=42, security_id="AAPL", oms_status="submitted",
            )
        ]
    )
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("1"), fill_price=Decimal("100"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )

    await sync_orders(sql_store, "main", client=client, accounts=accounts, resolver=resolver)

    order = (await sql_store.query_orders(OrderFilter(account="main")))[0]
    assert order.origin is Origin.ZTS
    assert order.signal_id == "SIG-1"
    assert order.strategy_id == 42
    assert order.oms_status == "filled"


# --- sync_transactions ---------------------------------------------------------------------


async def test_sync_transactions_creates_rows_and_resolves_security(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.add_transaction(
        BrokerTransaction(
            id="TXN-1", account_number="ACCT1", order_id="O-1", underlying_symbol="AAPL", symbol="AAPL",
            instrument_type="Equity", transaction_type="Trade", action="Buy to Open",
            quantity=Decimal("10"), price=Decimal("150.25"), net_value=Decimal("-1502.50"),
            net_value_effect="Debit", commission=Decimal("1.00"), clearing_fees=Decimal("0.05"),
            executed_at=datetime(2026, 1, 5, tzinfo=UTC), transaction_date=date(2026, 1, 5),
        )
    )

    count = await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert count == 1

    activity = await store.account_activity(ActivityFilter(account="main"))
    assert len(activity) == 1
    row = activity[0]
    assert row.tt_transaction_id == "TXN-1"
    assert row.tt_order_id == "O-1"
    assert row.security_id == "AAPL"
    assert row.underlying == "AAPL"
    assert row.net_value == Decimal("-1502.50")
    assert row.commission == Decimal("1.00")
    assert row.clearing_fees == Decimal("0.05")
    assert row.order_id is None  # linking is the reconcile pass's job, not this importer's

    security = store._securities.get_by_key("AAPL")
    assert security is not None
    assert security.tt_symbol == "AAPL"


async def test_sync_transactions_is_idempotent(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.add_transaction(
        BrokerTransaction(
            id="TXN-1", account_number="ACCT1", symbol="AAPL", instrument_type="Equity",
            net_value=Decimal("-100"), executed_at=datetime(2026, 1, 5, tzinfo=UTC),
        )
    )
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)

    activity = await store.account_activity(ActivityFilter(account="main"))
    assert len(activity) == 1


async def test_sync_transactions_without_a_symbol_leaves_security_id_none(store, accounts, resolver):
    # e.g. a cash movement: dividend, interest, ACH transfer -- no security involved at all.
    client = MockTastyTradeClient()
    client.add_transaction(
        BrokerTransaction(
            id="TXN-1", account_number="ACCT1", transaction_type="Money Movement",
            transaction_sub_type="Transfer", net_value=Decimal("5000"),
            executed_at=datetime(2026, 1, 5, tzinfo=UTC),
        )
    )
    count = await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert count == 1

    activity = await store.account_activity(ActivityFilter(account="main"))
    assert activity[0].security_id is None


async def test_sync_transactions_uses_since_as_the_lower_bound(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.add_transaction(BrokerTransaction(id="TXN-old", account_number="ACCT1", executed_at=datetime(2020, 1, 1, tzinfo=UTC)))
    client.add_transaction(BrokerTransaction(id="TXN-new", account_number="ACCT1", executed_at=datetime(2026, 1, 1, tzinfo=UTC)))

    count = await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver, since=date(2025, 1, 1))
    assert count == 1
    activity = await store.account_activity(ActivityFilter(account="main"))
    assert [a.tt_transaction_id for a in activity] == ["TXN-new"]


async def test_sync_transactions_returns_zero_for_no_transactions(store, accounts, resolver):
    count = await sync_transactions(store, "main", client=MockTastyTradeClient(), accounts=accounts, resolver=resolver)
    assert count == 0


async def test_sync_orders_then_sync_transactions_then_link(store, accounts, resolver):
    """The intended pipeline: pull orders, pull transactions, then reconcile's link step ties
    ``tt_order_id`` to the order's surrogate id (sync_transactions itself never does this)."""
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150.25"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    await sync_orders(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=client, accounts=accounts, resolver=resolver)

    activity_before = await store.account_activity(ActivityFilter(account="main"))
    assert activity_before[0].order_id is None

    linked = await TransactionRepository(store, resolver=resolver).link_to_orders("main")
    assert linked == 1

    activity_after = await store.account_activity(ActivityFilter(account="main"))
    assert activity_after[0].order_id is not None
    assert activity_after[0].origin is Origin.BROKER


# --- confirmatory tests against the real SQL store ---------------------------------------


async def test_sync_transactions_against_sql_store_fresh_and_idempotent(sql_store, accounts, resolver):
    client = MockTastyTradeClient()
    client.add_transaction(
        BrokerTransaction(
            id="TXN-1", account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
            transaction_type="Trade", quantity=Decimal("10"), price=Decimal("150.25"),
            net_value=Decimal("-1502.50"), commission=Decimal("1.00"), clearing_fees=Decimal("0.05"),
            regulatory_fees=Decimal("0.01"), executed_at=datetime(2026, 1, 5, tzinfo=UTC),
            transaction_date=date(2026, 1, 5),
        )
    )
    count1 = await sync_transactions(sql_store, "main", client=client, accounts=accounts, resolver=resolver)
    count2 = await sync_transactions(sql_store, "main", client=client, accounts=accounts, resolver=resolver)
    assert count1 == count2 == 1

    activity = await sql_store.account_activity(ActivityFilter(account="main"))
    assert len(activity) == 1
    assert activity[0].net_value == Decimal("-1502.50")
    assert activity[0].commission == Decimal("1.00")
    assert activity[0].clearing_fees == Decimal("0.05")


# --- sync_positions ------------------------------------------------------------------------


async def test_sync_positions_creates_rows_and_resolves_security(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.set_positions(
        "ACCT1",
        [
            BrokerPosition(
                account_number="ACCT1", symbol="AAPL", instrument_type="Equity",
                quantity=Decimal("100"), quantity_direction="Long",
                average_open_price=Decimal("150"), mark_price=Decimal("155.50"), multiplier=1,
            )
        ],
    )

    count = await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert count == 1

    position = await store.get_position("main", "AAPL")
    assert position is not None
    assert position.quantity == Decimal("100")
    assert position.quantity_direction == "Long"
    assert position.average_open_price == Decimal("150")
    assert position.mark_price == Decimal("155.50")
    assert position.unrealized_pnl == Decimal("550.00")  # derived: (155.50-150)*100*1, no broker field for this

    security = store._securities.get_by_key("AAPL")
    assert security is not None
    assert security.tt_symbol == "AAPL"


async def test_sync_positions_is_idempotent(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("100"), quantity_direction="Long")],
    )
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)

    assert len(store._positions.all()) == 1


async def test_sync_positions_updates_market_data_but_preserves_attribution(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("100"), quantity_direction="Long", mark_price=Decimal("150"))],
    )
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)

    # simulate a reconcile pass having attributed this position to a trade group
    existing = await store.get_position("main", "AAPL")
    await store.upsert_positions([replace(existing, trade_group_id=7, opening_order_id=99, strategy_id=3)])

    # a later sync_positions call must not clobber that attribution, but must refresh market data
    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("120"), quantity_direction="Long", mark_price=Decimal("160"))],
    )
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)

    updated = await store.get_position("main", "AAPL")
    assert updated.quantity == Decimal("120")
    assert updated.mark_price == Decimal("160")
    assert updated.trade_group_id == 7
    assert updated.opening_order_id == 99
    assert updated.strategy_id == 3


async def test_sync_positions_returns_zero_for_no_positions(store, accounts, resolver):
    count = await sync_positions(store, "main", client=MockTastyTradeClient(), accounts=accounts, resolver=resolver)
    assert count == 0


async def test_sync_positions_derives_unrealized_pnl_for_a_short_position(store, accounts, resolver):
    # TastyTrade's real CurrentPosition has no unrealized-pnl field -- for a short, a rising mark
    # is a loss, so the sign flips relative to the long-position case tested above.
    client = MockTastyTradeClient()
    client.set_positions(
        "ACCT1",
        [
            BrokerPosition(
                account_number="ACCT1", symbol="AAPL", instrument_type="Equity",
                quantity=Decimal("100"), quantity_direction="Short",
                average_open_price=Decimal("150"), mark_price=Decimal("155.50"),
            )
        ],
    )
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)

    position = await store.get_position("main", "AAPL")
    assert position.unrealized_pnl == Decimal("-550.00")  # -(155.50-150)*100*1


async def test_sync_positions_leaves_unrealized_pnl_none_without_mark_or_open_price(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("100"), quantity_direction="Long")],
    )
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)

    position = await store.get_position("main", "AAPL")
    assert position.unrealized_pnl is None


async def test_sync_positions_applies_realized_day_gain_effect(store, accounts, resolver):
    # TastyTrade sends realized-day-gain as a magnitude + a separate Credit/Debit/None effect
    # string (the same convention as BrokerTransaction's value/value-effect) -- PositionRepository
    # combines them into the one signed value the internal schema stores.
    client = MockTastyTradeClient()
    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("100"), quantity_direction="Long", realized_day_gain=Decimal("42"), realized_day_gain_effect="Credit")],
    )
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert (await store.get_position("main", "AAPL")).realized_day_gain == Decimal("42")

    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("100"), quantity_direction="Long", realized_day_gain=Decimal("42"), realized_day_gain_effect="Debit")],
    )
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert (await store.get_position("main", "AAPL")).realized_day_gain == Decimal("-42")

    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("100"), quantity_direction="Long", realized_day_gain=Decimal("0"), realized_day_gain_effect="None")],
    )
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert (await store.get_position("main", "AAPL")).realized_day_gain == Decimal("0")

    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("100"), quantity_direction="Long")],
    )
    await sync_positions(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert (await store.get_position("main", "AAPL")).realized_day_gain is None


async def test_sync_positions_against_sql_store_fresh_idempotent_and_preserves_attribution(sql_store, accounts, resolver):
    client = MockTastyTradeClient()
    client.set_positions(
        "ACCT1",
        [
            BrokerPosition(
                account_number="ACCT1", symbol="AAPL", instrument_type="Equity",
                quantity=Decimal("100"), quantity_direction="Long",
                average_open_price=Decimal("150"), mark_price=Decimal("155.50"),
            )
        ],
    )
    count1 = await sync_positions(sql_store, "main", client=client, accounts=accounts, resolver=resolver)
    count2 = await sync_positions(sql_store, "main", client=client, accounts=accounts, resolver=resolver)
    assert count1 == count2 == 1

    position = await sql_store.get_position("main", "AAPL")
    assert position.mark_price == Decimal("155.50")
    assert position.unrealized_pnl == Decimal("550.00")  # derived: (155.50-150)*100*1

    # trade_group_id/opening_order_id are real FKs (unlike strategy_id, a documented soft ref) --
    # seed the rows they'd actually reference.
    await sql_store.upsert_trade_group(TradeGroupRow(group_id="GRP-1", account="main", origin=Origin.BROKER))
    async with sql_store._sessionmaker() as session:
        group_pk = (
            await session.execute(select(models.TradeGroup.__table__.c.id).where(models.TradeGroup.__table__.c.group_id == "GRP-1"))
        ).scalar_one()
    await sql_store.upsert_positions([replace(position, trade_group_id=group_pk)])

    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("100"), quantity_direction="Long", mark_price=Decimal("160"))],
    )
    await sync_positions(sql_store, "main", client=client, accounts=accounts, resolver=resolver)

    updated = await sql_store.get_position("main", "AAPL")
    assert updated.mark_price == Decimal("160")
    assert updated.trade_group_id == group_pk


# --- sync_all --------------------------------------------------------------------------------


class _FlakyPositionsClient:
    """Wraps a MockTastyTradeClient but fails the positions feed -- for exercising sync_all's
    per-step error isolation."""

    def __init__(self, inner: MockTastyTradeClient) -> None:
        self._inner = inner

    async def get_order_history(self, account_number, start, end):  # noqa: ANN001
        return await self._inner.get_order_history(account_number, start, end)

    async def get_transaction_history(self, account_number, start, end):  # noqa: ANN001
        return await self._inner.get_transaction_history(account_number, start, end)

    async def get_positions(self, account_number):  # noqa: ANN001
        raise RuntimeError("broker positions endpoint is down")


async def test_sync_all_runs_orders_transactions_and_positions(store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("10"), quantity_direction="Long")],
    )

    result = await sync_all(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert result.orders == 1
    assert result.transactions == 1  # fill() seeds a matching transaction too
    assert result.positions == 1
    assert result.errors == []


async def test_sync_all_with_no_data_returns_zero_counts(store, accounts, resolver):
    result = await sync_all(store, "main", client=MockTastyTradeClient(), accounts=accounts, resolver=resolver)
    assert result.orders == 0
    assert result.transactions == 0
    assert result.positions == 0
    assert result.errors == []


async def test_sync_all_continues_past_a_failing_step_and_records_the_error(store, accounts, resolver):
    inner = MockTastyTradeClient()
    inner.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    client = _FlakyPositionsClient(inner)

    result = await sync_all(store, "main", client=client, accounts=accounts, resolver=resolver)
    assert result.orders == 1
    assert result.transactions == 1
    assert result.positions == 0
    assert len(result.errors) == 1
    assert "sync_positions" in result.errors[0]

    # the successful steps' work is still there despite the later failure
    assert len(await store.query_orders(OrderFilter(account="main"))) == 1


async def test_sync_all_against_sql_store(sql_store, accounts, resolver):
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("10"), quantity_direction="Long")],
    )

    result = await sync_all(sql_store, "main", client=client, accounts=accounts, resolver=resolver)
    assert result.orders == 1
    assert result.transactions == 1
    assert result.positions == 1
    assert result.errors == []


# --- map_order_status: verified against TastyTrade's real Order Flow vocabulary -------------


def test_map_order_status_matches_the_documented_vocabulary():
    assert map_order_status("Received") is OrderStatus.PENDING
    assert map_order_status("Routed") is OrderStatus.SUBMITTED
    assert map_order_status("In Flight") is OrderStatus.SUBMITTED
    assert map_order_status("Live") is OrderStatus.WORKING
    assert map_order_status("Cancel Requested") is OrderStatus.WORKING
    assert map_order_status("Replace Requested") is OrderStatus.WORKING
    assert map_order_status("Contingent") is OrderStatus.WORKING
    assert map_order_status("Filled") is OrderStatus.FILLED
    assert map_order_status("Cancelled") is OrderStatus.CANCELLED
    assert map_order_status("Rejected") is OrderStatus.REJECTED
    assert map_order_status("Expired") is OrderStatus.EXPIRED


def test_map_order_status_removed_and_partially_removed_are_both_cancelled():
    # "Partially Removed" means an admin manually removed part of the order -- an admin action,
    # unrelated to fills -- so it maps like "Removed" does, not like a partial fill.
    assert map_order_status("Removed") is OrderStatus.CANCELLED
    assert map_order_status("Partially Removed") is OrderStatus.CANCELLED


def test_map_order_status_has_no_partially_filled_entry():
    # TastyTrade's real order-status vocabulary has no "Partially Filled" status at all --
    # partial-fill information lives on the leg's quantity/remaining-quantity, not order status.
    assert map_order_status("Partially Filled") is None
