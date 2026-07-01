"""``MockTastyTradeClient`` (docs/ingestion.md -> Pull) — the fake BrokerClient used to test
ingest/pull.py without a live TastyTrade connection."""

from __future__ import annotations

from datetime import date, datetime, UTC
from decimal import Decimal

from tt_ledger.ingest.broker import BrokerClient, BrokerPosition, BrokerTransaction, PlacedFill, PlacedLeg, PlacedOrder
from tt_ledger.ingest.mock_broker import MockTastyTradeClient


def test_conforms_to_broker_client_protocol():
    assert isinstance(MockTastyTradeClient(), BrokerClient)


async def test_get_order_history_filters_by_account_and_date_range():
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC),
    )
    client.fill(
        account_number="ACCT1", order_id="O-2", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("151"),
        filled_at=datetime(2026, 2, 5, tzinfo=UTC),
    )
    client.fill(
        account_number="ACCT2", order_id="O-3", symbol="MSFT", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("1"), fill_price=Decimal("300"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC),
    )

    result = await client.get_order_history("ACCT1", date(2026, 1, 1), date(2026, 1, 31))
    assert [o.id for o in result] == ["O-1"]

    all_acct1 = await client.get_order_history("ACCT1", date(2026, 1, 1), date(2026, 3, 1))
    assert [o.id for o in all_acct1] == ["O-1", "O-2"]  # sorted by received_at

    assert await client.get_order_history("ACCT3", date(2026, 1, 1), date(2026, 12, 31)) == []


async def test_fill_convenience_creates_matching_order_and_transaction():
    client = MockTastyTradeClient()
    order = client.fill(
        account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150.25"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), price=Decimal("150.25"), price_effect="Debit",
    )
    assert order.id == "O-1"
    assert len(order.legs) == 1
    assert order.legs[0].fills[0].fill_price == Decimal("150.25")

    orders = await client.get_order_history("ACCT1", date(2026, 1, 1), date(2026, 1, 31))
    assert orders == [order]

    txns = await client.get_transaction_history("ACCT1", date(2026, 1, 1), date(2026, 1, 31))
    assert len(txns) == 1
    assert txns[0].order_id == "O-1"
    assert txns[0].quantity == Decimal("10")
    assert txns[0].price == Decimal("150.25")


async def test_get_transaction_history_filters_by_account_and_date_range():
    client = MockTastyTradeClient()
    client.add_transaction(
        BrokerTransaction(
            id="T-1", account_number="ACCT1", executed_at=datetime(2026, 1, 5, tzinfo=UTC),
            transaction_type="Trade",
        )
    )
    client.add_transaction(
        BrokerTransaction(
            id="T-2", account_number="ACCT1", executed_at=datetime(2026, 3, 5, tzinfo=UTC),
            transaction_type="Trade",
        )
    )

    result = await client.get_transaction_history("ACCT1", date(2026, 1, 1), date(2026, 1, 31))
    assert [t.id for t in result] == ["T-1"]


async def test_get_positions_returns_the_seeded_snapshot():
    client = MockTastyTradeClient()
    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol="AAPL", quantity=Decimal("100"), quantity_direction="Long")],
    )
    positions = await client.get_positions("ACCT1")
    assert len(positions) == 1
    assert positions[0].symbol == "AAPL"
    assert await client.get_positions("ACCT2") == []


async def test_pagination_reassembles_all_pages():
    client = MockTastyTradeClient(page_size=2)
    for i in range(5):
        client.fill(
            account_number="ACCT1", order_id=f"O-{i}", symbol="AAPL", instrument_type="Equity",
            action="Buy to Open", quantity=Decimal("1"), fill_price=Decimal("150"),
            filled_at=datetime(2026, 1, i + 1, tzinfo=UTC),
        )

    result = await client.get_order_history("ACCT1", date(2026, 1, 1), date(2026, 1, 31))
    assert len(result) == 5
    assert client.last_page_count == 3  # ceil(5 / 2)


async def test_pagination_of_empty_result_is_one_page():
    client = MockTastyTradeClient()
    await client.get_order_history("ACCT1", date(2026, 1, 1), date(2026, 1, 31))
    assert client.last_page_count == 1


async def test_calls_are_logged_for_assertions():
    client = MockTastyTradeClient()
    await client.get_order_history("ACCT1", date(2026, 1, 1), date(2026, 1, 31))
    await client.get_positions("ACCT1")
    assert client.calls == [
        ("get_order_history", "ACCT1", date(2026, 1, 1), date(2026, 1, 31)),
        ("get_positions", "ACCT1", None, None),
    ]


def test_multi_leg_order_can_be_built_directly():
    client = MockTastyTradeClient()
    order = PlacedOrder(
        id="O-IC", account_number="ACCT1", received_at=datetime(2026, 1, 5, tzinfo=UTC),
        underlying_symbol="SPX", complex_order_id="CO-1", complex_order_tag="Iron Condor",
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
    client.add_order(order)
    assert len(order.legs) == 2
