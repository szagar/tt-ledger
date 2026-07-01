"""``InMemoryStore`` behavior — same conflict-key/read-model semantics as ``SqlLedgerStore``
(docs/storage.md), exercised without a database. No FK enforcement here (unlike SQL), so tests
don't need to seed accounts/securities first.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, UTC
from decimal import Decimal

import pytest

from tt_ledger.enums import Ingest, Origin, ReviewStatus
from tt_ledger.rows import (
    ActivityFilter,
    EventRow,
    FillRow,
    LegRow,
    OrderFilter,
    OrderRow,
    PositionRow,
    SecurityRow,
    TradeFilter,
    TradeGroupRow,
    TxnRow,
)
from tt_ledger.store.memory import InMemoryStore


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


async def test_upsert_orders_is_idempotent_by_tt_order_id(store):
    order = OrderRow(
        tt_order_id="TT-1", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY,
        security_id="/ESM6", oms_status="filled", price=Decimal("1234.5678"),
    )
    await store.upsert_orders([order])
    await store.upsert_orders([replace(order, oms_status="working", price=Decimal("2.25"))])

    rows = await store.query_orders(OrderFilter(account="main"))
    assert len(rows) == 1
    assert rows[0].oms_status == "working"
    assert rows[0].price == Decimal("2.25")
    assert rows[0].origin is Origin.BROKER


async def test_upsert_orders_with_null_tt_order_id_never_conflict(store):
    rows = [OrderRow(tt_order_id=None, account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT) for _ in range(3)]
    await store.upsert_orders(rows)
    await store.upsert_orders(rows)

    result = await store.query_orders(OrderFilter(account="main"))
    assert len(result) == 6


async def test_upsert_legs_idempotent_by_order_and_index(store):
    await store.upsert_orders([OrderRow(tt_order_id="TT-2", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)])
    order_id = store._orders.id_of("TT-2")

    leg = LegRow(order_id=order_id, leg_index=0, security_id="/ESM6", action="buy_to_open", quantity=Decimal("1"))
    await store.upsert_legs([leg])
    await store.upsert_legs([replace(leg, action="sell_to_close", quantity=Decimal("2"))])

    legs = [row for _, row in store._legs.all() if row.order_id == order_id]
    assert len(legs) == 1
    assert legs[0].action == "sell_to_close"
    assert legs[0].quantity == Decimal("2")


async def test_upsert_fills_idempotent_by_fill_id(store):
    await store.upsert_orders([OrderRow(tt_order_id="TT-3", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)])
    order_id = store._orders.id_of("TT-3")

    fill = FillRow(fill_id="F-1", order_id=order_id, tt_order_id="TT-3", quantity=Decimal("1"), fill_price=Decimal("100"))
    await store.upsert_fills([fill])
    await store.upsert_fills([replace(fill, fill_price=Decimal("101"))])

    fills = [row for _, row in store._fills.all() if row.fill_id == "F-1"]
    assert len(fills) == 1
    assert fills[0].fill_price == Decimal("101")


async def test_upsert_transactions_idempotent_and_linking(store):
    await store.upsert_orders([OrderRow(tt_order_id="TT-4", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)])

    txn = TxnRow(
        tt_transaction_id="TXN-1", tt_order_id="TT-4", account="main",
        net_value=Decimal("-100.50"), executed_at=datetime(2026, 1, 5, tzinfo=UTC),
    )
    await store.upsert_transactions([txn])
    await store.upsert_transactions([replace(txn, net_value=Decimal("-99.99"))])

    stored = store._transactions.get_by_key("TXN-1")
    assert stored.net_value == Decimal("-99.99")
    assert stored.order_id is None  # not yet linked

    linked = await store.link_transactions_to_orders("main")
    assert linked == 1

    order_id = store._orders.id_of("TT-4")
    assert store._transactions.get_by_key("TXN-1").order_id == order_id

    assert await store.link_transactions_to_orders("main") == 0  # idempotent


async def test_upsert_positions_idempotent_by_account_and_security(store):
    pos = PositionRow(account="main", security_id="AAPL", quantity=Decimal("100"), quantity_direction="long")
    await store.upsert_positions([pos])
    await store.upsert_positions([replace(pos, quantity=Decimal("150"), mark_price=Decimal("190.25"))])

    stored = store._positions.get_by_key(("main", "AAPL"))
    assert stored.quantity == Decimal("150")
    assert stored.mark_price == Decimal("190.25")


async def test_upsert_security_idempotent(store):
    await store.upsert_security(SecurityRow(security_id="/ESM6", product_type="F", underlying="/ES", tt_symbol="/ESM6"))
    await store.upsert_security(SecurityRow(security_id="/ESM6", product_type="F", underlying="/ES", tt_symbol="ES-updated"))

    stored = store._securities.get_by_key("/ESM6")
    assert stored.tt_symbol == "ES-updated"


async def test_trade_group_lifecycle_and_events(store):
    tg = TradeGroupRow(
        group_id="GRP-1", account="main", origin=Origin.BROKER, underlying="/ES",
        strategy_type="single", executed_at=datetime(2026, 1, 5, tzinfo=UTC),
    )
    await store.upsert_trade_group(tg)
    fetched = await store.get_trade_group("GRP-1")
    assert fetched.review_status is ReviewStatus.NEEDS_REVIEW
    assert fetched.origin is Origin.BROKER

    await store.upsert_trade_group(replace(tg, review_status=ReviewStatus.CONFIRMED, realized_pnl=Decimal("42.5")))
    fetched = await store.get_trade_group("GRP-1")
    assert fetched.review_status is ReviewStatus.CONFIRMED
    assert fetched.realized_pnl == Decimal("42.5")

    group_pk = store._trade_groups.id_of("GRP-1")
    await store.add_trade_group_event(EventRow(trade_group_id=group_pk, event_type="entry", quantity_change=Decimal("1")))
    events = [row for _, row in store._events.all() if row.trade_group_id == group_pk]
    assert len(events) == 1
    assert events[0].event_type == "entry"
    assert events[0].event_at is not None  # defaulted

    assert await store.get_trade_group("does-not-exist") is None


async def test_unified_trades_filters_by_origin_and_review_status(store):
    await store.upsert_trade_group(TradeGroupRow(group_id="GRP-A", account="main", origin=Origin.BROKER, underlying="/ES"))
    await store.upsert_trade_group(
        TradeGroupRow(group_id="GRP-B", account="main", origin=Origin.ZTS, underlying="AAPL", review_status=ReviewStatus.CONFIRMED)
    )

    broker_trades = await store.unified_trades(TradeFilter(origin=Origin.BROKER))
    assert {t.group_id for t in broker_trades} == {"GRP-A"}

    confirmed = await store.unified_trades(TradeFilter(review_status=ReviewStatus.CONFIRMED))
    assert {t.group_id for t in confirmed} == {"GRP-B"}

    by_underlying = await store.unified_trades(TradeFilter(underlying="AAPL"))
    assert {t.group_id for t in by_underlying} == {"GRP-B"}


async def test_account_activity_joins_origin_and_review_status_and_unreconciled(store):
    await store.upsert_orders([OrderRow(tt_order_id="TT-5", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT)])
    await store.upsert_trade_group(TradeGroupRow(group_id="GRP-C", account="main", origin=Origin.ZTS, review_status=ReviewStatus.CONFIRMED))
    group_pk = store._trade_groups.id_of("GRP-C")

    linked_txn = TxnRow(
        tt_transaction_id="TXN-linked", tt_order_id="TT-5", account="main",
        executed_at=datetime(2026, 1, 6, tzinfo=UTC), net_value=Decimal("-10"),
    )
    await store.upsert_transactions([linked_txn])
    await store.link_transactions_to_orders("main")
    store._transactions.get_by_key("TXN-linked").trade_group_id = group_pk

    unreconciled_txn = TxnRow(
        tt_transaction_id="TXN-unreconciled", tt_order_id=None, account="main",
        executed_at=datetime(2026, 1, 6, tzinfo=UTC), net_value=Decimal("-5"),
    )
    await store.upsert_transactions([unreconciled_txn])

    activity = await store.account_activity(ActivityFilter(account="main"))
    by_id = {a.tt_transaction_id: a for a in activity}
    assert by_id["TXN-linked"].origin is Origin.ZTS
    assert by_id["TXN-linked"].review_status is ReviewStatus.CONFIRMED
    assert by_id["TXN-unreconciled"].origin is None
    assert by_id["TXN-unreconciled"].review_status is None

    unreconciled = await store.account_activity(ActivityFilter(account="main", unreconciled_only=True))
    assert {a.tt_transaction_id for a in unreconciled} == {"TXN-unreconciled"}


async def test_query_orders_date_range_filter(store):
    await store.upsert_orders(
        [
            OrderRow(tt_order_id="TT-early", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY, submitted_at=datetime(2026, 1, 1, tzinfo=UTC)),
            OrderRow(tt_order_id="TT-late", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY, submitted_at=datetime(2026, 2, 1, tzinfo=UTC)),
        ]
    )
    result = await store.query_orders(OrderFilter(account="main", start=date(2026, 1, 15), end=date(2026, 2, 15)))
    assert {r.tt_order_id for r in result} == {"TT-late"}


async def test_get_order_by_tt_order_id(store):
    await store.upsert_orders([OrderRow(tt_order_id="TT-6", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)])
    found = await store.get_order("TT-6")
    assert found is not None
    assert found.account == "main"
    assert await store.get_order("does-not-exist") is None


async def test_upsert_orders_and_legs_return_ids_in_input_order(store):
    order_ids = await store.upsert_orders(
        [
            OrderRow(tt_order_id="TT-A", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY),
            OrderRow(tt_order_id="TT-B", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY),
        ]
    )
    assert len(set(order_ids)) == 2

    # re-upsert TT-A -> same id back
    resubmitted = await store.upsert_orders(
        [OrderRow(tt_order_id="TT-A", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY, oms_status="filled")]
    )
    assert resubmitted == [order_ids[0]]

    leg_ids = await store.upsert_legs(
        [
            LegRow(order_id=order_ids[0], leg_index=0, security_id="AAPL", action="buy_to_open"),
            LegRow(order_id=order_ids[0], leg_index=1, security_id="/ESM6", action="sell_to_open"),
        ]
    )
    assert len(set(leg_ids)) == 2


async def test_upsert_trade_group_returns_id_and_is_stable_across_updates(store):
    tg = TradeGroupRow(group_id="GRP-ID", account="main", origin=Origin.BROKER)
    group_pk = await store.upsert_trade_group(tg)
    assert isinstance(group_pk, int)

    same_pk = await store.upsert_trade_group(replace(tg, review_status=ReviewStatus.CONFIRMED))
    assert same_pk == group_pk


async def test_attach_transactions_to_trade_group(store):
    await store.upsert_transactions(
        [
            TxnRow(tt_transaction_id="TXN-1", tt_order_id=None, account="main"),
            TxnRow(tt_transaction_id="TXN-2", tt_order_id=None, account="main"),
            TxnRow(tt_transaction_id="TXN-3", tt_order_id=None, account="main"),
        ]
    )
    group_pk = await store.upsert_trade_group(TradeGroupRow(group_id="GRP-1", account="main", origin=Origin.BROKER))

    attached = await store.attach_transactions_to_trade_group(["TXN-1", "TXN-2"], group_pk)
    assert attached == 2

    assert store._transactions.get_by_key("TXN-1").trade_group_id == group_pk
    assert store._transactions.get_by_key("TXN-2").trade_group_id == group_pk
    assert store._transactions.get_by_key("TXN-3").trade_group_id is None

    assert await store.attach_transactions_to_trade_group([], group_pk) == 0
    assert await store.attach_transactions_to_trade_group(["does-not-exist"], group_pk) == 0


async def test_get_security(store):
    await store.upsert_security(SecurityRow(security_id="/ESM6", product_type="F", underlying="/ES"))
    found = await store.get_security("/ESM6")
    assert found is not None
    assert found.product_type == "F"
    assert await store.get_security("does-not-exist") is None
