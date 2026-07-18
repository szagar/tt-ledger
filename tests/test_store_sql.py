"""``SqlLedgerStore`` behavior — runs against every URL in ``store_url`` (docs/implementation-notes.md
testing matrix: SQLite always, Postgres when TT_LEDGER_TEST_PG is set).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, UTC
from decimal import Decimal

import pytest
from sqlalchemy import select

from tt_ledger.enums import Ingest, Origin, ReviewStatus
from tt_ledger.rows import (
    ActivityFilter,
    ClosedPositionRow,
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
from tt_ledger.schema import metadata, models
from tt_ledger.store.sql import SqlLedgerStore


@pytest.fixture
async def store(store_url):
    s = SqlLedgerStore(store_url)
    # a persistent (e.g. shared Postgres) backend keeps rows across test functions; drop first
    # so every test starts from an empty schema regardless of backend.
    async with s._engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await s.create_all()
    yield s
    await s.dispose()


@pytest.fixture
async def seeded(store):
    """One account ("main") + two securities, inserted directly (accounts.toml sync is out of scope)."""
    async with store._sessionmaker() as session, session.begin():
        await session.execute(
            models.Account.__table__.insert().values(
                nickname="main", account_number="5WX00001", login="user1",
            )
        )
        await session.execute(
            models.Security.__table__.insert().values(security_id="/ESM6", product_type="F", underlying="/ES")
        )
        await session.execute(
            models.Security.__table__.insert().values(security_id="AAPL", product_type="S", underlying="AAPL")
        )
    return store


async def _order_surrogate_id(store: SqlLedgerStore, tt_order_id: str) -> int:
    async with store._sessionmaker() as session:
        row = (
            await session.execute(
                select(models.Order.__table__.c.id).where(models.Order.__table__.c.tt_order_id == tt_order_id)
            )
        ).first()
    return row.id


async def _group_surrogate_id(store: SqlLedgerStore, group_id: str) -> int:
    async with store._sessionmaker() as session:
        row = (
            await session.execute(
                select(models.TradeGroup.__table__.c.id).where(models.TradeGroup.__table__.c.group_id == group_id)
            )
        ).first()
    return row.id


async def test_create_all_is_idempotent(store):
    await store.create_all()  # a second call must not raise (tables already exist)


async def test_upsert_orders_is_idempotent_by_tt_order_id(seeded):
    store = seeded
    order = OrderRow(
        tt_order_id="TT-1", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY,
        security_id="/ESM6", underlying="/ES", oms_status="filled", price=Decimal("1234.5678"),
        submitted_at=datetime(2026, 1, 5, tzinfo=UTC),
    )
    await store.upsert_orders([order])
    await store.upsert_orders([replace(order, oms_status="working", price=Decimal("2.25"))])

    rows = await store.query_orders(OrderFilter(account="main"))
    assert len(rows) == 1
    assert rows[0].oms_status == "working"
    assert rows[0].price == Decimal("2.25")  # last-write-wins
    assert rows[0].origin is Origin.BROKER
    assert rows[0].ingest is Ingest.ORDER_HISTORY


async def test_upsert_orders_with_null_tt_order_id_never_conflict(seeded):
    store = seeded
    rows = [
        OrderRow(tt_order_id=None, account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT)
        for _ in range(3)
    ]
    await store.upsert_orders(rows)
    await store.upsert_orders(rows)  # still no natural key -> 6 rows total, not deduped

    result = await store.query_orders(OrderFilter(account="main"))
    assert len(result) == 6


async def test_money_round_trips_exactly(seeded):
    store = seeded
    await store.upsert_orders(
        [
            OrderRow(
                tt_order_id="TT-money", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY,
                price=Decimal("0.0001"), average_fill_price=Decimal("-19.5"),
            )
        ]
    )
    rows = await store.query_orders(OrderFilter(account="main"))
    row = next(r for r in rows if r.tt_order_id == "TT-money")
    assert row.price == Decimal("0.0001")
    assert row.average_fill_price == Decimal("-19.5")


async def test_upsert_legs_idempotent_by_order_and_index(seeded):
    store = seeded
    await store.upsert_orders([OrderRow(tt_order_id="TT-2", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)])
    order_id = await _order_surrogate_id(store, "TT-2")

    leg = LegRow(order_id=order_id, leg_index=0, security_id="/ESM6", action="buy_to_open", quantity=Decimal("1"))
    await store.upsert_legs([leg])
    await store.upsert_legs([replace(leg, action="sell_to_close", quantity=Decimal("2"))])

    async with store._sessionmaker() as session:
        legs = (
            await session.execute(
                select(models.OrderLeg.__table__).where(models.OrderLeg.__table__.c.order_id == order_id)
            )
        ).all()
    assert len(legs) == 1
    assert legs[0].action == "sell_to_close"
    assert legs[0].quantity == Decimal("2")


async def test_upsert_fills_idempotent_by_fill_id(seeded):
    store = seeded
    await store.upsert_orders([OrderRow(tt_order_id="TT-3", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)])
    order_id = await _order_surrogate_id(store, "TT-3")

    fill = FillRow(fill_id="F-1", order_id=order_id, tt_order_id="TT-3", quantity=Decimal("1"), fill_price=Decimal("100"))
    await store.upsert_fills([fill])
    await store.upsert_fills([replace(fill, fill_price=Decimal("101"))])

    async with store._sessionmaker() as session:
        fills = (
            await session.execute(
                select(models.OrderFill.__table__).where(models.OrderFill.__table__.c.fill_id == "F-1")
            )
        ).all()
    assert len(fills) == 1
    assert fills[0].fill_price == Decimal("101")


async def test_upsert_transactions_idempotent_and_linking(seeded):
    store = seeded
    await store.upsert_orders([OrderRow(tt_order_id="TT-4", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)])

    txn = TxnRow(
        tt_transaction_id="TXN-1", tt_order_id="TT-4", account="main", security_id="/ESM6",
        transaction_type="Trade", quantity=Decimal("1"), net_value=Decimal("-100.50"),
        executed_at=datetime(2026, 1, 5, tzinfo=UTC),
    )
    await store.upsert_transactions([txn])
    await store.upsert_transactions([replace(txn, net_value=Decimal("-99.99"))])

    async with store._sessionmaker() as session:
        txns = (
            await session.execute(
                select(models.Transaction.__table__).where(
                    models.Transaction.__table__.c.tt_transaction_id == "TXN-1"
                )
            )
        ).all()
    assert len(txns) == 1
    assert txns[0].net_value == Decimal("-99.99")
    assert txns[0].order_id is None  # not yet linked

    linked = await store.link_transactions_to_orders("main")
    assert linked == 1

    order_id = await _order_surrogate_id(store, "TT-4")
    async with store._sessionmaker() as session:
        row = (
            await session.execute(
                select(models.Transaction.__table__.c.order_id).where(
                    models.Transaction.__table__.c.tt_transaction_id == "TXN-1"
                )
            )
        ).first()
    assert row.order_id == order_id

    # idempotent: re-running links nothing new
    assert await store.link_transactions_to_orders("main") == 0


async def test_upsert_positions_idempotent_by_account_and_security(seeded):
    store = seeded
    pos = PositionRow(account="main", security_id="AAPL", quantity=Decimal("100"), quantity_direction="long")
    await store.upsert_positions([pos])
    await store.upsert_positions([replace(pos, quantity=Decimal("150"), mark_price=Decimal("190.25"))])

    async with store._sessionmaker() as session:
        positions = (
            await session.execute(
                select(models.Position.__table__).where(
                    models.Position.__table__.c.account == "main",
                    models.Position.__table__.c.security_id == "AAPL",
                )
            )
        ).all()
    assert len(positions) == 1
    assert positions[0].quantity == Decimal("150")
    assert positions[0].mark_price == Decimal("190.25")


async def test_upsert_security_idempotent(seeded):
    store = seeded
    await store.upsert_security(SecurityRow(security_id="/ESM6", product_type="F", underlying="/ES", tt_symbol="/ESM6"))
    await store.upsert_security(SecurityRow(security_id="/ESM6", product_type="F", underlying="/ES", tt_symbol="ES-updated"))

    async with store._sessionmaker() as session:
        secs = (
            await session.execute(
                select(models.Security.__table__).where(models.Security.__table__.c.security_id == "/ESM6")
            )
        ).all()
    assert len(secs) == 1
    assert secs[0].tt_symbol == "ES-updated"


async def test_trade_group_lifecycle_and_events(seeded):
    store = seeded
    tg = TradeGroupRow(
        group_id="GRP-1", account="main", origin=Origin.BROKER, underlying="/ES",
        strategy_type="single", executed_at=datetime(2026, 1, 5, tzinfo=UTC),
    )
    await store.upsert_trade_group(tg)
    fetched = await store.get_trade_group("GRP-1")
    assert fetched is not None
    assert fetched.review_status is ReviewStatus.NEEDS_REVIEW
    assert fetched.origin is Origin.BROKER

    await store.upsert_trade_group(replace(tg, review_status=ReviewStatus.CONFIRMED, realized_pnl=Decimal("42.5")))
    fetched = await store.get_trade_group("GRP-1")
    assert fetched.review_status is ReviewStatus.CONFIRMED
    assert fetched.realized_pnl == Decimal("42.5")

    group_pk = await _group_surrogate_id(store, "GRP-1")
    await store.add_trade_group_event(EventRow(trade_group_id=group_pk, event_type="entry", quantity_change=Decimal("1")))
    async with store._sessionmaker() as session:
        events = (
            await session.execute(
                select(models.TradeGroupEvent.__table__).where(
                    models.TradeGroupEvent.__table__.c.trade_group_id == group_pk
                )
            )
        ).all()
    assert len(events) == 1
    assert events[0].event_type == "entry"

    assert await store.get_trade_group("does-not-exist") is None


async def test_trade_group_structure_json_round_trips(seeded):
    store = seeded
    structure = {
        "legs": [
            {"action": "Sell to Open", "security_id": "option:SPXW:2026-02-03:put:6830", "strike": "6830"},
            {"action": "Buy to Open", "security_id": "option:SPXW:2026-02-03:put:6810", "strike": "6810"},
        ],
        "expiry": "2026-02-03",
        "dte": 1,
    }
    await store.upsert_trade_group(
        TradeGroupRow(group_id="GRP-S", account="main", origin=Origin.ZTS, structure=structure)
    )
    fetched = await store.get_trade_group("GRP-S")
    assert fetched.structure == structure

    # an upsert that doesn't carry structure=None-away: replace() keeps the loaded value
    await store.upsert_trade_group(replace(fetched, realized_pnl=Decimal("10")))
    fetched = await store.get_trade_group("GRP-S")
    assert fetched.structure == structure


async def test_unified_trades_filters_by_origin_and_review_status(seeded):
    store = seeded
    await store.upsert_trade_group(TradeGroupRow(group_id="GRP-A", account="main", origin=Origin.BROKER, underlying="/ES"))
    await store.upsert_trade_group(
        TradeGroupRow(
            group_id="GRP-B", account="main", origin=Origin.ZTS, underlying="AAPL",
            review_status=ReviewStatus.CONFIRMED,
        )
    )

    broker_trades = await store.unified_trades(TradeFilter(origin=Origin.BROKER))
    assert {t.group_id for t in broker_trades} == {"GRP-A"}

    confirmed = await store.unified_trades(TradeFilter(review_status=ReviewStatus.CONFIRMED))
    assert {t.group_id for t in confirmed} == {"GRP-B"}

    by_underlying = await store.unified_trades(TradeFilter(underlying="AAPL"))
    assert {t.group_id for t in by_underlying} == {"GRP-B"}


async def test_account_activity_joins_origin_and_review_status_and_unreconciled(seeded):
    store = seeded
    await store.upsert_orders([OrderRow(tt_order_id="TT-5", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT)])
    await store.upsert_trade_group(
        TradeGroupRow(group_id="GRP-C", account="main", origin=Origin.ZTS, review_status=ReviewStatus.CONFIRMED)
    )
    group_pk = await _group_surrogate_id(store, "GRP-C")

    linked_txn = TxnRow(
        tt_transaction_id="TXN-linked", tt_order_id="TT-5", account="main",
        executed_at=datetime(2026, 1, 6, tzinfo=UTC), net_value=Decimal("-10"),
    )
    await store.upsert_transactions([linked_txn])
    await store.link_transactions_to_orders("main")
    # attach to the trade group directly (trade_group_id lives on transactions per docs/schema.md)
    async with store._sessionmaker() as session, session.begin():
        await session.execute(
            models.Transaction.__table__.update()
            .where(models.Transaction.__table__.c.tt_transaction_id == "TXN-linked")
            .values(trade_group_id=group_pk)
        )

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


async def test_query_orders_date_range_filter(seeded):
    store = seeded
    await store.upsert_orders(
        [
            OrderRow(
                tt_order_id="TT-early", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY,
                submitted_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
            OrderRow(
                tt_order_id="TT-late", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY,
                submitted_at=datetime(2026, 2, 1, tzinfo=UTC),
            ),
        ]
    )
    result = await store.query_orders(OrderFilter(account="main", start=date(2026, 1, 15), end=date(2026, 2, 15)))
    assert {r.tt_order_id for r in result} == {"TT-late"}


async def test_query_orders_oms_order_id_filter(seeded):
    store = seeded
    await store.upsert_orders(
        [
            OrderRow(
                tt_order_id="TT-a", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT,
                oms_order_id="OMS-aaa",
            ),
            OrderRow(
                tt_order_id="TT-b", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT,
                oms_order_id="OMS-bbb",
            ),
            OrderRow(
                tt_order_id="TT-c", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY,
            ),
        ]
    )
    result = await store.query_orders(OrderFilter(oms_order_id="OMS-bbb"))
    assert [r.tt_order_id for r in result] == ["TT-b"]
    assert await store.query_orders(OrderFilter(oms_order_id="OMS-missing")) == []


async def test_get_order_by_tt_order_id(seeded):
    store = seeded
    await store.upsert_orders([OrderRow(tt_order_id="TT-6", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)])

    found = await store.get_order("TT-6")
    assert found is not None
    assert found.account == "main"
    assert await store.get_order("does-not-exist") is None


async def test_upsert_orders_returns_ids_in_input_order(seeded):
    """``OrderRepository`` needs each order's surrogate id (for legs/fills FKs) back from a single
    batched upsert call, in the same order as the input rows — including when a row in the middle
    of the batch is an update (ON CONFLICT) rather than a fresh insert."""
    store = seeded
    first_ids = await store.upsert_orders(
        [
            OrderRow(tt_order_id="TT-A", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY),
            OrderRow(tt_order_id="TT-B", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY),
            OrderRow(tt_order_id="TT-C", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY),
        ]
    )
    assert len(first_ids) == 3
    assert len(set(first_ids)) == 3  # distinct

    # re-upsert with TT-B (the middle row) now a conflict-update; TT-D a fresh insert
    second_ids = await store.upsert_orders(
        [
            OrderRow(tt_order_id="TT-A", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY, oms_status="working"),
            OrderRow(tt_order_id="TT-B", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY, oms_status="filled"),
            OrderRow(tt_order_id="TT-D", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY),
        ]
    )
    assert second_ids[0] == first_ids[0]  # TT-A: same id as before (updated, not re-inserted)
    assert second_ids[1] == first_ids[1]  # TT-B: same id as before
    assert second_ids[2] not in first_ids  # TT-D: brand new id


async def test_upsert_legs_returns_ids_in_input_order(seeded):
    store = seeded
    order_ids = await store.upsert_orders(
        [OrderRow(tt_order_id="TT-legs", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)]
    )
    order_id = order_ids[0]

    leg_ids = await store.upsert_legs(
        [
            LegRow(order_id=order_id, leg_index=0, security_id="/ESM6", action="buy_to_open"),
            LegRow(order_id=order_id, leg_index=1, security_id="AAPL", action="sell_to_open"),
        ]
    )
    assert len(leg_ids) == 2
    assert len(set(leg_ids)) == 2

    # re-upsert leg 0 (conflict on (order_id, leg_index)) -> same id back
    resubmitted_ids = await store.upsert_legs(
        [LegRow(order_id=order_id, leg_index=0, security_id="/ESM6", action="sell_to_close")]
    )
    assert resubmitted_ids == [leg_ids[0]]


async def test_upsert_trade_group_returns_id_and_is_stable_across_updates(seeded):
    store = seeded
    tg = TradeGroupRow(group_id="GRP-ID", account="main", origin=Origin.BROKER)
    group_pk = await store.upsert_trade_group(tg)
    assert isinstance(group_pk, int)

    same_pk = await store.upsert_trade_group(replace(tg, review_status=ReviewStatus.CONFIRMED))
    assert same_pk == group_pk


async def test_attach_transactions_to_trade_group(seeded):
    store = seeded
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

    activity = await store.account_activity(ActivityFilter(account="main"))
    by_id = {a.tt_transaction_id: a for a in activity}
    assert by_id["TXN-1"].trade_group_id == group_pk
    assert by_id["TXN-2"].trade_group_id == group_pk
    assert by_id["TXN-3"].trade_group_id is None

    assert await store.attach_transactions_to_trade_group([], group_pk) == 0
    assert await store.attach_transactions_to_trade_group(["does-not-exist"], group_pk) == 0


async def test_get_security(seeded):
    store = seeded
    found = await store.get_security("/ESM6")
    assert found is not None
    assert found.product_type == "F"
    assert await store.get_security("does-not-exist") is None


async def test_bulk_upsert_chunks_past_the_bind_parameter_cap(store):
    """Regression: a full-history backfill inserts thousands of rows in one upsert call;
    asyncpg caps a statement at 32767 bind parameters, so _upsert must chunk (found live —
    ~940 orders x 35 columns already exceeds the cap)."""
    from tt_ledger.enums import Ingest, Origin
    from tt_ledger.rows import AccountRow

    await store.upsert_account(AccountRow(nickname="main", account_number="ACCT1", login="u"))
    rows = [
        OrderRow(tt_order_id=f"O-{i}", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY)
        for i in range(2000)
    ]
    ids = await store.upsert_orders(rows)
    assert len(ids) == 2000
    assert len(set(ids)) == 2000
    # input order preserved across chunks
    first = await store.get_order("O-0")
    last = await store.get_order("O-1999")
    assert first is not None and last is not None

    # legs are the sharp edge: a small row dict (9 fields) on a table whose compiled INSERT
    # adds default-column binds (created_at/updated_at) -- chunking must size on the TABLE's
    # columns or a 3333-row chunk lands at 36k+ params.
    await store.upsert_security(SecurityRow(security_id="AAPL", product_type="S"))
    legs = [
        LegRow(order_id=ids[i % 2000], leg_index=i // 2000, security_id="AAPL", quantity=Decimal("1"))
        for i in range(4000)
    ]
    leg_ids = await store.upsert_legs(legs)
    assert len(leg_ids) == 4000


async def test_link_closed_positions_to_groups(seeded):
    """closed_positions.trade_group_id is stamped from its closing order's group
    (tt-ledger#2) — only for attributed orders, only NULLs, idempotently."""
    store = seeded
    group_pk = await store.upsert_trade_group(
        TradeGroupRow(group_id="GRP-C", account="main", origin=Origin.ZTS)
    )
    # an ATTRIBUTED closing order + the position it closed
    await store.upsert_orders([
        OrderRow(
            tt_order_id="TT-CLOSE", account="main", origin=Origin.ZTS,
            ingest=Ingest.OMS_SUBMIT, security_id="AAPL", trade_group_id=group_pk,
        )
    ])
    close_oid = await _order_surrogate_id(store, "TT-CLOSE")
    await store.upsert_closed_position(ClosedPositionRow(
        account="main", security_id="AAPL", quantity=Decimal("1"), quantity_direction="Long",
        closing_order_id=close_oid,
        opened_at=datetime(2026, 7, 1, tzinfo=UTC), closed_at=datetime(2026, 7, 2, tzinfo=UTC),
    ))
    # an UNATTRIBUTED closing order + its position (must stay NULL)
    await store.upsert_orders([
        OrderRow(
            tt_order_id="TT-ORPHAN", account="main", origin=Origin.BROKER,
            ingest=Ingest.ORDER_HISTORY, security_id="/ESM6",
        )
    ])
    orphan_oid = await _order_surrogate_id(store, "TT-ORPHAN")
    await store.upsert_closed_position(ClosedPositionRow(
        account="main", security_id="/ESM6", quantity=Decimal("1"), quantity_direction="Short",
        closing_order_id=orphan_oid,
        opened_at=datetime(2026, 7, 1, tzinfo=UTC), closed_at=datetime(2026, 7, 3, tzinfo=UTC),
    ))

    assert await store.link_closed_positions_to_groups("main") == 1

    by_sec = {cp.security_id: cp for cp in await store.get_closed_positions("main")}
    assert by_sec["AAPL"].trade_group_id == group_pk   # stamped from the closing order
    assert by_sec["/ESM6"].trade_group_id is None      # unattributed order → stays NULL

    assert await store.link_closed_positions_to_groups("main") == 0  # idempotent
