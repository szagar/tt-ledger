"""The host-UI drill-down read surface: order structure (legs + fills), paged transactions,
and the open-position -> trade_group join. Store methods run against BOTH backends (SQL via
``store_url`` + InMemoryStore); the SDK wrappers run over InMemoryStore.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from tt_ledger.enums import Ingest, Origin, ReviewStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.rows import (
    AccountRow,
    FillRow,
    LegRow,
    OrderRow,
    TradeGroupRow,
    TransactionQuery,
    TxnRow,
)
from tt_ledger.sdk import LedgerClient
from tt_ledger.store.memory import InMemoryStore
from tt_ledger.store.sql import SqlLedgerStore

T0 = datetime(2026, 7, 1, 14, 30, tzinfo=UTC)


@pytest.fixture(params=["sql", "memory"])
async def any_store(request, store_url):
    if request.param == "memory":
        yield InMemoryStore()
        return
    from tt_ledger.schema import metadata

    s = SqlLedgerStore(store_url)
    async with s._engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await s.create_all()
    yield s
    await s.dispose()


async def _seed_group_with_structure(store) -> tuple[int, list[int]]:
    """One open group: 2 orders (spread entry + partial close), legs on each, fills on the first."""
    await store.upsert_account(AccountRow(nickname="main", account_number="ACCT1", login="user1"))
    group_pk = await store.upsert_trade_group(
        TradeGroupRow(group_id="g-1", account="main", origin=Origin.ZTS,
                      review_status=ReviewStatus.CONFIRMED, status="open")
    )
    order_ids = await store.upsert_orders([
        OrderRow(tt_order_id="O-1", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT,
                 signal_id="sig-1", trade_group_id=group_pk,
                 received_at=T0, filled_at=T0),
        OrderRow(tt_order_id="O-2", account="main", origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY,
                 trade_group_id=group_pk, received_at=datetime(2026, 7, 2, 15, 0, tzinfo=UTC)),
    ])
    leg_ids = await store.upsert_legs([
        LegRow(order_id=order_ids[0], leg_index=0, security_id="option:SPXW:2026-07-03:put:6200",
               action="Sell to Open", quantity=Decimal("1"), fill_price=Decimal("1.20")),
        LegRow(order_id=order_ids[0], leg_index=1, security_id="option:SPXW:2026-07-03:put:6180",
               action="Buy to Open", quantity=Decimal("1"), fill_price=Decimal("0.55")),
        LegRow(order_id=order_ids[1], leg_index=0, security_id="option:SPXW:2026-07-03:put:6200",
               action="Buy to Close", quantity=Decimal("1")),
    ])
    await store.upsert_fills([
        FillRow(fill_id="F-1", order_id=order_ids[0], order_leg_id=leg_ids[0], tt_order_id="O-1",
                quantity=Decimal("1"), fill_price=Decimal("1.20"), filled_at=T0),
        FillRow(fill_id="F-2", order_id=order_ids[0], order_leg_id=leg_ids[1], tt_order_id="O-1",
                quantity=Decimal("1"), fill_price=Decimal("0.55"), filled_at=T0),
    ])
    return group_pk, order_ids


# --- order structure ------------------------------------------------------------------


async def test_group_orders_legs_and_fills_round_trip(any_store):
    group_pk, order_ids = await _seed_group_with_structure(any_store)

    orders = await any_store.get_group_orders_with_ids(group_pk)
    assert [pk for pk, _ in orders] == order_ids  # received_at ascending
    assert orders[0][1].signal_id == "sig-1"

    legs = await any_store.get_legs_for_orders(order_ids)
    assert [(leg.order_id, leg.leg_index) for leg in legs] == [
        (order_ids[0], 0), (order_ids[0], 1), (order_ids[1], 0),
    ]
    assert legs[0].id > 0  # surrogate id exposed for the fill join

    fills = await any_store.get_fills_for_orders(order_ids)
    assert {f.fill_id for f in fills} == {"F-1", "F-2"}
    assert {f.order_leg_id for f in fills} == {legs[0].id, legs[1].id}


async def test_get_legs_for_orders_empty_input(any_store):
    assert await any_store.get_legs_for_orders([]) == []
    assert await any_store.get_fills_for_orders([]) == []


# --- paged transactions ----------------------------------------------------------------


async def _seed_transactions(store) -> int:
    await store.upsert_account(AccountRow(nickname="main", account_number="ACCT1", login="user1"))
    await store.upsert_account(AccountRow(nickname="other", account_number="ACCT2", login="user1"))
    group_pk = await store.upsert_trade_group(
        TradeGroupRow(group_id="g-1", account="main", origin=Origin.ZTS,
                      review_status=ReviewStatus.CONFIRMED, status="open")
    )
    order_ids = await store.upsert_orders([
        OrderRow(tt_order_id="O-1", account="main", origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT,
                 signal_id="sig-1", trade_group_id=group_pk),
    ])
    await store.upsert_transactions([
        TxnRow(tt_transaction_id="T-1", tt_order_id="O-1", account="main",
               transaction_type="Trade", action="Sell to Open",
               security_id="option:SPXW:2026-07-03:put:6200", underlying="SPXW",
               quantity=Decimal("1"), price=Decimal("1.20"),
               net_value=Decimal("119.34"), net_value_effect="Credit",
               order_id=order_ids[0], trade_group_id=group_pk,
               executed_at=datetime(2026, 7, 1, 14, 30, tzinfo=UTC)),
        TxnRow(tt_transaction_id="T-2", tt_order_id=None, account="main",
               transaction_type="Money Movement", description="INTEREST",
               net_value=Decimal("4.10"), net_value_effect="Credit",
               executed_at=datetime(2026, 7, 2, 20, 0, tzinfo=UTC)),
        TxnRow(tt_transaction_id="T-3", tt_order_id=None, account="other",
               transaction_type="Trade", action="Buy to Open",
               security_id="equity:AAPL", underlying="AAPL", quantity=Decimal("10"),
               executed_at=datetime(2026, 7, 3, 15, 0, tzinfo=UTC)),
    ])
    return group_pk


async def test_query_transactions_pages_newest_first_with_joined_signal(any_store):
    group_pk = await _seed_transactions(any_store)

    rows, total = await any_store.query_transactions(TransactionQuery())
    assert total == 3
    assert [r.tt_transaction_id for r in rows] == ["T-3", "T-2", "T-1"]

    trade = rows[2]
    assert trade.id > 0
    assert trade.signal_id == "sig-1"  # joined from the order
    assert trade.trade_group_id == group_pk
    assert rows[1].signal_id is None  # money movement: no order join

    page, total = await any_store.query_transactions(TransactionQuery(limit=1, offset=1))
    assert total == 3
    assert [r.tt_transaction_id for r in page] == ["T-2"]


async def test_query_transactions_filters(any_store):
    await _seed_transactions(any_store)

    rows, total = await any_store.query_transactions(TransactionQuery(account="main"))
    assert total == 2 and {r.account for r in rows} == {"main"}

    rows, total = await any_store.query_transactions(TransactionQuery(accounts=["other"]))
    assert total == 1 and rows[0].tt_transaction_id == "T-3"

    rows, total = await any_store.query_transactions(TransactionQuery(transaction_type="Trade"))
    assert total == 2

    rows, total = await any_store.query_transactions(
        TransactionQuery(start=date(2026, 7, 2), end=date(2026, 7, 2))
    )
    assert [r.tt_transaction_id for r in rows] == ["T-2"]


# --- open position groups ----------------------------------------------------------------


async def test_open_position_groups_only_covers_open_groups(any_store):
    group_pk = await _seed_transactions(any_store)
    closed_pk = await any_store.upsert_trade_group(
        TradeGroupRow(group_id="g-2", account="main", origin=Origin.BROKER,
                      review_status=ReviewStatus.NEEDS_REVIEW, status="closed")
    )
    await any_store.upsert_transactions([
        TxnRow(tt_transaction_id="T-9", tt_order_id=None, account="main",
               transaction_type="Trade", security_id="equity:TSLA",
               trade_group_id=closed_pk,
               executed_at=datetime(2026, 6, 1, 15, 0, tzinfo=UTC)),
    ])

    rows = await any_store.get_open_position_groups()
    assert rows == [("main", "option:SPXW:2026-07-03:put:6200", group_pk)]


# --- net open by group -------------------------------------------------------------------


async def _seed_net_open(store) -> tuple[int, int]:
    """Two groups that REUSE strike put:6200 (the shared-strike case). Group A holds
    its long put open + a settled leg; group B is fully closed."""
    await store.upsert_account(AccountRow(nickname="main", account_number="ACCT1", login="user1"))
    pk_a = await store.upsert_trade_group(
        TradeGroupRow(group_id="g-a", account="main", origin=Origin.ZTS,
                      review_status=ReviewStatus.CONFIRMED, status="open")
    )
    pk_b = await store.upsert_trade_group(
        TradeGroupRow(group_id="g-b", account="main", origin=Origin.ZTS,
                      review_status=ReviewStatus.CONFIRMED, status="closed")
    )

    def _txn(tid, group_pk, action, sid, qty, *, at):
        return TxnRow(tt_transaction_id=tid, tt_order_id=None, account="main",
                      transaction_type="Trade", action=action, security_id=sid,
                      quantity=Decimal(qty), trade_group_id=group_pk,
                      executed_at=datetime(2026, 7, 1, at, 30, tzinfo=UTC))

    await store.upsert_transactions([
        # Group A: short 6200 opened then bought back (net 0); long 6180 still open;
        # 6100 opened then cash-SETTLED (settlement action must NOT net it out).
        _txn("A-1", pk_a, "Sell to Open", "option:SPXW:2026-07-03:put:6200", "2", at=14),
        _txn("A-2", pk_a, "Buy to Open", "option:SPXW:2026-07-03:put:6180", "2", at=14),
        _txn("A-3", pk_a, "Buy to Close", "option:SPXW:2026-07-03:put:6200", "2", at=15),
        _txn("A-4", pk_a, "Sell to Open", "option:SPXW:2026-07-03:put:6100", "1", at=14),
        _txn("A-5", pk_a, "Receive Deliver", "option:SPXW:2026-07-03:put:6100", "1", at=16),
        # Group B: SAME 6200 strike + 6180, both fully closed → all net 0.
        _txn("B-1", pk_b, "Sell to Open", "option:SPXW:2026-07-03:put:6200", "1", at=14),
        _txn("B-2", pk_b, "Buy to Open", "option:SPXW:2026-07-03:put:6180", "1", at=14),
        _txn("B-3", pk_b, "Buy to Close", "option:SPXW:2026-07-03:put:6200", "1", at=15),
        _txn("B-4", pk_b, "Sell to Close", "option:SPXW:2026-07-03:put:6180", "1", at=15),
    ])
    return pk_a, pk_b


async def test_net_open_by_group_scopes_shared_strike_per_group(any_store):
    pk_a, pk_b = await _seed_net_open(any_store)

    net = await any_store.net_open_by_group([pk_a, pk_b])

    # The reused 6200 strike nets independently per group — never collapsed.
    assert net[pk_a]["option:SPXW:2026-07-03:put:6200"] == 0
    assert net[pk_a]["option:SPXW:2026-07-03:put:6180"] == 2  # long still open
    assert net[pk_a]["option:SPXW:2026-07-03:put:6100"] == 1  # settled, NOT netted by the settlement
    # Group B is fully closed: every leg nets to 0 (present, so "closed", not "unknown").
    assert net[pk_b] == {
        "option:SPXW:2026-07-03:put:6200": 0,
        "option:SPXW:2026-07-03:put:6180": 0,
    }


async def test_net_open_by_group_empty_and_unknown(any_store):
    pk_a, _ = await _seed_net_open(any_store)
    assert await any_store.net_open_by_group([]) == {}
    # Unknown pk is simply absent (caller's .get(pk) is None → "not confirmed closed").
    result = await any_store.net_open_by_group([pk_a, 999_999])
    assert set(result) == {pk_a}


async def test_sdk_net_open_by_group(client):
    pk_a, pk_b = await _seed_net_open(client._store)
    net = await client.net_open_by_group([pk_a, pk_b])
    assert net[pk_a]["option:SPXW:2026-07-03:put:6180"] == 2
    assert net[pk_b]["option:SPXW:2026-07-03:put:6200"] == 0


# --- SDK wrappers -------------------------------------------------------------------------


@pytest.fixture
def client() -> LedgerClient:
    return LedgerClient(
        InMemoryStore(), accounts=AccountMapper({"main": "ACCT1", "other": "ACCT2"}),
        resolver=PassthroughResolver(),
    )


async def test_sdk_trade_structure(client):
    group_pk, order_ids = await _seed_group_with_structure(client._store)

    details = await client.trade_structure("g-1")
    assert [d.order_pk for d in details] == order_ids
    assert [len(d.legs) for d in details] == [2, 1]
    assert [len(d.fills) for d in details] == [2, 0]
    assert details[0].fills[0].order_leg_id == details[0].legs[0].id

    assert await client.trade_structure("nope") == []


async def test_sdk_transactions_and_open_position_groups(client):
    group_pk = await _seed_transactions(client._store)

    rows, total = await client.transactions(account="main", limit=10)
    assert total == 2
    assert rows[0].tt_transaction_id == "T-2"

    mapping = await client.open_position_groups()
    assert mapping == {("main", "option:SPXW:2026-07-03:put:6200"): group_pk}
