"""``remap_trade_group`` / ``regroup_transactions`` / ``dismiss_trade_group`` (docs/ingestion.md
-> Remap, the operator-driven corrections layer)."""

from __future__ import annotations

from datetime import datetime, timedelta, UTC
from decimal import Decimal

import pytest

from tt_ledger.enums import ReviewStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BrokerPosition
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_positions, sync_transactions
from tt_ledger.ingest.reconcile import reconcile
from tt_ledger.ingest.remap import dismiss_trade_group, regroup_transactions, remap_trade_group
from tt_ledger.rows import TradeFilter
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


async def _sync(store, account, client, accounts, resolver):
    await sync_orders(store, account, client=client, accounts=accounts, resolver=resolver)
    await sync_transactions(store, account, client=client, accounts=accounts, resolver=resolver)


def _single_leg_client(order_id: str, symbol: str, executed_at: datetime, quantity=Decimal("10"), price=Decimal("150")) -> MockTastyTradeClient:
    client = MockTastyTradeClient()
    client.fill(
        account_number="ACCT1", order_id=order_id, symbol=symbol, instrument_type="Equity",
        action="Buy to Open", quantity=quantity, fill_price=price, filled_at=executed_at, status="Filled",
    )
    return client


async def _reconciled_group(store, accounts, resolver, order_id="O-1", symbol="AAPL", executed_at=None):
    executed_at = executed_at or datetime(2026, 1, 5, tzinfo=UTC)
    client = _single_leg_client(order_id, symbol, executed_at)
    await _sync(store, "main", client, accounts, resolver)
    await reconcile(store, "main")
    return (await store.unified_trades(TradeFilter(account="main")))[-1]


# --- remap_trade_group -----------------------------------------------------------------------


async def test_remap_sets_attribution_and_flips_status(store, accounts, resolver):
    trade = await _reconciled_group(store, accounts, resolver)

    result = await remap_trade_group(
        store, trade.group_id, strategy=42, bot="my-bot", signal="SIG-1",
        strategy_type="single", reviewed_by="alice",
    )

    assert result.strategy_id == 42
    assert result.bot_name == "my-bot"
    assert result.signal_id == "SIG-1"
    assert result.strategy_type == "single"
    assert result.manually_attributed is True
    assert result.review_status is ReviewStatus.CONFIRMED

    persisted = await store.get_trade_group(trade.group_id)
    assert persisted.strategy_id == 42
    assert persisted.reviewed_by == "alice"
    assert persisted.reviewed_at is not None

    events = [ev for _, ev in store._events.all() if ev.trade_group_id == store._trade_groups.id_of(trade.group_id)]
    assert any(ev.event_type == "adjustment" for ev in events)


async def test_remap_cascades_strategy_id_to_order_and_position(store, accounts, resolver):
    executed_at = datetime(2026, 1, 5, tzinfo=UTC)
    client = _single_leg_client("O-1", "AAPL", executed_at)
    await _sync(store, "main", client, accounts, resolver)
    await sync_positions(
        store, "main", client=_positions_client("AAPL"), accounts=accounts, resolver=resolver,
    )
    await reconcile(store, "main")
    trade = (await store.unified_trades(TradeFilter(account="main")))[0]

    await remap_trade_group(store, trade.group_id, strategy=42, reviewed_by="alice")

    order = await store.get_order("O-1")
    assert order.strategy_id == 42

    position = await store.get_position("main", "AAPL")
    assert position.strategy_id == 42
    assert position.trade_group_id is not None


def _positions_client(symbol: str) -> MockTastyTradeClient:
    client = MockTastyTradeClient()
    client.set_positions(
        "ACCT1",
        [BrokerPosition(account_number="ACCT1", symbol=symbol, quantity=Decimal("10"), quantity_direction="Long")],
    )
    return client


async def test_remap_preserves_unspecified_fields_on_a_second_call(store, accounts, resolver):
    trade = await _reconciled_group(store, accounts, resolver)

    await remap_trade_group(store, trade.group_id, strategy=42, bot="bot-a", reviewed_by="alice")
    result = await remap_trade_group(store, trade.group_id, bot="bot-b", reviewed_by="bob")

    assert result.strategy_id == 42  # untouched by the second call
    assert result.bot_name == "bot-b"


async def test_remap_raises_for_unknown_group(store):
    with pytest.raises(ValueError, match="not found"):
        await remap_trade_group(store, "does-not-exist", strategy=1, reviewed_by="alice")


# --- dismiss_trade_group ----------------------------------------------------------------------


async def test_dismiss_sets_ignored_without_attribution(store, accounts, resolver):
    trade = await _reconciled_group(store, accounts, resolver)

    result = await dismiss_trade_group(store, trade.group_id, reviewed_by="alice")

    assert result.review_status is ReviewStatus.IGNORED
    assert result.manually_attributed is False  # dismissing isn't attributing

    persisted = await store.get_trade_group(trade.group_id)
    assert persisted.review_status is ReviewStatus.IGNORED
    assert persisted.reviewed_by == "alice"


async def test_dismiss_raises_for_unknown_group(store):
    with pytest.raises(ValueError, match="not found"):
        await dismiss_trade_group(store, "does-not-exist", reviewed_by="alice")


async def test_dismissed_group_is_never_re_grouped_by_reconcile(store, accounts, resolver):
    trade = await _reconciled_group(store, accounts, resolver)
    await dismiss_trade_group(store, trade.group_id, reviewed_by="alice")

    result = await reconcile(store, "main")
    assert result.trade_groups == 0
    persisted = await store.get_trade_group(trade.group_id)
    assert persisted.review_status is ReviewStatus.IGNORED  # still ignored, not re-touched


# --- regroup_transactions ---------------------------------------------------------------------


async def test_regroup_splits_a_transaction_into_a_new_group(store, accounts, resolver):
    t0 = datetime(2026, 1, 5, 10, 0, 0, tzinfo=UTC)
    client = MockTastyTradeClient()
    client.fill(account_number="ACCT1", order_id="O-A", symbol="AAPL", instrument_type="Equity", action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=t0, status="Filled")
    client.fill(account_number="ACCT1", order_id="O-B", symbol="MSFT", instrument_type="Equity", action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("300"), filled_at=t0 + timedelta(seconds=1), status="Filled")
    await _sync(store, "main", client, accounts, resolver)
    await reconcile(store, "main")

    trades = await store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 1
    original = trades[0]
    assert original.leg_count == 2

    txn = next(row for _, row in store._transactions.all() if row.tt_order_id == "O-B")
    txn_id = store._transactions.id_of(txn.tt_transaction_id)

    updated = await regroup_transactions(store, [txn_id], target_group_id=None, reviewed_by="alice")
    assert len(updated) == 2  # the original (now-smaller) group + the new split-off group

    trades_after = await store.unified_trades(TradeFilter(account="main"))
    assert len(trades_after) == 2
    by_group = {t.group_id: t for t in trades_after}
    original_after = by_group[original.group_id]
    assert original_after.leg_count == 1  # only AAPL left

    new_group = next(t for t in trades_after if t.group_id != original.group_id)
    assert new_group.manually_attributed is True
    assert new_group.leg_count == 1


async def test_regroup_merges_transactions_into_an_existing_group(store, accounts, resolver):
    client_a = _single_leg_client("O-A", "AAPL", datetime(2026, 1, 5, 10, 0, tzinfo=UTC))
    await _sync(store, "main", client_a, accounts, resolver)
    client_b = _single_leg_client("O-B", "MSFT", datetime(2026, 1, 5, 15, 0, tzinfo=UTC))  # far apart -> separate group
    await _sync(store, "main", client_b, accounts, resolver)
    await reconcile(store, "main")

    trades = await store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 2
    group_a, group_b = trades[0], trades[1]

    txn_b = next(row for _, row in store._transactions.all() if row.tt_order_id == "O-B")
    txn_b_id = store._transactions.id_of(txn_b.tt_transaction_id)

    updated = await regroup_transactions(store, [txn_b_id], target_group_id=group_a.group_id, reviewed_by="alice")
    assert {t.group_id for t in updated} == {group_a.group_id, group_b.group_id}

    trades_after = await store.unified_trades(TradeFilter(account="main"))
    by_group = {t.group_id: t for t in trades_after}
    assert by_group[group_a.group_id].leg_count == 2  # merged
    assert by_group[group_b.group_id].leg_count == 1  # empty cluster falls back to the min (1)
    assert by_group[group_b.group_id].total_premium == Decimal("0")
    assert by_group[group_a.group_id].manually_attributed is True


async def test_regroup_raises_for_unknown_target_group(store, accounts, resolver):
    await _reconciled_group(store, accounts, resolver)
    txn = next(row for _, row in store._transactions.all())
    txn_id = store._transactions.id_of(txn.tt_transaction_id)

    with pytest.raises(ValueError, match="not found"):
        await regroup_transactions(store, [txn_id], target_group_id="does-not-exist", reviewed_by="alice")


async def test_regroup_with_empty_txn_ids_is_a_noop(store):
    assert await regroup_transactions(store, [], target_group_id=None, reviewed_by="alice") == []


async def test_regroup_writes_adjustment_events(store, accounts, resolver):
    await _reconciled_group(store, accounts, resolver)
    txn = next(row for _, row in store._transactions.all())
    txn_id = store._transactions.id_of(txn.tt_transaction_id)

    await regroup_transactions(store, [txn_id], target_group_id=None, reviewed_by="alice")

    events = [ev for _, ev in store._events.all() if ev.event_type == "adjustment"]
    assert len(events) == 2  # source group + new group


# --- confirmatory tests against the real SQL store --------------------------------------------


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


async def test_remap_and_dismiss_against_sql_store(sql_store, accounts, resolver):
    trade = await _reconciled_group(sql_store, accounts, resolver)

    remapped = await remap_trade_group(sql_store, trade.group_id, strategy=7, bot="bot-x", reviewed_by="alice")
    assert remapped.strategy_id == 7
    assert remapped.manually_attributed is True

    dismissed_trade = await _reconciled_group(sql_store, accounts, resolver, order_id="O-2", symbol="MSFT", executed_at=datetime(2026, 2, 1, tzinfo=UTC))
    dismissed = await dismiss_trade_group(sql_store, dismissed_trade.group_id, reviewed_by="alice")
    assert dismissed.review_status is ReviewStatus.IGNORED


async def test_regroup_against_sql_store(sql_store, accounts, resolver):
    client_a = _single_leg_client("O-A", "AAPL", datetime(2026, 1, 5, 10, 0, tzinfo=UTC))
    await _sync(sql_store, "main", client_a, accounts, resolver)
    client_b = _single_leg_client("O-B", "MSFT", datetime(2026, 1, 5, 10, 0, 1, tzinfo=UTC))
    await _sync(sql_store, "main", client_b, accounts, resolver)
    await reconcile(sql_store, "main")

    trades = await sql_store.unified_trades(TradeFilter(account="main"))
    assert len(trades) == 1
    assert trades[0].leg_count == 2

    async with sql_store._sessionmaker() as session:
        from sqlalchemy import select

        row = (
            await session.execute(
                select(models.Transaction.__table__.c.id).where(
                    models.Transaction.__table__.c.tt_order_id == "O-B"
                )
            )
        ).first()
    txn_id = row.id

    updated = await regroup_transactions(sql_store, [txn_id], target_group_id=None, reviewed_by="alice")
    assert len(updated) == 2

    trades_after = await sql_store.unified_trades(TradeFilter(account="main"))
    assert len(trades_after) == 2
