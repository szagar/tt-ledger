"""Balance snapshots — parsing, throttled stream persistence, REST sync, SDK reads.

The ``balance_snapshots`` table is the account balance TIME SERIES (NLV history for
sizing/equity-curve analysis); the latest row per account is the live view.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BalanceMessage
from tt_ledger.ingest.mock_broker import MockMessageSource, MockTastyTradeClient
from tt_ledger.ingest.pull import sync_all, sync_balances
from tt_ledger.ingest.push import StreamConsumer
from tt_ledger.ingest.tastytrade_client import balance_from_json
from tt_ledger.rows import BalanceSnapshotRow
from tt_ledger.store.memory import InMemoryStore
from tt_ledger.store.sql import SqlLedgerStore

T0 = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


def _msg(nlv: str, *, captured_at: datetime, account_number: str = "ACCT1") -> BalanceMessage:
    return BalanceMessage(
        account_number=account_number, raw={"net-liquidating-value": nlv},
        net_liquidating_value=Decimal(nlv), cash_balance=Decimal("10000"),
        derivative_buying_power=Decimal("8000"), captured_at=captured_at,
    )


# --------------------------------------------------------------------- parsing


def test_balance_from_json_parses_the_dasherized_broker_shape():
    item = {
        "account-number": "ACCT1",
        "net-liquidating-value": "50123.45",
        "cash-balance": "12000.5",
        "equity-buying-power": "24001.0",
        "derivative-buying-power": "12000.5",
        "maintenance-requirement": "3000",
        "pending-cash": "0.0",
        "day-trading-buying-power": "48002.0",
        "updated-at": "2026-01-05T15:00:00+00:00",
    }
    msg = balance_from_json(item)
    assert msg.account_number == "ACCT1"
    assert msg.net_liquidating_value == Decimal("50123.45")
    assert msg.cash_balance == Decimal("12000.5")
    assert msg.equity_buying_power == Decimal("24001.0")
    assert msg.derivative_buying_power == Decimal("12000.5")
    assert msg.maintenance_requirement == Decimal("3000")
    assert msg.pending_cash == Decimal("0.0")
    assert msg.day_trading_buying_power == Decimal("48002.0")
    assert msg.captured_at == T0
    assert msg.raw is item


# --------------------------------------------------------------------- stream persistence


async def _run_consumer(store, accounts, messages, **kw):
    source = MockMessageSource()
    for m in messages:
        source.push(m)
    consumer = StreamConsumer(store, source, accounts=accounts, resolver=PassthroughResolver(), **kw)
    await consumer.run()
    return consumer


async def test_stream_persists_balance_snapshots(accounts):
    store = InMemoryStore()
    await _run_consumer(store, accounts, [_msg("50000", captured_at=T0)])
    latest = await store.get_latest_balance("main")
    assert latest is not None
    assert latest.source == "stream"
    assert latest.net_liquidating_value == Decimal("50000")
    assert latest.captured_at == T0


async def test_stream_throttles_but_never_drops_nlv_changes(accounts):
    store = InMemoryStore()
    await _run_consumer(
        store, accounts,
        [
            _msg("50000", captured_at=T0),                            # first: persist
            _msg("50000", captured_at=T0 + timedelta(seconds=10)),    # inside window, same NLV: drop
            _msg("50250", captured_at=T0 + timedelta(seconds=20)),    # inside window, NLV changed: persist
            _msg("50250", captured_at=T0 + timedelta(seconds=90)),    # window elapsed: persist
        ],
        balance_min_interval_seconds=60.0,
    )
    series = await store.get_balances("main")
    assert [(r.captured_at, r.net_liquidating_value) for r in series] == [
        (T0, Decimal("50000")),
        (T0 + timedelta(seconds=20), Decimal("50250")),
        (T0 + timedelta(seconds=90), Decimal("50250")),
    ]


async def test_stream_persistence_can_be_disabled_and_hook_still_fires(accounts):
    store = InMemoryStore()
    seen: list[BalanceMessage] = []
    await _run_consumer(
        store, accounts, [_msg("50000", captured_at=T0)],
        persist_balances=False, on_balance=seen.append,
    )
    assert await store.get_latest_balance("main") is None
    assert len(seen) == 1


# --------------------------------------------------------------------- REST sync


async def test_sync_balances_records_a_rest_sync_snapshot(accounts):
    store = InMemoryStore()
    client = MockTastyTradeClient()
    client.set_balance("ACCT1", _msg("61000", captured_at=T0))

    count = await sync_balances(store, "main", client=client, accounts=accounts)

    assert count == 1
    latest = await store.get_latest_balance("main")
    assert latest.source == "rest_sync"
    assert latest.net_liquidating_value == Decimal("61000")


async def test_sync_all_includes_a_balance_snapshot(accounts):
    store = InMemoryStore()
    client = MockTastyTradeClient()
    client.set_balance("ACCT1", _msg("61000", captured_at=T0))

    result = await sync_all(
        store, "main", client=client, accounts=accounts, resolver=PassthroughResolver(),
    )

    assert result.balances == 1
    assert not [e for e in result.errors if "balance" in e]


# --------------------------------------------------------------------- store round-trip (dual-dialect)


async def test_sql_store_balance_roundtrip(store_url):
    store = SqlLedgerStore(store_url)
    from tt_ledger.schema import metadata

    async with store._engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await store.create_all()
    try:
        async with store._sessionmaker() as session, session.begin():
            from tt_ledger.schema import models

            await session.execute(
                models.Account.__table__.insert().values(nickname="main", account_number="ACCT1", login="u")
            )

        for i, nlv in enumerate(["50000", "50100", "50200"]):
            await store.upsert_balance_snapshot(
                BalanceSnapshotRow(
                    account="main", captured_at=T0 + timedelta(days=i), source="stream",
                    net_liquidating_value=Decimal(nlv), raw={"net-liquidating-value": nlv},
                )
            )
        # idempotent on (account, captured_at, source)
        await store.upsert_balance_snapshot(
            BalanceSnapshotRow(
                account="main", captured_at=T0, source="stream",
                net_liquidating_value=Decimal("50001"),
            )
        )

        latest = await store.get_latest_balance("main")
        assert latest.net_liquidating_value == Decimal("50200")

        series = await store.get_balances("main")
        assert [r.net_liquidating_value for r in series] == [Decimal("50001"), Decimal("50100"), Decimal("50200")]

        windowed = await store.get_balances("main", start=date(2026, 1, 6), end=date(2026, 1, 6))
        assert [r.net_liquidating_value for r in windowed] == [Decimal("50100")]
    finally:
        await store.dispose()


async def test_writes_seed_the_accounts_dimension_automatically(store_url, accounts):
    """Regression: fact tables FK account -> accounts.nickname, and nothing else populates
    the dimension — the SDK/StreamConsumer must seed it before their first write. On Postgres
    this is a REAL FK; the original bug only appeared against a live backfill."""
    from tt_ledger.sdk import LedgerClient
    from tt_ledger.schema import metadata

    store = SqlLedgerStore(store_url)
    async with store._engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await store.create_all()
    try:
        client = MockTastyTradeClient()
        client.set_balance("ACCT1", _msg("61000", captured_at=T0))
        ledger = LedgerClient(store, accounts=accounts, client=client)

        # NO manual accounts seed -- sync must create the dimension row itself.
        result = await ledger.sync("main")

        assert not result.errors
        latest = await store.get_latest_balance("main")
        assert latest is not None and latest.net_liquidating_value == Decimal("61000")

        # intent + import paths seed too (fresh client, fresh cache)
        ledger2 = LedgerClient(store, accounts=AccountMapper({"second": "ACCT9"}))
        trade = await ledger2.open_trade_group("second", strategy_type="single")
        assert trade.group_id
    finally:
        await store.dispose()
