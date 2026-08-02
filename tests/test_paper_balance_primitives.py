"""Paper-balance primitives: standalone ``Money Movement`` imports (seed/reset
adjustments injected by a host platform) and ``transaction_value_total`` — the
cash fold those adjustments feed. See zts-massive
``docs/plans/paper-account-balances.md``.

The load-bearing pins:
- a Money Movement row with no order_id / no symbol imports cleanly, is
  idempotent, and NEVER enters the reconcile candidate path (no trade group);
- it does not disturb an existing open group's membership or lifecycle;
- ``transaction_value_total`` folds trades + money movements, signed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BrokerTransaction
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import reconcile
from tt_ledger.rows import OrderInput, TradeFilter, TransactionQuery
from tt_ledger.sdk import LedgerClient
from tt_ledger.store.memory import InMemoryStore

T0 = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)
PUT_A = "SPY   260116P00580000"


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


@pytest.fixture
def client(store, accounts) -> LedgerClient:
    return LedgerClient(store, accounts=accounts, resolver=PassthroughResolver())


def _adjustment(amount: str, *, ts: datetime = T0, suffix: str = "seed") -> BrokerTransaction:
    return BrokerTransaction(
        id=f"paper-adjust-main-{suffix}",
        account_number="ACCT1",
        transaction_type="Money Movement",
        transaction_sub_type="Balance Adjustment",
        value=Decimal(amount),
        net_value=Decimal(amount),
        executed_at=ts,
        transaction_date=ts.date(),
        description=f"Paper balance {suffix}",
    )


async def test_standalone_money_movement_imports_without_grouping(client, store):
    result = await client.import_transactions("main", [_adjustment("100000")], source_system="paper")
    again = await client.import_transactions("main", [_adjustment("100000")], source_system="paper")

    assert result.transactions == 1
    assert result.errors == []
    assert result.trade_groups == 0
    assert again.trade_groups == 0
    # No trade group exists at all — the row never entered the candidate path.
    assert await store.unified_trades(TradeFilter(account="main")) == []
    # Idempotent on the deterministic id: one row, not two.
    _, total = await store.query_transactions(TransactionQuery(account="main"))
    assert total == 1


async def test_money_movement_leaves_open_groups_untouched(client, store, accounts):
    # An open paper position recorded through the intent path.
    trade = await client.open_trade_group("main", strategy_type="single", underlying="SPY")
    await client.record_order(OrderInput(account="main", tt_order_id="O-1", trade_group=trade.group_id))
    broker = MockTastyTradeClient()
    broker.fill(
        account_number="ACCT1", order_id="O-1", symbol=PUT_A, instrument_type="Equity Option",
        action="Sell to Open", quantity=Decimal("1"), fill_price=Decimal("2.5"),
        filled_at=T0, underlying_symbol="SPY",
    )
    resolver = PassthroughResolver()
    await sync_orders(store, "main", client=broker, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=broker, accounts=accounts, resolver=resolver)
    await reconcile(store, "main")

    before = await store.unified_trades(TradeFilter(account="main"))
    assert len(before) == 1 and before[0].status == "open"

    await client.import_transactions(
        "main", [_adjustment("-1500", ts=T0 + timedelta(hours=1), suffix="reset-1")],
        source_system="paper",
    )

    after = await store.unified_trades(TradeFilter(account="main"))
    assert len(after) == 1
    assert after[0].status == "open"
    assert after[0].group_id == before[0].group_id
    # The adjustment row itself stays ungrouped.
    pk = await store.get_trade_group_id(after[0].group_id)
    member_ids = {t.tt_transaction_id for t in await store.get_group_transactions(pk)}
    assert "paper-adjust-main-reset-1" not in member_ids


async def test_transaction_value_total_folds_trades_and_adjustments(client):
    assert await client.transaction_value_total("main") == Decimal("0")

    fill = BrokerTransaction(
        id="paper-PAPER-1-0", account_number="ACCT1", order_id="PAPER-1",
        symbol=PUT_A, instrument_type="Equity Option", underlying_symbol="SPY",
        transaction_type="Trade", action="Sell to Open",
        quantity=Decimal("1"), price=Decimal("2.5"),
        value=Decimal("250"), net_value=Decimal("250"),
        executed_at=T0 + timedelta(minutes=5), transaction_date=T0.date(),
    )
    await client.import_transactions("main", [_adjustment("100000")], source_system="paper")
    await client.import_transactions("main", [fill], source_system="paper")

    assert await client.transaction_value_total("main") == Decimal("100250")
    # Signed: a debit adjustment reduces the fold.
    await client.import_transactions(
        "main", [_adjustment("-50000", ts=T0 + timedelta(days=1), suffix="reset-2")],
        source_system="paper",
    )
    assert await client.transaction_value_total("main") == Decimal("50250")
