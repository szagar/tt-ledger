"""``find_orphan_settlement_groups``: a settlement that arrived after its group was already
closed mints a phantom OPEN group (``_route_cluster`` only ever considers open groups, so the
row falls into ``rest`` and ``_create_trade_group`` stamps it ``open`` with a ``+quantity``
ENTRY event).

Both existing healers are blind to it — ``heal_fully_closed_groups`` needs an opening row and
``find_misattributed_open_groups`` needs a non-zero group net — so it needs its own detector.

Reproduces ``individual`` 5251: a /ESU6 7695 call re-opened at 15:36 by an exit firing against
an already-flat position, orphaned by the 15:40 re-close, expiring at 17:00 — 85 minutes after
group 4968 had been stamped closed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tt_ledger.enums import TradeGroupStatus
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.broker import BrokerTransaction
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.ingest.pull import sync_orders, sync_transactions
from tt_ledger.ingest.reconcile import (
    find_misattributed_open_groups,
    find_orphan_settlement_groups,
    heal_fully_closed_groups,
    reconcile,
)
from tt_ledger.ingest.remap import regroup_transactions
from tt_ledger.rows import OrderInput, TradeFilter
from tt_ledger.sdk import LedgerClient
from tt_ledger.store.memory import InMemoryStore

T0 = datetime(2026, 8, 24, 12, 18, tzinfo=UTC)
CLOSE_AT = T0 + timedelta(hours=3, minutes=17)   # the group is stamped closed here
REOPEN_AT = CLOSE_AT + timedelta(minutes=1)      # the exit re-fires against a flat position
EXPIRE_AT = CLOSE_AT + timedelta(hours=1, minutes=25)

CALL = "SPY   260824C00769500"


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1"})


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


@pytest.fixture
def ledger(store, accounts) -> LedgerClient:
    return LedgerClient(store, accounts=accounts, resolver=PassthroughResolver())


def _fill(broker: MockTastyTradeClient, *, order_id: str, action: str, net_value: str,
          executed_at: datetime) -> None:
    broker.fill(
        account_number="ACCT1", order_id=order_id, symbol=CALL, instrument_type="Equity Option",
        action=action, quantity=Decimal("1"), fill_price=Decimal("1"),
        filled_at=executed_at, underlying_symbol="SPY",
    )
    broker._transactions["ACCT1"][-1].net_value = Decimal(net_value)


def _expiration(broker: MockTastyTradeClient, *, executed_at: datetime) -> None:
    """The broker's ``Removal of option due to expiration``. TT stamps a trade action on it;
    ``_is_nontrade_close`` keys on the Receive Deliver sub-type, not the action."""
    broker.add_transaction(
        BrokerTransaction(
            id="T-EXPIRE", account_number="ACCT1", order_id=None, underlying_symbol="SPY",
            symbol=CALL, instrument_type="Equity Option", transaction_type="Receive Deliver",
            transaction_sub_type="Expiration", action="Sell to Close", quantity=Decimal("1"),
            net_value=Decimal("0"), executed_at=executed_at, transaction_date=executed_at.date(),
        )
    )


async def _sync_and_reconcile(store, accounts, broker):
    resolver = PassthroughResolver()
    await sync_orders(store, "main", client=broker, accounts=accounts, resolver=resolver)
    await sync_transactions(store, "main", client=broker, accounts=accounts, resolver=resolver)
    return await reconcile(store, "main")


async def _trades(store) -> list:
    return await store.unified_trades(TradeFilter(account="main"))


async def _orphaned(ledger, store, accounts) -> int:
    """The 4968 shape, in three reconcile passes because that is how the broker delivered it.

    Pass 1 round-trips the short call and stamps the group CLOSED. Pass 2 brings the exit's
    re-fire, which the broker filled as an OPEN — it still carries submit-time intent, so it
    attaches to the (closed) group rather than minting its own. Pass 3 brings the overnight
    settlement, which carries no order at all: ``_route_cluster`` sees only OPEN groups, the
    owner is not one, and the row mints a phantom open group.
    """
    trade = await ledger.open_trade_group("main", strategy_type="single", underlying="SPY",
                                          bot="disc_iron_man", reviewed_by="oms")
    for tt_order_id in ("O-1", "O-2", "O-3"):
        await ledger.record_order(
            OrderInput(account="main", tt_order_id=tt_order_id, underlying="SPY",
                       trade_group=trade.group_id)
        )
    owner_pk = await store.get_trade_group_id(trade.group_id)

    broker = MockTastyTradeClient()
    _fill(broker, order_id="O-1", action="Sell to Open", net_value="300", executed_at=T0)
    _fill(broker, order_id="O-2", action="Buy to Close", net_value="-5", executed_at=CLOSE_AT)
    await _sync_and_reconcile(store, accounts, broker)
    assert (await _trades(store))[0].status == TradeGroupStatus.CLOSED.value

    _fill(broker, order_id="O-3", action="Buy to Open", net_value="-5", executed_at=REOPEN_AT)
    await _sync_and_reconcile(store, accounts, broker)

    _expiration(broker, executed_at=EXPIRE_AT)
    await _sync_and_reconcile(store, accounts, broker)
    return owner_pk


async def test_orphan_settlement_group_is_reported_with_its_owner(ledger, store, accounts):
    owner_pk = await _orphaned(ledger, store, accounts)

    found = await find_orphan_settlement_groups(store, "main")
    assert len(found) == 1
    assert found[0]["securities"] == [CALL]
    assert found[0]["covered_by"] == [owner_pk], "the closed owner must be named, not just hinted"


async def test_the_existing_healers_are_blind_to_it(ledger, store, accounts):
    """Both are, for structural reasons — which is why this detector exists at all."""
    await _orphaned(ledger, store, accounts)

    assert await heal_fully_closed_groups(store, "main", dry_run=True) == 0
    assert await find_misattributed_open_groups(store, "main") == []


async def test_regrouping_into_the_named_owner_closes_both(ledger, store, accounts):
    """The repair the report enables: move the settlement onto its owner and the owner's
    lifecycle + realized P&L restate, while the emptied orphan drops out of the review set."""
    owner_pk = await _orphaned(ledger, store, accounts)
    orphan = (await find_orphan_settlement_groups(store, "main"))[0]

    txns, _ = await ledger.transactions(trade_group_id=orphan["group_pk"], limit=100)
    owner_group_id = (await store.get_trade_group_by_id(owner_pk)).group_id
    await regroup_transactions(
        store, [t.id for t in txns], target_group_id=owner_group_id, reviewed_by="test",
    )

    owner = await store.get_trade_group_by_id(owner_pk)
    # closed + expired in one group -> MIXED, the same rule _apply_exit uses
    assert owner.status == TradeGroupStatus.MIXED.value
    # 300 - 5 (round trip) - 5 (the stray lot, expired worthless)
    assert owner.realized_pnl == Decimal("290")
    assert await find_orphan_settlement_groups(store, "main") == []


async def test_a_settlement_with_no_owner_in_history_is_reported_but_not_mergeable(store, accounts):
    """History that doesn't reach the entry: reportable, ``covered_by`` empty — never guessed
    onto an unrelated group."""
    broker = MockTastyTradeClient()
    _expiration(broker, executed_at=EXPIRE_AT)
    await _sync_and_reconcile(store, accounts, broker)

    found = await find_orphan_settlement_groups(store, "main")
    assert len(found) == 1
    assert found[0]["covered_by"] == []


async def test_a_genuinely_open_group_is_never_reported(store, accounts):
    broker = MockTastyTradeClient()
    _fill(broker, order_id="O-1", action="Sell to Open", net_value="300", executed_at=T0)
    await _sync_and_reconcile(store, accounts, broker)

    assert (await _trades(store))[0].status == TradeGroupStatus.OPEN.value
    assert await find_orphan_settlement_groups(store, "main") == []
