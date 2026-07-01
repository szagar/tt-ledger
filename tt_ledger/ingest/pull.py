"""Pull (REST) ingestion — idempotent (docs/ingestion.md).

Every importer takes an injected ``BrokerClient`` (``ingest/broker.py`` — a real TastyTrade REST
client, the ``[tastytrade]`` extra, or ``MockTastyTradeClient`` for tests) plus the ``AccountMapper``
and ``SecurityResolver`` needed to translate broker-native ids to nicknames/``security_id`` at this
boundary (Rule 1/Rule 2, docs/identity.md) before anything reaches the store.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from ..repositories import OrderRepository, PositionRepository, TransactionRepository
from ..rows import SyncResult
from .reconcile import reconcile

if TYPE_CHECKING:
    from ..identity import AccountMapper, SecurityResolver
    from ..ingest.broker import BrokerClient
    from ..store import LedgerStore

_EPOCH = date(1970, 1, 1)


async def sync_orders(
    store: "LedgerStore",
    account: str,
    *,
    client: "BrokerClient",
    accounts: "AccountMapper",
    resolver: "SecurityResolver",
    since: date | None = None,
) -> int:
    """Upsert orders on tt_order_id, legs, and fills on fill_id. One importer, both origins:
    enrich existing origin=zts rows (fill/status only), create broker rows."""
    account_number = accounts.to_account_number(account)
    placed_orders = await client.get_order_history(account_number, since or _EPOCH, date.today())
    return await OrderRepository(store, resolver=resolver).upsert_from_history(placed_orders, account=account)


async def sync_transactions(
    store: "LedgerStore",
    account: str,
    *,
    client: "BrokerClient",
    accounts: "AccountMapper",
    resolver: "SecurityResolver",
    since: date | None = None,
) -> int:
    """Upsert transactions on tt_transaction_id; capture the broker order-id into tt_order_id.

    Linking that ``tt_order_id`` to the order's surrogate id is the reconcile pass's job
    (``ingest/reconcile.py``), not this importer's — this only upserts cash-truth rows.
    """
    account_number = accounts.to_account_number(account)
    txns = await client.get_transaction_history(account_number, since or _EPOCH, date.today())
    return await TransactionRepository(store, resolver=resolver).upsert(txns, account=account)


async def sync_positions(
    store: "LedgerStore",
    account: str,
    *,
    client: "BrokerClient",
    accounts: "AccountMapper",
    resolver: "SecurityResolver",
) -> int:
    """Upsert positions on (account, security_id) — the current snapshot, no date range."""
    account_number = accounts.to_account_number(account)
    positions = await client.get_positions(account_number)
    return await PositionRepository(store, resolver=resolver).upsert(positions, account=account)


async def sync_all(
    store: "LedgerStore",
    account: str,
    *,
    client: "BrokerClient",
    accounts: "AccountMapper",
    resolver: "SecurityResolver",
    since: date | None = None,
) -> "SyncResult":
    """orders -> transactions -> positions -> reconcile.

    Each step's failure is caught and recorded in ``errors`` rather than aborting the rest —
    a broker hiccup on one feed shouldn't discard the others (that's what ``SyncResult.errors``
    is for). Reconcile runs last regardless of which pull steps succeeded, since it only acts on
    whatever's already in the store (a previous sync's data counts too).
    """
    result = SyncResult()

    try:
        result.orders = await sync_orders(store, account, client=client, accounts=accounts, resolver=resolver, since=since)
    except Exception as exc:  # noqa: BLE001 - a broker/store failure here must not abort the other feeds
        result.errors.append(f"sync_orders: {exc}")

    try:
        result.transactions = await sync_transactions(store, account, client=client, accounts=accounts, resolver=resolver, since=since)
    except Exception as exc:  # noqa: BLE001
        result.errors.append(f"sync_transactions: {exc}")

    try:
        result.positions = await sync_positions(store, account, client=client, accounts=accounts, resolver=resolver)
    except Exception as exc:  # noqa: BLE001
        result.errors.append(f"sync_positions: {exc}")

    try:
        reconcile_result = await reconcile(store, account, since=since)
        result.trade_groups += reconcile_result.trade_groups
        result.errors.extend(reconcile_result.errors)
    except Exception as exc:  # noqa: BLE001
        result.errors.append(f"reconcile: {exc}")

    return result
