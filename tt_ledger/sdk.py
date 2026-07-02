"""``LedgerClient`` — the in-process Python API (docs/api.md).

The canonical entry point. Accepts nicknames + security_id only (Rule 1/Rule 2). The HTTP
server (tt_ledger.api) and CLI (tt_ledger.cli) are thin wrappers over this.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from .enums import Ingest, Origin, ReviewStatus
from .identity import PassthroughResolver
from .ingest.pull import sync_all
from .ingest.reconcile import reconcile
from .ingest.remap import dismiss_trade_group, regroup_transactions, remap_trade_group
from .ingest.replay import rebuild_positions_from_transactions
from .repositories import apply_fill_event
from .rows import ActivityFilter, OrderFilter, OrderRow, TradeFilter, trade_group_to_row
from .store import make_store

if TYPE_CHECKING:
    from .identity import AccountMapper, SecurityResolver
    from .ingest.broker import BrokerClient
    from .rows import ActivityRow, ClosedPositionRow, FillEvent, OrderInput, PositionRow, SyncResult, TradeRow
    from .store import LedgerStore


class LedgerClient:
    def __init__(
        self,
        store: "LedgerStore",
        *,
        accounts: "AccountMapper",
        resolver: "SecurityResolver | None" = None,
        client: "BrokerClient | None" = None,
    ) -> None:
        self._store = store
        self._accounts = accounts
        # Injectable symbology. Default: canonical security_id == the raw vendor symbol.
        self._resolver: SecurityResolver = resolver or PassthroughResolver()
        # No default: no real TastyTrade REST client ships in this package yet (see
        # docs/implementation-notes.md). Required only by sync(); every other method works
        # without one. Pass MockTastyTradeClient for tests, or a real client once you have one.
        self._client = client

    @classmethod
    def open(
        cls,
        url: str = "sqlite+aiosqlite:///ledger.db",
        *,
        accounts: "AccountMapper",
        resolver: "SecurityResolver | None" = None,
        client: "BrokerClient | None" = None,
    ) -> "LedgerClient":
        """Open a ledger on ``url`` (SQLite default, Postgres opt-in).

        ``resolver`` translates broker symbols to your canonical ``security_id``; if omitted,
        the vendor symbol is used as the canonical id (PassthroughResolver). ``client`` is the
        broker connection ``sync()`` pulls from; omit it if you only need the read/remap surface.
        """
        return cls(make_store(url), accounts=accounts, resolver=resolver, client=client)

    # --- capture ---

    async def sync(self, account: str, since: date | None = None) -> "SyncResult":
        """Pull (orders + transactions + positions) then reconcile."""
        if self._client is None:
            raise RuntimeError(
                "LedgerClient.sync() requires a broker client -- pass client=<BrokerClient> to "
                "LedgerClient.open()/__init__ (a real TastyTrade REST client, or "
                "MockTastyTradeClient for testing)."
            )
        return await sync_all(
            self._store, account, client=self._client, accounts=self._accounts,
            resolver=self._resolver, since=since,
        )

    async def record_order(self, order: "OrderInput") -> "OrderRow":
        """Record an order at submission (oms_submit path) -- before any broker confirmation,
        so ``tt_order_id`` is unset; it arrives later via push/pull enrichment."""
        row = OrderRow(
            tt_order_id=None, account=order.account, origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT,
            security_id=order.security_id, underlying=order.underlying,
            order_type=order.order_type, time_in_force=order.time_in_force,
            price=order.price, price_effect=order.price_effect,
            is_complex=order.is_complex, complex_order_type=order.complex_order_type,
            signal_id=order.signal_id, trace_id=order.trace_id, strategy_id=order.strategy_id,
            market_context_id=order.market_context_id, received_at=datetime.now(UTC),
        )
        await self._store.upsert_orders([row])
        return row

    async def apply_fill(self, evt: "FillEvent") -> None:
        """A fill/status update from the push (stream) path. Enriches an existing order by
        ``tt_order_id`` only -- a fill for an unknown order is a no-op (docs/ingestion.md:
        sync_orders, not the stream, is authoritative for order structure)."""
        await apply_fill_event(self._store, evt)

    # --- read (consolidated views) ---

    async def orders(self, **f) -> "list[OrderRow]":
        if "origin" in f and isinstance(f["origin"], str):
            f["origin"] = Origin(f["origin"])
        return await self._store.query_orders(OrderFilter(**f))

    async def trades(self, **f) -> "list[TradeRow]":
        if "origin" in f and isinstance(f["origin"], str):
            f["origin"] = Origin(f["origin"])
        if "review_status" in f and isinstance(f["review_status"], str):
            f["review_status"] = ReviewStatus(f["review_status"])
        return await self._store.unified_trades(TradeFilter(**f))

    async def trade(self, group_id: str) -> "TradeRow | None":
        tg = await self._store.get_trade_group(group_id)
        return trade_group_to_row(tg) if tg is not None else None

    async def account_activity(self, account: str, **f) -> "list[ActivityRow]":
        return await self._store.account_activity(ActivityFilter(account=account, **f))

    async def trade_detail(self, group_id: str) -> "tuple[list[OrderRow], list[ActivityRow]]":
        """A trade's orders + transactions (docs/api.md's ``GET /trades/{group_id}`` detail view).
        Legs/fills/events aren't included -- no query-by-order/query-by-group method exists for
        those yet; this covers what's readily available without inventing more store surface."""
        pk = await self._store.get_trade_group_id(group_id)
        tg = await self._store.get_trade_group(group_id)
        if pk is None or tg is None:
            return [], []
        orders = await self._store.query_orders(OrderFilter(trade_group_id=pk))
        activity = await self._store.account_activity(ActivityFilter(account=tg.account))
        transactions = [row for row in activity if row.trade_group_id == pk]
        return orders, transactions

    async def position(self, account: str, security_id: str) -> "PositionRow | None":
        return await self._store.get_position(account, security_id)

    async def positions(self, account: str, *, open_only: bool = True) -> "list[PositionRow]":
        """The account's positions -- ``positions`` never deletes a row once a security fully
        closes (docs/ingestion.md → Replay), so ``open_only`` (default) filters those flat
        (``quantity == 0``) rows out; pass ``open_only=False`` to see everything ever held."""
        rows = await self._store.get_positions(account)
        return [r for r in rows if not open_only or r.quantity != 0]

    async def closed_positions(self, account: str, security_id: str | None = None) -> "list[ClosedPositionRow]":
        return await self._store.get_closed_positions(account, security_id)

    # --- reconcile ---

    async def reconcile(self, account: str | None = None, *, since: date | None = None, dry_run: bool = False) -> "SyncResult":
        """Re-run reconcile without a broker pull (e.g. after backfilling data another way)."""
        return await reconcile(self._store, account, since=since, dry_run=dry_run)

    async def rebuild_positions(self, account: str | None = None) -> "SyncResult":
        """Rebuild ``positions``/``closed_positions`` from transaction history
        (``ingest/replay.py``) -- no broker pull, safe to re-run any time after ``sync``."""
        return await rebuild_positions_from_transactions(self._store, account)

    # --- remap ---

    async def remap_trade(
        self, group_id: str, *, strategy=None, bot=None, signal=None,  # noqa: ANN001
        strategy_type=None, reviewed_by: str,  # noqa: ANN001
    ) -> "TradeRow":
        return await remap_trade_group(
            self._store, group_id, strategy=strategy, bot=bot, signal=signal,
            strategy_type=strategy_type, reviewed_by=reviewed_by,
        )

    async def regroup(self, txn_ids: list[int], *, target: str | None, reviewed_by: str) -> "list[TradeRow]":
        return await regroup_transactions(self._store, txn_ids, target_group_id=target, reviewed_by=reviewed_by)

    async def dismiss_trade(self, group_id: str, *, reviewed_by: str) -> "TradeRow":
        return await dismiss_trade_group(self._store, group_id, reviewed_by=reviewed_by)

    async def close(self) -> None:
        dispose = getattr(self._store, "dispose", None)
        if dispose is not None:
            await dispose()
