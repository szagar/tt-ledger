"""``LedgerClient`` — the in-process Python API (docs/api.md).

The canonical entry point. Accepts nicknames + security_id only (Rule 1/Rule 2). The HTTP
server (tt_ledger.api) and CLI (tt_ledger.cli) are thin wrappers over this.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from .enums import Ingest, Origin, ReviewStatus, TradeGroupEventType
from .identity import PassthroughResolver
from .ingest.pull import sync_all
from .ingest.push import StreamConsumer
from .ingest.reconcile import reconcile
from .ingest.remap import dismiss_trade_group, regroup_transactions, remap_trade_group
from .ingest.replay import rebuild_positions_from_transactions
from .repositories import apply_fill_event, ensure_account
from .rows import (
    ActivityFilter,
    EventRow,
    OrderDetail,
    OrderFilter,
    OrderRow,
    SyncResult,
    TradeFilter,
    TradeGroupRow,
    TransactionQuery,
    trade_group_to_row,
)
from .store import make_store

if TYPE_CHECKING:
    from typing import Callable

    from .identity import AccountMapper, SecurityResolver
    from .ingest.broker import BalanceMessage, BrokerClient, BrokerTransaction
    from .ingest.push import MessageSource
    from .rows import (
        ActivityRow,
        BalanceSnapshotRow,
        ClosedPositionRow,
        FillEvent,
        OrderInput,
        PositionRow,
        TradeRow,
        TransactionDetailRow,
    )
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
        # No default: required only by sync(); every other method works without one. Pass
        # ingest.tastytrade_client.TastyTradeClient (the real REST client, [tastytrade] extra)
        # or MockTastyTradeClient for tests.
        self._client = client
        self._ensured_accounts: set[str] = set()

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
        await ensure_account(self._store, self._accounts, account, self._ensured_accounts)
        return await sync_all(
            self._store, account, client=self._client, accounts=self._accounts,
            resolver=self._resolver, since=since,
        )

    async def record_order(self, order: "OrderInput") -> "OrderRow":
        """Record an order at submission (oms_submit path). Pass ``tt_order_id`` when the broker's
        submit response already supplied it (the pull/push paths then enrich this row instead of
        creating a broker-origin duplicate); pass ``trade_group`` (from ``open_trade_group``) to
        pre-attribute the order -- reconcile attaches its transactions to that group instead of
        clustering them into a new needs-review one."""
        await ensure_account(self._store, self._accounts, order.account, self._ensured_accounts)
        trade_group_id = None
        if order.trade_group is not None:
            trade_group_id = await self._store.get_trade_group_id(order.trade_group)
            if trade_group_id is None:
                raise ValueError(f"unknown trade_group {order.trade_group!r} -- open_trade_group() first")
        row = OrderRow(
            tt_order_id=order.tt_order_id, account=order.account, origin=Origin.ZTS, ingest=Ingest.OMS_SUBMIT,
            oms_order_id=order.oms_order_id,
            security_id=order.security_id, underlying=order.underlying,
            order_type=order.order_type, time_in_force=order.time_in_force,
            price=order.price, price_effect=order.price_effect,
            is_complex=order.is_complex, complex_order_type=order.complex_order_type,
            signal_id=order.signal_id, trace_id=order.trace_id, strategy_id=order.strategy_id,
            trade_group_id=trade_group_id,
            market_context_id=order.market_context_id, received_at=datetime.now(UTC),
        )
        await self._store.upsert_orders([row])
        return row

    async def open_trade_group(
        self,
        account: str,
        *,
        strategy_type: str | None = None,
        underlying: str | None = None,
        security_id: str | None = None,
        quantity: "Decimal | None" = None,
        total_premium: "Decimal | None" = None,
        max_profit: "Decimal | None" = None,
        max_loss: "Decimal | None" = None,
        initial_risk: "Decimal | None" = None,
        profit_target: str | None = None,
        stop_loss: str | None = None,
        exit_strategy: str | None = None,
        structure: dict | None = None,
        strategy_id: int | None = None,
        bot: str | None = None,
        signal: str | None = None,
        reviewed_by: str | None = None,
    ) -> "TradeRow":
        """Open a trade_group at submit time -- the moment strategy intent (bot, signal,
        strategy type, planned risk) exists. The group is ``origin=zts``, ``confirmed``, and
        ``manually_attributed`` (intent beats reconcile's clustering heuristics); pass its
        ``group_id`` to ``record_order(trade_group=...)`` so fills attach to it. Financials
        (premium/fees/quantity) are refined from actual fills by reconcile. ``structure`` is
        an opaque host-written descriptor of the traded structure (legs/strikes/expiry/...);
        the ledger stores it verbatim and never derives from or mutates it. ``initial_risk``
        is the planned 1R in dollars (where the host intends to stop — may be tighter than
        the structural ``max_loss``); it is frozen here and never recomputed."""
        await ensure_account(self._store, self._accounts, account, self._ensured_accounts)
        now = datetime.now(UTC)
        row = TradeGroupRow(
            group_id=str(uuid.uuid4()), account=account, origin=Origin.ZTS,
            review_status=ReviewStatus.CONFIRMED, manually_attributed=True,
            reviewed_at=now, reviewed_by=reviewed_by,
            underlying=underlying, security_id=security_id, strategy_type=strategy_type,
            total_premium=total_premium, quantity=quantity,
            max_profit=max_profit, max_loss=max_loss, initial_risk=initial_risk,
            profit_target=profit_target, stop_loss=stop_loss, exit_strategy=exit_strategy,
            structure=structure,
            strategy_id=strategy_id, bot_name=bot, signal_id=signal,
            executed_at=now,
        )
        group_pk = await self._store.upsert_trade_group(row)
        await self._store.add_trade_group_event(
            EventRow(
                trade_group_id=group_pk, event_type=TradeGroupEventType.ENTRY.value,
                quantity_change=quantity or Decimal("0"), premium_change=total_premium or Decimal("0"),
                event_at=now,
            )
        )
        return trade_group_to_row(row)

    async def import_transactions(
        self,
        account: str,
        txns: "list[BrokerTransaction]",
        *,
        source_system: str = "synthetic",
        reconcile_after: bool = True,
    ) -> "SyncResult":
        """Inject host-generated transactions — e.g. a paper account's synthetic settlements
        (expiration / cash-settled exercise), which have no broker feed behind them.

        Idempotent on each record's ``id`` (-> ``tt_transaction_id``): give synthetic records a
        stable deterministic id (e.g. ``paper-exp-<security>-<date>``) so re-imports are no-ops.
        ``reconcile_after`` (default) immediately routes the new rows — an expiration row lands
        on its open trade_group with the proper lifecycle event/status via the normal reconcile
        machinery."""
        from .repositories import TransactionRepository

        await ensure_account(self._store, self._accounts, account, self._ensured_accounts)
        result = SyncResult()
        result.transactions = await TransactionRepository(self._store, resolver=self._resolver).upsert(
            txns, account=account, source_system=source_system,
        )
        if reconcile_after:
            rec = await reconcile(self._store, account)
            result.trade_groups = rec.trade_groups
            result.errors.extend(rec.errors)
        return result

    async def apply_fill(self, evt: "FillEvent") -> None:
        """A fill/status update from the push (stream) path. Enriches an existing order by
        ``tt_order_id`` only -- a fill for an unknown order is a no-op (docs/ingestion.md:
        sync_orders, not the stream, is authoritative for order structure)."""
        await apply_fill_event(self._store, evt)

    def stream_consumer(
        self,
        source: "MessageSource",
        *,
        on_balance: "Callable[[BalanceMessage], None] | None" = None,
        persist_balances: bool = True,
        balance_min_interval_seconds: float = 60.0,
    ) -> "StreamConsumer":
        """A ``StreamConsumer`` bound to this ledger's store/accounts/resolver, consuming an
        already-built transport (``TastyTradeMessageSource`` for the real account-streamer,
        ``RedisMessageSource`` for a host platform's pub/sub, ``MockMessageSource`` for tests).
        ``LedgerClient`` stays transport-agnostic -- it never reads accounts.toml credentials
        itself, same as ``sync()`` takes an already-built ``BrokerClient`` rather than
        constructing one. Balance messages are persisted to ``balance_snapshots`` (throttled per
        ``balance_min_interval_seconds``; NLV changes always persist) unless
        ``persist_balances=False``."""
        return StreamConsumer(
            self._store, source, accounts=self._accounts, resolver=self._resolver,
            on_balance=on_balance, persist_balances=persist_balances,
            balance_min_interval_seconds=balance_min_interval_seconds,
        )

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

    async def trade_structure(self, group_id: str) -> "list[OrderDetail]":
        """A trade's orders with their legs + fills — the drill-down read path host UIs render
        (each ``OrderDetail.fills`` entry's ``order_leg_id`` matches a leg's ``id``). Empty for a
        group whose orders arrived without leg detail (e.g. intent-recorded paper orders — their
        activity lives in ``trade_detail()``'s transactions instead)."""
        pk = await self._store.get_trade_group_id(group_id)
        if pk is None:
            return []
        orders = await self._store.get_group_orders_with_ids(pk)
        order_ids = [order_pk for order_pk, _ in orders]
        legs = await self._store.get_legs_for_orders(order_ids)
        fills = await self._store.get_fills_for_orders(order_ids)
        legs_by_order: dict[int, list] = {}
        for leg in legs:
            legs_by_order.setdefault(leg.order_id, []).append(leg)
        fills_by_order: dict[int, list] = {}
        for fill in fills:
            if fill.order_id is not None:
                fills_by_order.setdefault(fill.order_id, []).append(fill)
        return [
            OrderDetail(
                order_pk=order_pk, order=order,
                legs=legs_by_order.get(order_pk, []), fills=fills_by_order.get(order_pk, []),
            )
            for order_pk, order in orders
        ]

    async def transactions(self, **f) -> "tuple[list[TransactionDetailRow], int]":
        """Paged transactions, newest first: ``(rows, total_matching)``. Filters are
        ``TransactionQuery`` fields (account/accounts/start/end/underlying/transaction_type/
        trade_group_id/limit/offset)."""
        return await self._store.query_transactions(TransactionQuery(**f))

    async def open_position_groups(self, account: str | None = None) -> "dict[tuple[str, str], int]":
        """``(account, security_id) -> trade_group_id`` over OPEN groups' member transactions —
        the join host position views stamp onto live position rows. A security claimed by more
        than one open group resolves to the most recently created one (highest pk)."""
        mapping: dict[tuple[str, str], int] = {}
        for acct, security_id, group_pk in await self._store.get_open_position_groups(account):
            key = (acct, security_id)
            if group_pk > mapping.get(key, -1):
                mapping[key] = group_pk
        return mapping

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

    async def latest_balance(self, account: str) -> "BalanceSnapshotRow | None":
        """The most recent balance snapshot (stream- or sync-written) for ``account``."""
        return await self._store.get_latest_balance(account)

    async def balances(
        self, account: str, *, since: date | None = None, until: date | None = None,
    ) -> "list[BalanceSnapshotRow]":
        """The account's balance time series (NLV history), oldest first."""
        return await self._store.get_balances(account, start=since, end=until)

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
        broker_close = getattr(self._client, "close", None)
        if broker_close is not None:
            await broker_close()
