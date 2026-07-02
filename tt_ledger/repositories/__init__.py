"""Repositories — domain operations over the LedgerStore (docs/storage.md, docs/schema.md).

Each repository takes a ``LedgerStore`` and exposes intent-level methods; it owns the
invariants (idempotent upsert keys, the consolidated-view query shapes). ``OrderRepository``
directly consumes broker-native ``PlacedOrder`` shapes (``ingest/broker.py``) — the repository
layer is where that translation happens, not the store.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from ..enums import Ingest, Origin, OrderStatus
from ..rows import AccountRow, BalanceSnapshotRow, FillRow, LegRow, OrderRow, PositionRow, SecurityRow, TxnRow

if TYPE_CHECKING:
    from ..identity import ResolvedSecurity, SecurityResolver
    from ..ingest.broker import BalanceMessage, BrokerPosition, BrokerTransaction, PlacedFill, PlacedOrder
    from ..rows import FillEvent, OrderFilter, TradeFilter, TradeGroupRow, TradeRow
    from ..store import LedgerStore


async def ensure_account(store: "LedgerStore", accounts, nickname: str, cache: set[str]) -> None:  # noqa: ANN001
    """Seed the accounts-dimension row for ``nickname`` (idempotent; ``cache`` skips repeats).

    Every fact table FKs ``account -> accounts.nickname``, and nothing else populates the
    dimension -- writers MUST call this before their first write for an account."""
    if nickname in cache:
        return
    await store.upsert_account(
        AccountRow(
            nickname=nickname,
            account_number=accounts.to_account_number(nickname),
            login=accounts.login_for(nickname) or "",  # accounts.login is NOT NULL
            env=accounts.env_for(nickname),
        )
    )
    cache.add(nickname)


class _Repo:
    def __init__(self, store: "LedgerStore") -> None:
        self._store = store


class SecurityRepository(_Repo):
    async def upsert(self, resolved: "ResolvedSecurity", *, tt_symbol: str | None = None) -> None:
        """Upsert the securities dimension row from a resolver result (+ the vendor symbol)."""
        await self._store.upsert_security(
            SecurityRow(
                security_id=resolved.security_id,
                product_type=resolved.product_type or "",
                underlying=resolved.underlying,
                expiry=resolved.expiry,
                strike=resolved.strike,
                option_type=resolved.option_type,
                tt_symbol=tt_symbol,
            )
        )


# TastyTrade raw order-status -> normalized OrderStatus, verified against their Order Flow doc
# (developer.tastytrade.com) — the full real vocabulary: Received, Routed, In Flight, Live, Cancel
# Requested, Replace Requested, Contingent (non-terminal); Filled, Cancelled, Expired, Rejected,
# Removed, Partially Removed (terminal). There is no "Partially Filled" order status at all —
# partial-fill information lives on the leg's quantity/remaining-quantity, not the order status —
# and "Partially Removed" means an admin manually removed part of the order, unrelated to fills,
# so it maps like "Removed" does. Unrecognized values map to None; ``tt_status`` always keeps the
# raw string regardless, so no information is lost either way.
_STATUS_MAP: dict[str, OrderStatus] = {
    "received": OrderStatus.PENDING,
    "routed": OrderStatus.SUBMITTED,
    "in flight": OrderStatus.SUBMITTED,
    "live": OrderStatus.WORKING,
    "contingent": OrderStatus.WORKING,
    "cancel requested": OrderStatus.WORKING,
    "replace requested": OrderStatus.WORKING,
    "filled": OrderStatus.FILLED,
    "cancelled": OrderStatus.CANCELLED,
    "canceled": OrderStatus.CANCELLED,
    "removed": OrderStatus.CANCELLED,
    "partially removed": OrderStatus.CANCELLED,
    "rejected": OrderStatus.REJECTED,
    "expired": OrderStatus.EXPIRED,
}


def map_order_status(raw: str | None) -> OrderStatus | None:
    if raw is None:
        return None
    return _STATUS_MAP.get(raw.strip().lower())


async def apply_fill_event(store: "LedgerStore", evt: "FillEvent") -> OrderRow | None:
    """A fill/status update from the push (stream) path. Enriches an existing order by
    ``tt_order_id`` only -- a fill for an unknown order is a no-op and returns ``None``
    (docs/ingestion.md: sync_orders, not the stream, is authoritative for order structure).
    Shared by ``LedgerClient.apply_fill`` and ``ingest.push.StreamConsumer``."""
    existing = await store.get_order(evt.tt_order_id)
    if existing is None:
        return None
    oms_status = map_order_status(evt.status)
    updated = replace(
        existing,
        oms_status=(oms_status.value if oms_status else existing.oms_status),
        tt_status=(evt.status if evt.status is not None else existing.tt_status),
        average_fill_price=(evt.average_fill_price if evt.average_fill_price is not None else existing.average_fill_price),
        filled_quantity=(evt.filled_quantity if evt.filled_quantity is not None else existing.filled_quantity),
        remaining_quantity=(evt.remaining_quantity if evt.remaining_quantity is not None else existing.remaining_quantity),
        filled_at=(evt.filled_at if evt.filled_at is not None else existing.filled_at),
    )
    await store.upsert_orders([updated])
    return updated


def _vwap(fills: "list[PlacedFill]") -> Decimal | None:
    """A leg's fills -> the quantity-weighted average fill price."""
    total_qty = sum((f.quantity for f in fills), Decimal("0"))
    if not fills or total_qty == 0:
        return None
    return sum((f.quantity * f.fill_price for f in fills), Decimal("0")) / total_qty


def order_level_fill_fields(legs: "list") -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    """(average_fill_price, filled_quantity, remaining_quantity) for a SINGLE-leg order only.

    TastyTrade's real Order object has no order-level fill fields at all (verified against their
    OpenAPI spec) — only each leg's own quantity/remaining-quantity/fills. A multi-leg spread's
    net execution price isn't a plain average of its legs' fill prices either, so this stays
    ``(None, None, None)`` for anything but exactly one leg.
    """
    if len(legs) != 1:
        return None, None, None
    leg = legs[0]
    filled_quantity = sum((f.quantity for f in leg.fills), Decimal("0"))
    return _vwap(leg.fills), filled_quantity, leg.remaining_quantity


class OrderRepository(_Repo):
    def __init__(self, store: "LedgerStore", *, resolver: "SecurityResolver") -> None:
        super().__init__(store)
        self._resolver = resolver
        self._securities = SecurityRepository(store)

    async def upsert_from_history(self, placed_orders: "list[PlacedOrder]", *, account: str) -> int:
        """sync_orders core: upsert orders+legs+fills. One importer serves both origins —
        an existing ``origin=zts`` row is enriched (fill/status fields only; attribution
        untouched); a ``tt_order_id`` with no row is created ``origin=broker``."""
        if not placed_orders:
            return 0

        seen_security_ids: set[str] = set()
        resolved_legs: list[list["ResolvedSecurity"]] = []
        for po in placed_orders:
            resolved_for_order = []
            for leg in po.legs:
                resolved = self._resolver.resolve(leg.symbol, leg.instrument_type)
                if resolved.security_id not in seen_security_ids:
                    await self._securities.upsert(resolved, tt_symbol=leg.symbol)
                    seen_security_ids.add(resolved.security_id)
                resolved_for_order.append(resolved)
            resolved_legs.append(resolved_for_order)

        order_rows = [
            await self._build_order_row(
                po, account=account,
                security_id=(resolved_legs[i][0].security_id if len(po.legs) == 1 else None),
                fill_fields=order_level_fill_fields(po.legs),
            )
            for i, po in enumerate(placed_orders)
        ]
        order_ids = await self._store.upsert_orders(order_rows)

        leg_rows: list[LegRow] = []
        leg_owner: list[tuple[int, int]] = []  # (placed_order index, leg index), parallel to leg_rows
        for po_index, po in enumerate(placed_orders):
            order_id = order_ids[po_index]
            for leg_index, leg in enumerate(po.legs):
                resolved = resolved_legs[po_index][leg_index]
                leg_rows.append(
                    LegRow(
                        order_id=order_id, leg_index=leg_index, security_id=resolved.security_id,
                        action=leg.action, quantity=leg.quantity, remaining_quantity=leg.remaining_quantity,
                        fill_price=_vwap(leg.fills),
                    )
                )
                leg_owner.append((po_index, leg_index))
        leg_ids = await self._store.upsert_legs(leg_rows) if leg_rows else []

        fill_rows: list[FillRow] = []
        for (po_index, leg_index), leg_id in zip(leg_owner, leg_ids):
            po = placed_orders[po_index]
            order_id = order_ids[po_index]
            leg = po.legs[leg_index]
            for f in leg.fills:
                fill_rows.append(
                    FillRow(
                        fill_id=f.fill_id, order_id=order_id, order_leg_id=leg_id, tt_order_id=po.id,
                        quantity=f.quantity, fill_price=f.fill_price, filled_at=f.filled_at,
                        destination_venue=f.destination_venue, ext_exec_id=f.ext_exec_id,
                        ext_group_fill_id=f.ext_group_fill_id,
                    )
                )
        if fill_rows:
            await self._store.upsert_fills(fill_rows)

        return len(order_rows)

    async def _build_order_row(
        self, po: "PlacedOrder", *, account: str, security_id: str | None,
        fill_fields: tuple[Decimal | None, Decimal | None, Decimal | None],
    ) -> OrderRow:
        average_fill_price, filled_quantity, remaining_quantity = fill_fields
        existing = await self._store.get_order(po.id)
        oms_status = map_order_status(po.status)
        is_filled = oms_status is OrderStatus.FILLED

        if existing is not None and existing.origin is Origin.ZTS:
            # enrich only: fill/status fields; origin, signal_id, trace_id, strategy_id, and
            # everything else about attribution/structure stays exactly as it was.
            return replace(
                existing,
                oms_status=(oms_status.value if oms_status else existing.oms_status),
                tt_status=po.status,
                status_message=po.reject_reason if po.reject_reason is not None else existing.status_message,
                average_fill_price=average_fill_price if average_fill_price is not None else existing.average_fill_price,
                filled_quantity=filled_quantity if filled_quantity is not None else existing.filled_quantity,
                remaining_quantity=remaining_quantity if remaining_quantity is not None else existing.remaining_quantity,
                filled_at=po.terminal_at if is_filled else existing.filled_at,
            )

        return OrderRow(
            tt_order_id=po.id, account=account, origin=Origin.BROKER, ingest=Ingest.ORDER_HISTORY,
            security_id=security_id, underlying=po.underlying_symbol,
            order_type=po.order_type, time_in_force=po.time_in_force, gtc_date=po.gtc_date,
            price=po.price, stop_trigger=po.stop_trigger, price_effect=po.price_effect,
            average_fill_price=average_fill_price, is_complex=po.is_complex,
            complex_order_type=po.complex_order_tag, status_message=po.reject_reason,
            oms_status=(oms_status.value if oms_status else None), tt_status=po.status,
            filled_quantity=filled_quantity, remaining_quantity=remaining_quantity,
            received_at=po.received_at, submitted_at=po.received_at,
            filled_at=(po.terminal_at if is_filled else None), terminal_at=po.terminal_at,
        )

    async def query(self, f: "OrderFilter") -> "list[OrderRow]":
        return await self._store.query_orders(f)


class TransactionRepository(_Repo):
    def __init__(self, store: "LedgerStore", *, resolver: "SecurityResolver") -> None:
        super().__init__(store)
        self._resolver = resolver
        self._securities = SecurityRepository(store)

    async def upsert(self, txns: "list[BrokerTransaction]", *, account: str, source_system: str = "tastytrade") -> int:
        """sync_transactions core: upsert on tt_transaction_id, capturing the broker's order-id
        into tt_order_id (the deterministic txn->order link key — actually linking to the
        order's surrogate id is the reconcile pass's job, not this importer's).

        ``source_system`` distinguishes real broker feeds from host-injected synthetic records
        (e.g. paper-account settlements imported via ``LedgerClient.import_transactions``)."""
        if not txns:
            return 0

        seen_security_ids: set[str] = set()
        rows: list[TxnRow] = []
        for t in txns:
            security_id = None
            if t.symbol:
                resolved = self._resolver.resolve(t.symbol, t.instrument_type)
                if resolved.security_id not in seen_security_ids:
                    await self._securities.upsert(resolved, tt_symbol=t.symbol)
                    seen_security_ids.add(resolved.security_id)
                security_id = resolved.security_id

            rows.append(
                TxnRow(
                    tt_transaction_id=t.id, tt_order_id=t.order_id, account=account,
                    account_number=t.account_number, source_system=source_system,
                    transaction_type=t.transaction_type,
                    transaction_sub_type=t.transaction_sub_type, action=t.action,
                    security_id=security_id, underlying=t.underlying_symbol,
                    quantity=t.quantity, price=t.price, value=t.value, value_effect=t.value_effect,
                    net_value=t.net_value, net_value_effect=t.net_value_effect,
                    commission=t.commission, clearing_fees=t.clearing_fees,
                    regulatory_fees=t.regulatory_fees,
                    proprietary_index_option_fees=t.proprietary_index_option_fees,
                    is_estimated_fee=t.is_estimated_fee, description=t.description,
                    executed_at=t.executed_at, transaction_date=t.transaction_date,
                )
            )

        await self._store.upsert_transactions(rows)
        return len(rows)

    async def link_to_orders(self, account: str) -> int:
        return await self._store.link_transactions_to_orders(account)


def _derive_unrealized_pnl(p: "BrokerPosition") -> Decimal | None:
    """TastyTrade's real CurrentPosition object has no unrealized-pnl field (verified against
    their OpenAPI spec) — only mark/mark-price and average-open-price. Derive it: the sign flips
    for a short position (mark rising is a loss when short), and multiplier scales options/futures
    contracts to their actual notional (a $1 move on a 100-multiplier option contract is $100)."""
    if p.mark_price is None or p.average_open_price is None:
        return None
    diff = p.mark_price - p.average_open_price
    if (p.quantity_direction or "").strip().lower() == "short":
        diff = -diff
    return diff * p.quantity * p.multiplier


def _apply_effect(magnitude: Decimal | None, effect: str | None) -> Decimal | None:
    """A (non-negative) magnitude + a Credit/Debit/None effect string -> one signed Decimal —
    TastyTrade's own value/value-effect convention (their docs example pairs effect=="None" with
    magnitude==0). A missing/unrecognized effect leaves the magnitude's own sign untouched;
    only "Debit" flips it negative."""
    if magnitude is None:
        return None
    if (effect or "").strip().lower() == "debit":
        return -magnitude
    return magnitude


class PositionRepository(_Repo):
    def __init__(self, store: "LedgerStore", *, resolver: "SecurityResolver") -> None:
        super().__init__(store)
        self._resolver = resolver
        self._securities = SecurityRepository(store)

    async def upsert(self, positions: "list[BrokerPosition]", *, account: str) -> int:
        """sync_positions core: upsert on (account, security_id). Market-data fields (quantity,
        prices, P&L) always take the broker's latest snapshot; attribution fields the broker
        snapshot can't know (strategy_id, opening_order_id, trade_group_id, position_opened_at —
        NULL for broker positions per docs/schema.md) are preserved from any existing row rather
        than reset to NULL on every re-sync."""
        if not positions:
            return 0

        seen_security_ids: set[str] = set()
        rows: list[PositionRow] = []
        for p in positions:
            resolved = self._resolver.resolve(p.symbol, p.instrument_type)
            if resolved.security_id not in seen_security_ids:
                await self._securities.upsert(resolved, tt_symbol=p.symbol)
                seen_security_ids.add(resolved.security_id)

            existing = await self._store.get_position(account, resolved.security_id)
            rows.append(
                PositionRow(
                    account=account, security_id=resolved.security_id,
                    quantity=p.quantity, quantity_direction=p.quantity_direction,
                    average_open_price=p.average_open_price, mark_price=p.mark_price,
                    close_price=p.close_price, unrealized_pnl=_derive_unrealized_pnl(p),
                    realized_day_gain=_apply_effect(p.realized_day_gain, p.realized_day_gain_effect),
                    multiplier=p.multiplier,
                    expires_at=p.expires_at,
                    strategy_id=existing.strategy_id if existing else None,
                    opening_order_id=existing.opening_order_id if existing else None,
                    trade_group_id=existing.trade_group_id if existing else None,
                    position_opened_at=existing.position_opened_at if existing else None,
                )
            )

        await self._store.upsert_positions(rows)
        return len(rows)


class TradeGroupRepository(_Repo):
    async def get(self, group_id: str) -> "TradeGroupRow | None":
        raise NotImplementedError

    async def unified(self, f: "TradeFilter") -> "list[TradeRow]":
        raise NotImplementedError


class BalanceRepository(_Repo):
    async def record(self, msg: "BalanceMessage", *, account: str, source: str = "stream") -> None:
        """Persist one balance snapshot (idempotent on ``(account, captured_at, source)``).
        ``captured_at`` falls back to now for messages that carry no timestamp."""
        await self._store.upsert_balance_snapshot(
            BalanceSnapshotRow(
                account=account,
                captured_at=msg.captured_at or datetime.now(UTC),
                source=source,
                net_liquidating_value=msg.net_liquidating_value,
                cash_balance=msg.cash_balance,
                equity_buying_power=msg.equity_buying_power,
                derivative_buying_power=msg.derivative_buying_power,
                maintenance_requirement=msg.maintenance_requirement,
                pending_cash=msg.pending_cash,
                day_trading_buying_power=msg.day_trading_buying_power,
                raw=msg.raw,
            )
        )


__all__ = [
    "ensure_account", "SecurityRepository", "OrderRepository", "TransactionRepository",
    "PositionRepository", "TradeGroupRepository", "BalanceRepository", "map_order_status",
    "apply_fill_event", "order_level_fill_fields",
]
