"""Operator-driven attribution / remap (docs/ingestion.md → Remap).

Each writes an ADJUSTMENT event and flips ``manually_attributed`` / ``review_status`` so the
reconciler never overwrites the edit (reconcile only ever touches transactions whose
``trade_group_id`` is still ``None`` — once remap/regroup claims one, it's permanently excluded
from automatic re-grouping, attributed or not).
"""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ..enums import Origin, ReviewStatus, TradeGroupEventType, TradeGroupStatus
from ..rows import ActivityFilter, EventRow, OrderFilter, TradeGroupRow, trade_group_to_row
from .reconcile import compute_group_fields, reconcile

if TYPE_CHECKING:
    from ..rows import TradeRow
    from ..store import LedgerStore


async def remap_trade_group(
    store: "LedgerStore",
    group_id: str,
    *,
    strategy: int | None = None,
    bot: str | None = None,
    signal: str | None = None,
    strategy_type: str | None = None,
    reviewed_by: str,
) -> "TradeRow":
    """Set attribution; cascade to the group's orders (and its position, when the group maps to
    exactly one security); flip manually_attributed + CONFIRMED; write an ADJUSTMENT event."""
    tg = await store.get_trade_group(group_id)
    if tg is None:
        raise ValueError(f"trade group {group_id!r} not found")

    now = datetime.now(UTC)
    updated = replace(
        tg,
        strategy_id=(strategy if strategy is not None else tg.strategy_id),
        bot_name=(bot if bot is not None else tg.bot_name),
        signal_id=(signal if signal is not None else tg.signal_id),
        strategy_type=(strategy_type if strategy_type is not None else tg.strategy_type),
        manually_attributed=True, review_status=ReviewStatus.CONFIRMED,
        reviewed_by=reviewed_by, reviewed_at=now,
    )
    group_pk = await store.upsert_trade_group(updated)

    if strategy is not None:
        orders = await store.query_orders(OrderFilter(trade_group_id=group_pk))
        if orders:
            await store.upsert_orders([replace(o, strategy_id=strategy) for o in orders])
        # a multi-leg group has no single unambiguous position to cascade to -- only cascade
        # when the group maps to exactly one security.
        if updated.security_id is not None:
            position = await store.get_position(updated.account, updated.security_id)
            if position is not None:
                await store.upsert_positions(
                    [replace(position, strategy_id=strategy, trade_group_id=group_pk)]
                )

    await store.add_trade_group_event(
        EventRow(
            trade_group_id=group_pk, event_type=TradeGroupEventType.ADJUSTMENT.value,
            event_at=now, notes=f"remapped by {reviewed_by}",
        )
    )
    return trade_group_to_row(updated)


async def regroup_transactions(
    store: "LedgerStore",
    txn_ids: list[int],
    *,
    target_group_id: str | None,  # None -> new group
    reviewed_by: str,
) -> "list[TradeRow]":
    """Move transactions (by surrogate id) into ``target_group_id`` (or a brand-new group when
    omitted); recompute every affected group's (the source group(s) and the target) P&L and
    classification from what remains; ADJUSTMENT event on each."""
    if not txn_ids:
        return []

    txns = await store.get_transactions_by_id(txn_ids)
    if not txns:
        return []

    account = txns[0].account
    source_group_pks = {t.trade_group_id for t in txns if t.trade_group_id is not None}
    now = datetime.now(UTC)

    if target_group_id is None:
        target_group_pk = await store.upsert_trade_group(
            TradeGroupRow(
                group_id=str(uuid.uuid4()), account=account, origin=Origin.BROKER,
                review_status=ReviewStatus.NEEDS_REVIEW, manually_attributed=True,
                status=TradeGroupStatus.OPEN.value, reviewed_by=reviewed_by, reviewed_at=now,
            )
        )
    else:
        target_tg = await store.get_trade_group(target_group_id)
        if target_tg is None:
            raise ValueError(f"target trade group {target_group_id!r} not found")
        target_group_pk = await store.upsert_trade_group(
            replace(target_tg, manually_attributed=True, reviewed_by=reviewed_by, reviewed_at=now)
        )

    await store.move_transactions_to_group(txn_ids, target_group_pk)

    updated_groups = []
    for pk in source_group_pks | {target_group_pk}:
        tg = await store.get_trade_group_by_id(pk)
        if tg is None:
            continue
        recomputed = await _recompute_group(store, pk, tg)
        await store.add_trade_group_event(
            EventRow(
                trade_group_id=pk, event_type=TradeGroupEventType.ADJUSTMENT.value,
                event_at=now, notes=f"regrouped by {reviewed_by}",
            )
        )
        updated_groups.append(recomputed)

    return [trade_group_to_row(tg) for tg in updated_groups]


async def _recompute_group(store: "LedgerStore", trade_group_id: int, tg: TradeGroupRow) -> TradeGroupRow:
    activity = await store.account_activity(ActivityFilter(account=tg.account))
    cluster = [row for row in activity if row.trade_group_id == trade_group_id]
    fields = await compute_group_fields(store, cluster)
    updated = replace(tg, **fields)
    await store.upsert_trade_group(updated)
    return updated


async def link_order_to_group(
    store: "LedgerStore",
    tt_order_id: str,
    *,
    target_group_id: str | None,  # None -> new group
    reviewed_by: str,
) -> "TradeRow":
    """Attach an unlinked order (typically a broker-entered one, ``origin=broker``) to
    ``target_group_id``, or to a brand-new group when omitted; ADJUSTMENT event; then run a
    scoped ``reconcile`` so the order's already-synced ungrouped fills attach immediately.

    Later fills follow automatically: once ``orders.trade_group_id`` is set, every future
    fill routes through reconcile's pre-attributed-order path (``_apply_intent_rows`` — opens
    attach as members, closes run the exit machinery), the same path OMS-submitted orders
    use. The stamp survives broker resyncs of the still-working order via ``upsert_orders``'s
    preserve-if-null.

    Fills that an earlier reconcile pass already routed to some OTHER group are not moved —
    that is ``regroup``'s job (transactions with a ``trade_group_id`` are permanently excluded
    from automatic re-grouping).
    """
    order = await store.get_order(tt_order_id)
    if order is None:
        raise ValueError(f"order {tt_order_id!r} not found")
    if order.trade_group_id is not None:
        raise ValueError(
            f"order {tt_order_id!r} is already linked to trade group pk={order.trade_group_id}"
            " — move its transactions with regroup instead"
        )

    now = datetime.now(UTC)
    if target_group_id is None:
        group_pk = await store.upsert_trade_group(
            TradeGroupRow(
                group_id=str(uuid.uuid4()), account=order.account, origin=Origin.BROKER,
                review_status=ReviewStatus.NEEDS_REVIEW, manually_attributed=True,
                status=TradeGroupStatus.OPEN.value, reviewed_by=reviewed_by, reviewed_at=now,
                underlying=order.underlying, security_id=order.security_id,
            )
        )
    else:
        target_tg = await store.get_trade_group(target_group_id)
        if target_tg is None:
            raise ValueError(f"target trade group {target_group_id!r} not found")
        if target_tg.account != order.account:
            raise ValueError(
                f"target trade group {target_group_id!r} belongs to account "
                f"{target_tg.account!r}, order {tt_order_id!r} to {order.account!r}"
            )
        group_pk = await store.upsert_trade_group(
            replace(target_tg, manually_attributed=True, reviewed_by=reviewed_by, reviewed_at=now)
        )

    await store.upsert_orders([replace(order, trade_group_id=group_pk)])

    await store.add_trade_group_event(
        EventRow(
            trade_group_id=group_pk, event_type=TradeGroupEventType.ADJUSTMENT.value,
            event_at=now, notes=f"order {tt_order_id} linked by {reviewed_by}",
        )
    )

    # Attach any fills that already synced ungrouped; future fills follow on every sync.
    await reconcile(store, order.account)

    tg = await store.get_trade_group_by_id(group_pk)
    if tg is None:  # pragma: no cover - the group was just written
        raise ValueError(f"trade group pk={group_pk} vanished during link")
    return trade_group_to_row(tg)


async def dismiss_trade_group(store: "LedgerStore", group_id: str, *, reviewed_by: str) -> "TradeRow":
    """review_status=IGNORED (transfers / non-trades) -- leaves the review queue without
    attribution (``manually_attributed`` stays untouched: dismissing isn't attributing)."""
    tg = await store.get_trade_group(group_id)
    if tg is None:
        raise ValueError(f"trade group {group_id!r} not found")

    now = datetime.now(UTC)
    updated = replace(tg, review_status=ReviewStatus.IGNORED, reviewed_by=reviewed_by, reviewed_at=now)
    group_pk = await store.upsert_trade_group(updated)

    await store.add_trade_group_event(
        EventRow(
            trade_group_id=group_pk, event_type=TradeGroupEventType.ADJUSTMENT.value,
            event_at=now, notes=f"dismissed by {reviewed_by}",
        )
    )
    return trade_group_to_row(updated)
