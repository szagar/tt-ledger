"""Reconcile broker activity into trade_groups (docs/ingestion.md → Reconcile).

Idempotent; NEVER touches a ``manually_attributed`` group. Steps: link transactions→order by
tt_order_id; group ungrouped by (account, executed_at); classify strategy_type; create the
trade_group (origin=broker, review_status=NEEDS_REVIEW, ENTRY event, realized P&L).

Idempotency + the manually_attributed guarantee both fall out of one design choice: a
transaction is a reconcile *candidate* only while its ``trade_group_id`` is still ``None``.
Once any group (manually attributed or not) claims a transaction, it never becomes a candidate
again — there's no separate "is this group protected?" check needed.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, replace
from datetime import date, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from ..enums import Origin, ReviewStatus, StrategyType, TradeGroupEventType, TradeGroupStatus
from ..rows import ActivityFilter, EventRow, SyncResult, TradeGroupRow

if TYPE_CHECKING:
    from ..rows import ActivityRow
    from ..store import LedgerStore

# Transactions executed within this window of each other cluster into one trade_group — covers
# both a single multi-leg order's near-simultaneous fills and a multi-order strategy a human
# legs into by hand (docs/ingestion.md edge case: "several tt_order_ids executed together").
_GROUP_TOLERANCE = timedelta(seconds=5)

_OPTION_PRODUCT_TYPES = {"OS", "OI", "OF"}


async def reconcile(
    store: "LedgerStore",
    account: str | None = None,
    *,
    since: date | None = None,
    dry_run: bool = False,
) -> "SyncResult":
    """Link → group → classify → create trade_groups, for ``account`` (every account with
    activity in range when ``account`` is omitted).

    ``dry_run`` skips the trade_group-creation writes (upsert, attach, attribution, event) but
    still runs the link step — deterministic exact-match linking is always safe to apply, and
    skipping it would make the dry-run preview undercount candidates.
    """
    result = SyncResult()
    accounts = [account] if account is not None else await _accounts_with_activity(store, since)

    for acct in accounts:
        try:
            await store.link_transactions_to_orders(acct)

            activity = await store.account_activity(ActivityFilter(account=acct, start=since))
            candidates = [a for a in activity if a.order_id is not None and a.trade_group_id is None]

            for cluster in _cluster_by_time(candidates):
                if not dry_run:
                    await _create_trade_group(store, acct, cluster)
                result.trade_groups += 1
        except Exception as exc:  # noqa: BLE001 - one account's failure must not abort the rest
            result.errors.append(f"{acct}: {exc}")

    return result


async def _accounts_with_activity(store: "LedgerStore", since: date | None) -> list[str]:
    activity = await store.account_activity(ActivityFilter(start=since))
    return sorted({row.account for row in activity})


def _cluster_by_time(rows: "list[ActivityRow]") -> "list[list[ActivityRow]]":
    dated = sorted((r for r in rows if r.executed_at is not None), key=lambda r: r.executed_at)
    undated = [r for r in rows if r.executed_at is None]  # can't window these -- each is its own group

    clusters: list[list["ActivityRow"]] = []
    for row in dated:
        if clusters and (row.executed_at - clusters[-1][-1].executed_at) <= _GROUP_TOLERANCE:
            clusters[-1].append(row)
        else:
            clusters.append([row])
    clusters.extend([row] for row in undated)
    return clusters


async def compute_group_fields(store: "LedgerStore", cluster: "list[ActivityRow]") -> dict:
    """A cluster of transactions -> the TradeGroupRow fields derivable purely from them
    (underlying, security_id, strategy_type, leg_count, total_premium, total_fees, quantity,
    executed_at). Shared between reconcile's group-creation and remap's regroup recompute —
    both need to (re)derive a group's financials/classification from its member transactions.
    """
    distinct_security_ids = list(dict.fromkeys(row.security_id for row in cluster if row.security_id))
    securities = {}
    for security_id in distinct_security_ids:
        sec = await store.get_security(security_id)
        if sec is not None:
            securities[security_id] = sec

    legs = [
        LegInfo(
            product_type=securities[row.security_id].product_type,
            option_type=securities[row.security_id].option_type,
            expiry=securities[row.security_id].expiry,
            strike=securities[row.security_id].strike,
            quantity=row.quantity or Decimal("0"),
        )
        for row in cluster
        if row.security_id in securities
    ]

    underlying = next((row.underlying for row in cluster if row.underlying), None)
    total_premium = sum((row.net_value for row in cluster if row.net_value is not None), Decimal("0"))
    total_fees = sum(
        (
            (row.commission or Decimal("0")) + (row.clearing_fees or Decimal("0")) + (row.regulatory_fees or Decimal("0"))
            for row in cluster
        ),
        Decimal("0"),
    )
    quantities = [abs(row.quantity) for row in cluster if row.quantity is not None]
    quantity = max(quantities) if quantities else None
    executed_ats = [row.executed_at for row in cluster if row.executed_at is not None]
    executed_at = min(executed_ats) if executed_ats else None

    return {
        "underlying": underlying,
        "security_id": (distinct_security_ids[0] if len(distinct_security_ids) == 1 else None),
        "strategy_type": detect_strategy_type(legs),
        "leg_count": max(len(distinct_security_ids), 1),
        "total_premium": total_premium,
        "total_fees": total_fees,
        "quantity": quantity,
        "executed_at": executed_at,
    }


async def _create_trade_group(store: "LedgerStore", account: str, cluster: "list[ActivityRow]") -> None:
    fields = await compute_group_fields(store, cluster)
    total_premium, quantity, executed_at = fields["total_premium"], fields["quantity"], fields["executed_at"]

    group_pk = await store.upsert_trade_group(
        TradeGroupRow(
            group_id=str(uuid.uuid4()), account=account, origin=Origin.BROKER,
            review_status=ReviewStatus.NEEDS_REVIEW, status=TradeGroupStatus.OPEN.value,
            **fields,
        )
    )

    await store.attach_transactions_to_trade_group([row.tt_transaction_id for row in cluster], group_pk)

    tt_order_ids = list(dict.fromkeys(row.tt_order_id for row in cluster if row.tt_order_id))
    primary_order_pk = None
    if tt_order_ids:
        orders_to_update = []
        for tt_order_id in tt_order_ids:
            existing_order = await store.get_order(tt_order_id)
            if existing_order is not None:
                orders_to_update.append(replace(existing_order, trade_group_id=group_pk))
        if orders_to_update:
            order_pks = await store.upsert_orders(orders_to_update)
            primary_order_pk = order_pks[0]

    await store.add_trade_group_event(
        EventRow(
            trade_group_id=group_pk, event_type=TradeGroupEventType.ENTRY.value,
            quantity_change=(quantity or Decimal("0")), premium_change=total_premium,
            event_at=executed_at, order_id=primary_order_pk,
        )
    )


@dataclass
class LegInfo:
    """The decomposed-security detail ``detect_strategy_type`` classifies on — one per group leg."""

    product_type: str
    option_type: str | None
    expiry: date | None
    strike: Decimal | None
    quantity: Decimal


def detect_strategy_type(legs: list[LegInfo]) -> str:
    """Classify legs into a StrategyType value.

    Best-effort pattern matching on the canonical shapes (docs/schema.md's StrategyType enum),
    NOT exhaustive derivative-strategy detection — it doesn't weigh buy/sell bias, so anything
    that doesn't clearly match a canonical shape (e.g. a broken-wing butterfly, an unusual
    custom ratio) falls through to ``custom`` rather than risk a wrong specific label.
    """
    if not legs:
        return StrategyType.CUSTOM.value
    if len(legs) == 1:
        return StrategyType.FUTURE.value if legs[0].product_type == "F" else StrategyType.SINGLE.value

    option_legs = [leg for leg in legs if leg.product_type in _OPTION_PRODUCT_TYPES]
    other_legs = [leg for leg in legs if leg.product_type not in _OPTION_PRODUCT_TYPES]

    if other_legs and option_legs:
        if len(other_legs) == 1 and len(option_legs) == 2 and _same({leg.expiry for leg in option_legs}) \
                and {leg.option_type for leg in option_legs} == {"P", "C"}:
            return StrategyType.COLLAR.value
        if len(other_legs) == 1 and len(option_legs) == 1:
            return StrategyType.COVERED.value
        return StrategyType.CUSTOM.value

    if not option_legs:
        if all(leg.product_type == "F" for leg in legs):
            return StrategyType.FUTURE_SPREAD.value
        return StrategyType.CUSTOM.value

    if len(option_legs) == 2:
        return _classify_two_leg(*option_legs)
    if len(option_legs) == 3:
        return _classify_three_leg(option_legs)
    if len(option_legs) == 4:
        return _classify_four_leg(option_legs)
    return StrategyType.CUSTOM.value


def _same(values: set) -> bool:  # noqa: ANN001
    return len(values) == 1


def _classify_two_leg(a: LegInfo, b: LegInfo) -> str:
    same_expiry = a.expiry == b.expiry
    same_strike = a.strike == b.strike
    same_type = a.option_type == b.option_type

    if same_expiry and same_type and a.quantity != b.quantity:
        return StrategyType.RATIO.value
    if same_expiry and same_strike and not same_type:
        return StrategyType.STRADDLE.value
    if same_expiry and not same_strike and not same_type:
        return StrategyType.STRANGLE.value
    if same_expiry and same_type and not same_strike:
        return StrategyType.VERTICAL.value
    if not same_expiry and same_strike and same_type:
        return StrategyType.CALENDAR.value
    if not same_expiry and not same_strike and same_type:
        return StrategyType.DIAGONAL.value
    return StrategyType.CUSTOM.value


def _classify_three_leg(legs: list[LegInfo]) -> str:
    if _same({leg.expiry for leg in legs}) and _same({leg.option_type for leg in legs}):
        return StrategyType.BUTTERFLY.value
    return StrategyType.CUSTOM.value


def _classify_four_leg(legs: list[LegInfo]) -> str:
    if not _same({leg.expiry for leg in legs}):
        return StrategyType.CUSTOM.value
    types = [leg.option_type for leg in legs]
    strikes = [leg.strike for leg in legs]
    if types.count("P") == 2 and types.count("C") == 2:
        return StrategyType.IRON_CONDOR.value if len(set(strikes)) == 4 else StrategyType.IRON_BUTTERFLY.value
    if _same(set(types)):  # all calls or all puts, 4 distinct strikes
        return StrategyType.CONDOR.value
    return StrategyType.CUSTOM.value
