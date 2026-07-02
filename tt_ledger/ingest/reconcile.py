"""Reconcile broker activity into trade_groups (docs/ingestion.md → Reconcile).

Idempotent; NEVER touches a ``manually_attributed`` group's *membership*. Steps: link
transactions→order by tt_order_id; group ungrouped by (account, executed_at); route each cluster:

* **closing** activity (``* to Close`` trades, or ``Receive Deliver``
  expiration/assignment/exercise rows) that offsets an OPEN group's legs attaches to THAT group
  and emits the matching lifecycle event (``partial_exit`` / ``full_exit`` / ``expiration`` /
  ``assignment`` / ``exercise``); a fully-offset group's status flips
  (closed/expired/assigned/exercised — ``mixed`` when causes differ) and its cash-basis
  ``realized_pnl`` (signed net across all member transactions, fees netted) is stamped.
* **opening** activity creates a new trade_group (origin=broker, review_status=NEEDS_REVIEW,
  ENTRY event) — the original behavior.
* a cluster that does BOTH against the same underlying is a **roll**: the closes attach to the
  old group (+ ``roll`` event with ``rolled_to_group_id``), the opens become the new group. A
  close-cluster and an open-cluster within ``_ROLL_TOLERANCE`` of each other (same underlying,
  same option type, same quantity) are linked the same way in a post-pass.

Idempotency + the manually_attributed guarantee both fall out of one design choice: a
transaction is a reconcile *candidate* only while its ``trade_group_id`` is still ``None``.
Once any group (manually attributed or not) claims a transaction, it never becomes a candidate
again — there's no separate "is this group protected?" check needed. (Closing activity may
*attach to* a manually-attributed group — that records the group's own lifecycle, it doesn't
re-attribute anything.)
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, replace
from datetime import date, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from ..enums import Origin, ReviewStatus, StrategyType, TradeGroupEventType, TradeGroupStatus
from ..rows import ActivityFilter, EventRow, SyncResult, TradeFilter, TradeGroupRow

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..rows import ActivityRow
    from ..store import LedgerStore

# Transactions executed within this window of each other cluster into one trade_group — covers
# both a single multi-leg order's near-simultaneous fills and a multi-order strategy a human
# legs into by hand (docs/ingestion.md edge case: "several tt_order_ids executed together").
_GROUP_TOLERANCE = timedelta(seconds=5)

# A close-cluster followed by an open-cluster inside this window (same underlying, option type,
# quantity) is a roll legged in as two orders — mirrors the host platform's heuristic.
_ROLL_TOLERANCE = timedelta(seconds=60)

_OPTION_PRODUCT_TYPES = {"OS", "OI", "OF"}

_CLOSING_ACTIONS = {"Sell to Close", "Buy to Close"}
_OPENING_ACTIONS = {"Buy to Open", "Sell to Open"}

# Receive Deliver sub-types that terminate a position without a trade — these transactions carry
# no order-id, so they're admitted as candidates on the sub-type alone.
_RD_EVENT_BY_SUBTYPE = {
    "Expiration": TradeGroupEventType.EXPIRATION,
    "Assignment": TradeGroupEventType.ASSIGNMENT,
    "Cash Settled Assignment": TradeGroupEventType.ASSIGNMENT,
    "Exercise": TradeGroupEventType.EXERCISE,
    "Cash Settled Exercise": TradeGroupEventType.EXERCISE,
}

# TradeGroupStatus a fully-offset group lands on, by the single event type that closed it;
# more than one distinct cause -> MIXED.
_STATUS_BY_EVENT = {
    TradeGroupEventType.FULL_EXIT: TradeGroupStatus.CLOSED,
    TradeGroupEventType.PARTIAL_EXIT: TradeGroupStatus.CLOSED,
    TradeGroupEventType.EXPIRATION: TradeGroupStatus.EXPIRED,
    TradeGroupEventType.ASSIGNMENT: TradeGroupStatus.ASSIGNED,
    TradeGroupEventType.EXERCISE: TradeGroupStatus.EXERCISED,
}


def _is_nontrade_close(row) -> bool:  # noqa: ANN001 -- ActivityRow | TxnRow (duck-typed)
    return row.transaction_type == "Receive Deliver" and row.transaction_sub_type in _RD_EVENT_BY_SUBTYPE


def _is_closing(row) -> bool:  # noqa: ANN001
    return row.action in _CLOSING_ACTIONS or _is_nontrade_close(row)


def _is_opening(row) -> bool:  # noqa: ANN001
    return row.action in _OPENING_ACTIONS


def _signed_net(row) -> Decimal:  # noqa: ANN001
    """``net_value`` with its effect applied: TT sends a magnitude + Credit/Debit; fixtures and
    some feeds carry an already-signed value with no effect — only an explicit Debit negates."""
    net = row.net_value or Decimal("0")
    return -net if getattr(row, "net_value_effect", None) == "Debit" else net


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
            # self-heal the reverse edge: orders whose member transactions were grouped in an
            # earlier pass but whose own trade_group_id is unset (e.g. rows re-synced before
            # the preserve-on-resync fix, or created after their transactions were grouped)
            await store.link_orders_to_groups(acct)

            activity = await store.account_activity(ActivityFilter(account=acct, start=since))
            candidates = [
                a for a in activity
                if a.trade_group_id is None and (a.order_id is not None or _is_nontrade_close(a))
            ]

            open_groups = await _load_open_groups(store, acct)
            exits: list[_AppliedExit] = []   # for the cross-cluster roll post-pass
            created: list[_CreatedGroup] = []

            for cluster in _cluster_by_time(candidates):
                cluster = await _apply_intent_rows(store, open_groups, cluster, dry_run=dry_run)
                if not cluster:
                    continue
                buckets, rest = _route_cluster(open_groups, cluster)

                new_pk: int | None = None
                if rest:
                    if not dry_run:
                        new_pk = await _create_trade_group(store, acct, rest)
                        created.append(_CreatedGroup(pk=new_pk, rows=list(rest)))
                        open_groups.append(_OpenGroup(pk=new_pk, rows=list(rest)))
                    result.trade_groups += 1

                if not dry_run:
                    for group, rows in buckets:
                        # closes + opens in ONE cluster against the same underlying = a roll
                        rolled_to = new_pk if new_pk is not None and _same_underlying(rows, rest) else None
                        applied = await _apply_exit(store, group, rows, rolled_to_pk=rolled_to)
                        exits.append(applied)
                        if applied.fully_closed:
                            open_groups.remove(group)

            if not dry_run:
                await _link_cross_cluster_rolls(store, exits, created)
        except Exception as exc:  # noqa: BLE001 - one account's failure must not abort the rest
            result.errors.append(f"{acct}: {exc}")
            logger.warning("reconcile failed for %s: %s", acct, exc)

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


# --------------------------------------------------------------------- exit / roll machinery


@dataclass(eq=False)
class _OpenGroup:
    """An OPEN trade_group + its member rows (``TxnRow`` when loaded from the store,
    ``ActivityRow`` when created earlier in this same run — the two are duck-type-compatible
    for every field the quantity/premium math reads)."""

    pk: int
    rows: list

    def security_ids(self) -> set[str]:
        return {r.security_id for r in self.rows if r.security_id}


@dataclass
class _CreatedGroup:
    pk: int
    rows: list


@dataclass
class _AppliedExit:
    group: "_OpenGroup"
    rows: list
    fully_closed: bool
    rolled: bool
    closed_at: "object"  # datetime | None


async def _load_open_groups(store: "LedgerStore", account: str) -> "list[_OpenGroup]":
    groups: list[_OpenGroup] = []
    for trade in await store.unified_trades(TradeFilter(account=account, status=TradeGroupStatus.OPEN.value)):
        pk = await store.get_trade_group_id(trade.group_id)
        if pk is None:
            continue
        groups.append(_OpenGroup(pk=pk, rows=list(await store.get_group_transactions(pk))))
    return groups


async def _apply_intent_rows(
    store: "LedgerStore", open_groups: "list[_OpenGroup]", cluster: "list[ActivityRow]", *, dry_run: bool,
) -> "list[ActivityRow]":
    """Route rows whose ORDER was pre-attributed to a trade_group at submit time
    (``record_order(trade_group=...)``): opening fills attach as members (their group's ENTRY
    event already exists) and refresh the group's fill-derived financials; closing fills go
    through the normal exit machinery against that group. Returns the rows reconcile still
    needs to route heuristically."""
    intent: dict[int, list["ActivityRow"]] = {}
    remaining: list["ActivityRow"] = []
    for row in cluster:
        if row.order_trade_group_id is not None:
            intent.setdefault(row.order_trade_group_id, []).append(row)
        else:
            remaining.append(row)
    if dry_run or not intent:
        return remaining

    for pk, rows in intent.items():
        group = await _group_for_pk(store, open_groups, pk)
        if group is None:  # the order points at a group that no longer exists -- fall back
            remaining.extend(rows)
            continue
        opening = [r for r in rows if not _is_closing(r)]
        closing = [r for r in rows if _is_closing(r)]
        if opening:
            await store.attach_transactions_to_trade_group([r.tt_transaction_id for r in opening], group.pk)
            group.rows.extend(opening)
            await _refresh_group_financials(store, group)
        if closing:
            applied = await _apply_exit(store, group, closing, rolled_to_pk=None)
            if applied.fully_closed and group in open_groups:
                open_groups.remove(group)
    return remaining


async def _group_for_pk(store: "LedgerStore", open_groups: "list[_OpenGroup]", pk: int) -> "_OpenGroup | None":
    for g in open_groups:
        if g.pk == pk:
            return g
    tg = await store.get_trade_group_by_id(pk)
    if tg is None:
        return None
    group = _OpenGroup(pk=pk, rows=list(await store.get_group_transactions(pk)))
    open_groups.append(group)
    return group


async def _refresh_group_financials(store: "LedgerStore", group: "_OpenGroup") -> None:
    """Refine an intent group's fill-derived fields from its actual member transactions.
    Attribution and submit-time intent (strategy_type, targets, max profit/loss) are never
    touched; sparse fields (underlying/security_id/executed_at) fill in only when unset."""
    tg = await store.get_trade_group_by_id(group.pk)
    if tg is None:
        return
    fields = await compute_group_fields(store, group.rows)
    await store.upsert_trade_group(
        replace(
            tg,
            total_premium=fields["total_premium"],
            total_fees=fields["total_fees"],
            quantity=fields["quantity"],
            leg_count=fields["leg_count"],
            underlying=tg.underlying or fields["underlying"],
            security_id=tg.security_id or fields["security_id"],
            strategy_type=tg.strategy_type or fields["strategy_type"],
            executed_at=fields["executed_at"] or tg.executed_at,
        )
    )


def _route_cluster(
    open_groups: "list[_OpenGroup]", cluster: "list[ActivityRow]",
) -> "tuple[list[tuple[_OpenGroup, list[ActivityRow]]], list[ActivityRow]]":
    """Split a cluster into per-open-group closing buckets + the rest (which opens a new group).

    A closing row belongs to the first open group holding its security; a closing row with no
    matching open group (history doesn't reach the entry) falls into ``rest`` like any opener.
    """
    buckets: list[tuple[_OpenGroup, list["ActivityRow"]]] = []
    by_id: dict[int, list["ActivityRow"]] = {}
    rest: list["ActivityRow"] = []
    for row in cluster:
        group = None
        if _is_closing(row) and row.security_id is not None:
            group = next((g for g in open_groups if row.security_id in g.security_ids()), None)
        if group is None:
            rest.append(row)
            continue
        if id(group) not in by_id:
            bucket: list["ActivityRow"] = []
            by_id[id(group)] = bucket
            buckets.append((group, bucket))
        by_id[id(group)].append(row)
    return buckets, rest


def _fully_closed(rows: list) -> bool:
    """Every security the group opened has been offset by at least as much closing quantity."""
    opened: dict[str, Decimal] = {}
    closed: dict[str, Decimal] = {}
    for r in rows:
        if r.security_id is None or r.quantity is None:
            continue
        if _is_opening(r):
            opened[r.security_id] = opened.get(r.security_id, Decimal("0")) + abs(r.quantity)
        elif _is_closing(r):
            closed[r.security_id] = closed.get(r.security_id, Decimal("0")) + abs(r.quantity)
    if not opened:
        return False
    return all(closed.get(sec, Decimal("0")) >= qty for sec, qty in opened.items())


def _close_causes(rows: list) -> "set[TradeGroupStatus]":
    causes: set[TradeGroupStatus] = set()
    for r in rows:
        if r.action in _CLOSING_ACTIONS:
            causes.add(TradeGroupStatus.CLOSED)
        elif _is_nontrade_close(r):
            causes.add(_STATUS_BY_EVENT[_RD_EVENT_BY_SUBTYPE[r.transaction_sub_type]])
    return causes


def _same_underlying(close_rows: list, open_rows: list) -> bool:
    close_unders = {r.underlying for r in close_rows if r.underlying}
    open_unders = {r.underlying for r in open_rows if r.underlying}
    return bool(close_unders & open_unders)


async def _apply_exit(
    store: "LedgerStore", group: "_OpenGroup", rows: "list[ActivityRow]", *, rolled_to_pk: int | None,
) -> "_AppliedExit":
    """Attach closing rows to their open group, emit lifecycle events, and flip the group's
    status (+ cash-basis realized_pnl) once fully offset."""
    await store.attach_transactions_to_trade_group([r.tt_transaction_id for r in rows], group.pk)
    group.rows.extend(rows)

    fully_closed = _fully_closed(group.rows)
    event_ats = [r.executed_at for r in rows if r.executed_at is not None]
    event_at = max(event_ats) if event_ats else None

    trade_rows = [r for r in rows if r.action in _CLOSING_ACTIONS]
    if trade_rows:
        event_type = TradeGroupEventType.FULL_EXIT if fully_closed else TradeGroupEventType.PARTIAL_EXIT
        await store.add_trade_group_event(
            EventRow(
                trade_group_id=group.pk, event_type=event_type.value,
                quantity_change=-sum(abs(r.quantity) for r in trade_rows if r.quantity is not None),
                premium_change=sum((_signed_net(r) for r in trade_rows), Decimal("0")),
                event_at=event_at,
            )
        )

    rd_rows_by_event: dict[TradeGroupEventType, list] = {}
    for r in rows:
        if _is_nontrade_close(r):
            rd_rows_by_event.setdefault(_RD_EVENT_BY_SUBTYPE[r.transaction_sub_type], []).append(r)
    for event_type, rd_rows in rd_rows_by_event.items():
        rd_ats = [r.executed_at for r in rd_rows if r.executed_at is not None]
        await store.add_trade_group_event(
            EventRow(
                trade_group_id=group.pk, event_type=event_type.value,
                quantity_change=-sum(abs(r.quantity) for r in rd_rows if r.quantity is not None),
                premium_change=sum((_signed_net(r) for r in rd_rows), Decimal("0")),
                event_at=max(rd_ats) if rd_ats else event_at,
            )
        )

    if rolled_to_pk is not None:
        await store.add_trade_group_event(
            EventRow(
                trade_group_id=group.pk, event_type=TradeGroupEventType.ROLL.value,
                event_at=event_at, rolled_to_group_id=rolled_to_pk,
            )
        )

    if fully_closed:
        causes = _close_causes(group.rows)
        status = causes.pop() if len(causes) == 1 else TradeGroupStatus.MIXED
        tg = await store.get_trade_group_by_id(group.pk)
        if tg is not None:
            # cash-basis realized P&L: signed net across every member transaction (fees netted
            # by the broker's own net-value); position-level cost-basis P&L stays replay's job.
            realized = sum((_signed_net(r) for r in group.rows), Decimal("0"))
            await store.upsert_trade_group(
                replace(tg, status=status.value, closed_at=event_at, realized_pnl=realized)
            )

    return _AppliedExit(
        group=group, rows=rows, fully_closed=fully_closed,
        rolled=rolled_to_pk is not None, closed_at=event_at,
    )


async def _link_cross_cluster_rolls(
    store: "LedgerStore", exits: "list[_AppliedExit]", created: "list[_CreatedGroup]",
) -> None:
    """A close-cluster and an open-cluster within ``_ROLL_TOLERANCE`` (same underlying, same
    option type, same quantity) are a roll legged in as two orders — link them."""
    for ex in exits:
        if ex.rolled or ex.closed_at is None:
            continue
        for cg in created:
            open_ats = [r.executed_at for r in cg.rows if r.executed_at is not None]
            if not open_ats:
                continue
            opened_at = min(open_ats)
            if not (timedelta(0) <= opened_at - ex.closed_at <= _ROLL_TOLERANCE):
                continue
            if not _same_underlying(ex.rows, cg.rows):
                continue
            if _max_quantity(ex.rows) != _max_quantity(cg.rows):
                continue
            if not await _same_option_types(store, ex.rows, cg.rows):
                continue
            await store.add_trade_group_event(
                EventRow(
                    trade_group_id=ex.group.pk, event_type=TradeGroupEventType.ROLL.value,
                    event_at=opened_at, rolled_to_group_id=cg.pk,
                )
            )
            ex.rolled = True
            break


def _max_quantity(rows: list) -> Decimal:
    return max((abs(r.quantity) for r in rows if r.quantity is not None), default=Decimal("0"))


async def _same_option_types(store: "LedgerStore", a_rows: list, b_rows: list) -> bool:
    async def types(rows: list) -> set:
        out = set()
        for r in rows:
            if r.security_id is None:
                continue
            sec = await store.get_security(r.security_id)
            if sec is not None and sec.option_type is not None:
                out.add(sec.option_type)
        return out

    a_types, b_types = await types(a_rows), await types(b_rows)
    # non-options (empty sets) match, mirroring the host heuristic's leniency
    return a_types == b_types if (a_types and b_types) else True


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
    total_premium = sum((_signed_net(row) for row in cluster if row.net_value is not None), Decimal("0"))
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


async def _create_trade_group(store: "LedgerStore", account: str, cluster: "list[ActivityRow]") -> int:
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
    return group_pk


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
