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
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from ..enums import Origin, ReviewStatus, StrategyType, TradeGroupEventType, TradeGroupStatus
from ..rows import ActivityFilter, ActivityRow, EventRow, SyncResult, TradeFilter, TradeGroupRow, TxnRow
from .replay import net_open_quantities

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
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


_BARE_ACTIONS = {"Buy", "Sell"}  # futures trade without open/close intent -- direction from context


async def synthesize_lapsed_settlements(
    store: "LedgerStore",
    account: str,
    *,
    dry_run: bool = False,
) -> "list[ActivityRow]":
    """Synthesize the MISSING broker settlement row for open lots past expiry.

    Some expirations never produce a broker transaction (futures options that
    just vanish; corporate-action re-symbols whose settlement arrives under a
    different security_id). Without one, transaction-driven accounting — group
    nets, group status — can never see the close, even though replay's lapse
    backstop flattens the *position*. This pass recreates the row the broker
    should have sent, and everything downstream consumes it like any real
    ``Receive Deliver / Expiration``.

    Rules (deterministic, idempotent):
    * open lots come from ``net_open_quantities`` over the account's FULL
      transaction history — never the positions table, which replay's lapse
      backstop has already flattened for exactly these lots.
    * clock = the account's own latest transaction, never wall-clock (same
      rule as replay's ``_lapse_expired_lot``); a lot lapses only once the
      account has activity a full day past the contract's expiry.
    * id = ``lapse-<account>-<security_id>`` — re-runs upsert the same row.
    * a real settlement (or a prior synthetic one) already nets the lot to
      zero, so late-arriving broker truth wins and re-runs no-op — no
      separate collision check needed.
    * only lots some OPEN group still holds synthesize — the feature exists
      to close stuck groups, and a settlement with no group to land in would
      just orphan into a junk NEEDS_REVIEW group (position-level flattening
      is already replay's backstop). A lot whose entry hasn't been grouped
      yet synthesizes on the NEXT pass, once its group exists.
    * price 0 at expiry 21:15Z; replay's ``_effective_delta_price`` offsets
      the open lot exactly like a feed-delivered expiration.

    Returns the synthesized rows in ``v_account_activity`` shape so the
    calling reconcile pass can admit them as candidates immediately (their
    historical ``executed_at`` would fall outside any ``since`` window).
    """
    activity = await store.account_activity(ActivityFilter(account=account))
    last_activity = max((a.executed_at for a in activity if a.executed_at is not None), default=None)
    if last_activity is None:
        return []

    held_by_open_group: set[str] = set()
    for group in await _load_open_groups(store, account):
        held_by_open_group.update(
            sid for sid, qty in _net_quantities(group.rows).items() if qty != 0
        )

    txns: list[TxnRow] = []
    rows: list[ActivityRow] = []
    for security_id, net in sorted(net_open_quantities(activity).items()):
        if net == 0 or security_id not in held_by_open_group:
            continue
        sec = await store.get_security(security_id)
        expiry = sec.expiry if sec is not None else None
        if expiry is None or last_activity.date() <= expiry + timedelta(days=1):
            continue
        txn_id = f"lapse-{account}-{security_id}"
        common = {
            "tt_transaction_id": txn_id,
            "account": account,
            "transaction_type": "Receive Deliver",
            "transaction_sub_type": "Expiration",
            "security_id": security_id,
            "underlying": sec.underlying if sec is not None else None,
            "quantity": abs(net),
            "price": Decimal("0"),
            "net_value": Decimal("0"),
            "executed_at": datetime.combine(expiry, datetime.min.time(), tzinfo=UTC).replace(
                hour=21, minute=15
            ),
        }
        txns.append(
            TxnRow(
                tt_order_id=None,
                description="synthesized lapse: contract expired with no broker settlement row",
                transaction_date=expiry,
                **common,
            )
        )
        rows.append(ActivityRow(**common))

    if txns and not dry_run:
        await store.upsert_transactions(txns)
        logger.info("synthesized %d lapse settlement(s) for %s", len(txns), account)
    return rows


def _is_nontrade_close(row) -> bool:  # noqa: ANN001 -- ActivityRow | TxnRow (duck-typed)
    return row.transaction_type == "Receive Deliver" and row.transaction_sub_type in _RD_EVENT_BY_SUBTYPE


def _is_delivery(row) -> bool:  # noqa: ANN001
    """A Receive Deliver row with a trade-like action: the position leg of an option
    assignment/exercise (the delivered future/shares), or an ACAT-style transfer. Carries no
    order-id, so it must be admitted as a candidate on its own shape."""
    return (
        row.transaction_type == "Receive Deliver"
        and row.action in (_OPENING_ACTIONS | _CLOSING_ACTIONS | _BARE_ACTIONS)
    )


def _is_closing(row) -> bool:  # noqa: ANN001
    return row.action in _CLOSING_ACTIONS or _is_nontrade_close(row)


def _action_delta(row) -> Decimal:  # noqa: ANN001
    """Signed position delta of a trade-like row (buys +, sells -)."""
    qty = abs(row.quantity or Decimal("0"))
    return -qty if (row.action or "").strip().lower().startswith("sell") else qty


def _net_quantities(rows: list) -> dict[str, Decimal]:
    """Net signed position per security from a group's member rows, walked in time order:
    trade-like rows apply their action-signed delta; settlement rows (no action) offset the
    running net toward zero; cash-only rows are ignored."""
    ordered = sorted(
        (r for r in rows if r.security_id and r.quantity is not None),
        key=lambda r: (r.executed_at is None, r.executed_at),
    )
    net: dict[str, Decimal] = {}
    for r in ordered:
        if getattr(r, "transaction_type", None) == "Money Movement":
            continue
        current = net.get(r.security_id, Decimal("0"))
        if _is_nontrade_close(r):
            if current == 0:
                continue
            qty = min(abs(r.quantity), abs(current))
            net[r.security_id] = current - qty if current > 0 else current + qty
        elif r.action:
            net[r.security_id] = current + _action_delta(r)
    return net


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
            # Self-heal: recreate settlement rows the broker never sent for
            # open lots past expiry, then admit them as candidates in this
            # same pass (their executed_at is the historical expiry, which a
            # ``since`` window would otherwise exclude).
            synthesized = await synthesize_lapsed_settlements(store, acct, dry_run=dry_run)
            if synthesized:
                result.transactions += len(synthesized)
                activity = [*activity, *synthesized]
            candidates = [
                a for a in activity
                if a.trade_group_id is None
                and (a.order_id is not None or _is_nontrade_close(a) or _is_delivery(a))
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
                        # an assignment/exercise whose delivered position (the option's
                        # underlying) landed in the rest-created group gets a continuation link
                        delivery = (
                            new_pk
                            if new_pk is not None and await _delivers_underlying(store, rows, rest)
                            else None
                        )
                        applied = await _apply_exit(
                            store, group, rows, rolled_to_pk=rolled_to, delivery_pk=delivery,
                        )
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
        if row.security_id is not None:
            if _is_closing(row):
                group = next((g for g in open_groups if row.security_id in g.security_ids()), None)
            elif row.action in _BARE_ACTIONS:
                # a bare futures Buy/Sell closes when an open group holds the OPPOSITE
                # position in that security (e.g. covering an assignment-delivered short);
                # same-sign or no holder -> it opens/extends nothing here, falls to rest.
                delta = _action_delta(row)
                group = next(
                    (
                        g for g in open_groups
                        if (net := _net_quantities(g.rows).get(row.security_id)) and net * delta < 0
                    ),
                    None,
                )
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
    """Every security the group traded has netted back to zero (signed walk -- so bare futures
    Buy/Sell round-trips and assignment deliveries count, not just ``* to Open/Close`` pairs).
    A group whose only activity is unmatched closes (window artifacts) is NOT "fully closed" --
    it never opened."""
    net = _net_quantities(rows)
    if not net:
        return False
    opened_something = any(_is_opening(r) or r.action in _BARE_ACTIONS for r in rows)
    return opened_something and all(qty == 0 for qty in net.values())


def _close_causes(rows: list) -> "set[TradeGroupStatus]":
    causes: set[TradeGroupStatus] = set()
    net = _net_quantities(rows)
    for r in rows:
        if r.action in _CLOSING_ACTIONS:
            causes.add(TradeGroupStatus.CLOSED)
        elif _is_nontrade_close(r):
            causes.add(_STATUS_BY_EVENT[_RD_EVENT_BY_SUBTYPE[r.transaction_sub_type]])
        elif r.action in _BARE_ACTIONS and r.security_id and net.get(r.security_id) == 0:
            # a bare Buy/Sell that participated in a flattened security acted as a close
            causes.add(TradeGroupStatus.CLOSED)
    return causes


def _norm_underlying(value: str | None) -> str | None:
    """TT underlying-symbol variants -> a comparable root: "/ESM6" and "/ES" both -> "ES..."
    prefixes; comparison is prefix-based within one execution cluster, where a same-instant
    delivery is essentially always the assignment's consequence."""
    if not value:
        return None
    return value.lstrip("/").strip().upper() or None


def _roots_relate(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return a.startswith(b) or b.startswith(a)


async def _delivers_underlying(store: "LedgerStore", close_rows: list, rest_rows: list) -> bool:
    """True when an assignment/exercise in ``close_rows`` delivered a position now sitting in
    ``rest_rows``. Matched through the securities dimension when the resolver decomposed
    underlyings; falls back to the transactions' own (normalized) underlying symbols --
    TT writes the option's as the dated contract ("/ESM6") and the delivery's as the product
    root ("/ES")."""
    assigned = [
        r for r in close_rows
        if _is_nontrade_close(r)
        and _RD_EVENT_BY_SUBTYPE.get(r.transaction_sub_type)
        in (TradeGroupEventType.ASSIGNMENT, TradeGroupEventType.EXERCISE)
    ]
    deliveries = [r for r in rest_rows if _is_delivery(r)]
    if not assigned or not deliveries:
        return False

    async def sec_underlying(security_id: str | None) -> str | None:
        if not security_id:
            return None
        sec = await store.get_security(security_id)
        return sec.underlying if sec is not None else None

    assigned_sec_unders = {u for r in assigned if (u := await sec_underlying(r.security_id))}
    assigned_txn_unders = {u for r in assigned if (u := _norm_underlying(r.underlying))}
    for r in deliveries:
        delivered = await store.get_security(r.security_id) if r.security_id else None
        if delivered is not None and assigned_sec_unders and (
            delivered.underlying in assigned_sec_unders
            or delivered.security_id in assigned_sec_unders
        ):
            return True
        delivered_under = _norm_underlying(r.underlying) or _norm_underlying(
            delivered.underlying if delivered is not None else None
        )
        if any(_roots_relate(delivered_under, a) for a in assigned_txn_unders):
            return True
    return False


def _same_underlying(close_rows: list, open_rows: list) -> bool:
    close_unders = {r.underlying for r in close_rows if r.underlying}
    open_unders = {r.underlying for r in open_rows if r.underlying}
    return bool(close_unders & open_unders)


async def _apply_exit(
    store: "LedgerStore", group: "_OpenGroup", rows: "list[ActivityRow]", *,
    rolled_to_pk: int | None, delivery_pk: int | None = None,
) -> "_AppliedExit":
    """Attach closing rows to their open group, emit lifecycle events, and flip the group's
    status (+ cash-basis realized_pnl) once fully offset. ``delivery_pk`` links an
    assignment/exercise event to the group holding the position it delivered (the option's
    underlying future/shares) -- the same continuation edge rolls use."""
    await store.attach_transactions_to_trade_group([r.tt_transaction_id for r in rows], group.pk)
    group.rows.extend(rows)

    fully_closed = _fully_closed(group.rows)
    event_ats = [r.executed_at for r in rows if r.executed_at is not None]
    event_at = max(event_ats) if event_ats else None

    # every routed-as-close trade row: explicit "* to Close" plus bare futures Buy/Sell
    trade_rows = [r for r in rows if r.action in _CLOSING_ACTIONS or r.action in _BARE_ACTIONS]
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
        links_delivery = (
            delivery_pk is not None
            and event_type in (TradeGroupEventType.ASSIGNMENT, TradeGroupEventType.EXERCISE)
        )
        await store.add_trade_group_event(
            EventRow(
                trade_group_id=group.pk, event_type=event_type.value,
                quantity_change=-sum(abs(r.quantity) for r in rd_rows if r.quantity is not None),
                premium_change=sum((_signed_net(r) for r in rd_rows), Decimal("0")),
                event_at=max(rd_ats) if rd_ats else event_at,
                rolled_to_group_id=(delivery_pk if links_delivery else None),
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
