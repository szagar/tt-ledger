"""Replay (docs/ingestion.md → Replay): rebuild ``positions``/``closed_positions`` purely from
transaction history -- the only endpoint with a real historical record. ``sync_positions``
(``ingest/pull.py``) only ever gives the broker's CURRENT snapshot; ``transactions`` is the
authoritative event log of every position-affecting event, so replaying it forward can reconstruct
quantity, cost basis, and every completed open->close lifecycle, not just the present moment.

Full rebuild, not incremental: walks every position-affecting transaction for an account from
scratch, in ``executed_at`` order, maintaining a running weighted-average-cost lot per security.
This makes multi-transaction partial closes trivially correct (no accumulator needs to persist
between calls) and the whole thing idempotent by construction:
  - ``positions`` upserts on ``(account, security_id)`` -- same row, same surrogate id, every run.
  - ``closed_positions`` "upserts" (app-level key, no DB unique constraint -- see store/sql.py) on
    ``(account, security_id, opened_at, closed_at)`` -- a completed lifecycle always recomputes to
    the same key, so re-running never duplicates it.
  - ``transactions.position_id`` / ``closed_position_id`` (scaffolded in docs/schema.md, unused
    until this module) get re-linked to the same ids every time.

``position_id`` marks a transaction that fed into the account+security's *current* open lot;
``closed_position_id`` marks the transaction that closed a lifecycle. A direction-flip trade (e.g.
long 10, sell 15 -> short 5) gets BOTH: it closes the old lot and opens the new one in the same
row. Because ``positions`` holds only the current/most-recent lot, a `position_id` on an old
transaction can end up pointing at a row that's since been overwritten by a later reopening --
the durable, frozen record of a closed lifecycle lives in ``closed_positions``, not through
``position_id``.

P&L convention: ``realized_pnl`` is GROSS (price moves × quantity × multiplier — matches
``_derive_unrealized_pnl``); ``fees`` accumulates every member transaction's commissions +
clearing/regulatory/index-option fees across the lifecycle; ``pnl_net = realized_pnl - fees`` is
what R-multiple / expectancy analysis should read. A direction-flip transaction's fees are
attributed entirely to the lifecycle it CLOSES (not split with the lot it opens).

Known limitations:
  - If transaction history doesn't reach back to when a still-open position was first opened (a
    transfer from another broker, or a sync window starting after inception), replay treats the
    earliest visible activity as the opening trade -- cost basis/opening date for such a position
    is understated to the visible window, not fabricated.
  - A transaction with a ``security_id`` + ``quantity`` but no ``action`` (some corporate actions --
    splits, symbol changes) is applied as a signed delta using the transaction's own quantity sign
    as given, rather than an inferred Buy/Sell direction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import replace as replace_dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from ..rows import ActivityFilter, ClosedPositionRow, PositionRow, SyncResult

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..rows import ActivityRow
    from ..store import LedgerStore


async def rebuild_positions_from_transactions(store: "LedgerStore", account: str | None = None) -> "SyncResult":
    """Rebuild every (account, security) lot from transaction history. Each account's failure is
    caught and recorded rather than aborting the rest, matching ``sync_all``/``reconcile``."""
    result = SyncResult()
    accounts = [account] if account is not None else await _accounts_with_activity(store)

    for acct in accounts:
        try:
            result.positions += await _rebuild_for_account(store, acct)
        except Exception as exc:  # noqa: BLE001 - one account's failure must not abort the rest
            result.errors.append(f"{acct}: {exc}")
            logger.warning("replay failed for %s: %s", acct, exc)

    return result


async def _accounts_with_activity(store: "LedgerStore") -> list[str]:
    activity = await store.account_activity(ActivityFilter())
    return sorted({row.account for row in activity})


async def _rebuild_for_account(store: "LedgerStore", account: str) -> int:
    activity = await store.account_activity(ActivityFilter(account=account))

    by_security: dict[str, list["ActivityRow"]] = {}
    for row in activity:
        if row.security_id is None or row.quantity is None or row.executed_at is None:
            continue  # cash-only movement (fee, transfer, dividend paid in cash) -- no position effect
        by_security.setdefault(row.security_id, []).append(row)

    # deterministic "now" for lapse detection: the account's own latest activity, never wall-clock
    last_activity = max((r.executed_at for rows in by_security.values() for r in rows), default=None)

    for security_id, rows in by_security.items():
        rows.sort(key=lambda r: (r.executed_at, _closes_last(r)))
        multiplier = await _multiplier_of(store, security_id)
        sec = await store.get_security(security_id)
        existing = await store.get_position(account, security_id)
        position_row, plan = _replay_security(account, security_id, rows, multiplier, existing)
        position_row, plan = _lapse_expired_lot(
            account, security_id, position_row, plan,
            expiry=(sec.expiry if sec is not None else None),
            multiplier=multiplier, last_activity=last_activity,
        )

        await store.upsert_positions([position_row])
        position_id = await store.get_position_id(account, security_id)

        links: list[tuple[str, int | None, int | None]] = []
        for tt_transaction_id, marks_open, closed_row in plan:
            closed_position_id = await store.upsert_closed_position(closed_row) if closed_row is not None else None
            links.append((tt_transaction_id, position_id if marks_open else None, closed_position_id))
        await store.link_transactions_to_positions(links)

    return len(by_security)


def _lapse_expired_lot(
    account: str, security_id: str, position_row: "PositionRow",
    plan: "list[tuple[str, bool, ClosedPositionRow | None]]", *,
    expiry, multiplier: int, last_activity,
):  # noqa: ANN001, ANN201
    """Close a still-open option lot whose contract expired without a matching settlement row.

    Corporate actions (OCC strike adjustments -- e.g. QQQ's 2024 $0.22 special distribution)
    re-symbol a contract, so its Receive Deliver settlement arrives under a DIFFERENT
    security_id and never offsets the original lot; the phantom stays "open" forever. Once
    the account has activity a full day past the contract's expiry, the lot definitionally
    lapsed: close it at 0 (the cash effect, if any, lives under the adjusted symbol's own
    rows). Determinism: measured against the account's latest transaction, never wall-clock.
    """
    if (
        expiry is None
        or last_activity is None
        or position_row.quantity == 0
        or last_activity.date() <= expiry + timedelta(days=1)
    ):
        return position_row, plan

    lapsed_at = datetime.combine(expiry, datetime.min.time(), tzinfo=UTC).replace(hour=21, minute=15)
    opened_at = position_row.position_opened_at
    if opened_at is not None and opened_at.tzinfo is None:
        opened_at = opened_at.replace(tzinfo=UTC)  # SQLite round-trips DateTime(timezone=True) naive
    sign = Decimal(1) if position_row.quantity_direction == "Long" else Decimal(-1)
    gross = (Decimal("0") - (position_row.average_open_price or Decimal("0"))) \
        * position_row.quantity * multiplier * sign
    closed = ClosedPositionRow(
        account=account, security_id=security_id,
        quantity=position_row.quantity, quantity_direction=position_row.quantity_direction,
        average_open_price=position_row.average_open_price, average_close_price=Decimal("0"),
        realized_pnl=gross, fees=Decimal("0"), pnl_net=gross,
        opening_order_id=position_row.opening_order_id,
        opened_at=opened_at, closed_at=lapsed_at,
        holding_period_days=_holding_period(opened_at, lapsed_at),
    )
    logger.info(
        "lapsed expired option lot with no settlement row: %s %s x%s (expiry %s)",
        account, security_id, position_row.quantity, expiry,
    )
    flattened = replace_dataclass(
        position_row, quantity=Decimal("0"),
        average_open_price=None, opening_order_id=None, position_opened_at=None,
    )
    return flattened, [*plan, (f"__lapsed__{security_id}", False, closed)]


def net_open_quantities(rows: "list[ActivityRow]") -> dict[str, Decimal]:
    """Signed net open quantity per security, using replay's exact lot rules.

    This is the transaction-level answer to "which lots does the ledger still think are
    open" — the same walk ``_replay_security`` does (Money Movement excluded, settlements
    clamp toward zero, an explicit close against a flat lot is a no-op, opens-before-closes
    within one timestamp), reduced to the running signed quantity. Reconcile's lapse
    synthesis keys off this instead of the positions table, because ``_lapse_expired_lot``
    has already flattened the *stored* position for exactly the lots it needs to find.
    """
    by_security: dict[str, list[ActivityRow]] = {}
    for row in rows:
        if row.security_id is None or row.quantity is None or row.executed_at is None:
            continue
        by_security.setdefault(row.security_id, []).append(row)

    nets: dict[str, Decimal] = {}
    for security_id, sec_rows in by_security.items():
        sec_rows.sort(key=lambda r: (r.executed_at, _closes_last(r)))
        lot = _Lot()
        for row in sec_rows:
            delta, price = _effective_delta_price(row, lot)
            if delta == 0 or price is None:
                continue
            lot.signed_quantity += delta
        nets[security_id] = lot.signed_quantity
    return nets


async def _multiplier_of(store: "LedgerStore", security_id: str) -> int:
    sec = await store.get_security(security_id)
    return sec.multiplier if sec is not None and sec.multiplier else 1


@dataclass
class _Lot:
    """The currently-open running lot for one (account, security) -- reset to a fresh, empty
    ``_Lot`` every time it fully closes."""

    signed_quantity: Decimal = Decimal("0")  # + long, - short
    average_open_price: Decimal | None = None
    opened_at: datetime | None = None
    opening_order_id: int | None = None
    realized_pnl: Decimal = Decimal("0")  # gross; accumulates across partial closes of THIS lifecycle
    fees: Decimal = Decimal("0")  # accumulates every member transaction's fees for THIS lifecycle


# Receive Deliver sub-types that terminate a lot without a trade. They carry no action and
# usually no price -- the delta must OFFSET the open lot toward zero, settling at price 0
# (or the row's price when the feed provides one, e.g. cash-settled exercise).
_SETTLEMENT_SUBTYPES = {
    "Expiration",
    "Assignment",
    "Cash Settled Assignment",
    "Exercise",
    "Cash Settled Exercise",
}


def _is_settlement(row) -> bool:  # noqa: ANN001 -- ActivityRow (duck-typed)
    return row.transaction_type == "Receive Deliver" and row.transaction_sub_type in _SETTLEMENT_SUBTYPES


def _effective_delta_price(row, lot: "_Lot") -> "tuple[Decimal, Decimal | None]":  # noqa: ANN001
    """(signed delta, effective price) for one row against the current lot.

    Trades use the action-signed quantity + trade price. Settlements offset the open lot
    (long -> negative delta, short -> positive) at price 0 when the feed omits one -- an
    expired long realizes -cost, an expired short keeps its credit. A settlement against a
    flat lot is a no-op (history window didn't reach the opening).

    Excluded from position quantity entirely:
    * ``Money Movement`` rows -- cash-only; futures daily Mark to Market carries a
      quantity + price but is NOT a fill (counting it inflated futures lots by one
      contract per settlement day in the first live rehearsal).
    * an explicit ``* to Close`` against a FLAT lot -- the matching open predates the
      sync window; fabricating a fresh lot from it produced phantom "open" positions.
    """
    if row.transaction_type == "Money Movement":
        return Decimal("0"), None
    if _is_settlement(row):
        if lot.signed_quantity == 0:
            return Decimal("0"), None
        qty = min(abs(row.quantity), abs(lot.signed_quantity))
        delta = -qty if lot.signed_quantity > 0 else qty
        return delta, (row.price if row.price is not None else Decimal("0"))
    if lot.signed_quantity == 0 and (row.action or "").strip().endswith("to Close"):
        return Decimal("0"), None
    return _signed_delta(row.action, row.quantity), row.price


def _closes_last(row) -> int:  # noqa: ANN001
    """Within one timestamp, apply opens before closes/settlements -- option-exercise
    delivery batches book the delivered future's open and its offsetting close at the
    same instant, and close-first would hit the close-on-flat no-op."""
    if _is_settlement(row):
        return 1
    return 1 if (row.action or "").strip().endswith("to Close") else 0


def _signed_delta(action: str | None, quantity: Decimal) -> Decimal:
    if action is not None and action.strip().lower().startswith("sell"):
        return -quantity
    return quantity


def _fees_of(row) -> Decimal:  # noqa: ANN001 -- ActivityRow (duck-typed)
    return sum(
        (
            row.commission or Decimal("0"),
            row.clearing_fees or Decimal("0"),
            row.regulatory_fees or Decimal("0"),
            getattr(row, "proprietary_index_option_fees", None) or Decimal("0"),
        ),
        Decimal("0"),
    )


def _closed_row(account: str, security_id: str, lot: "_Lot", *, close_price: Decimal, closing_order_id: int | None, closed_at: datetime | None) -> ClosedPositionRow:
    return ClosedPositionRow(
        account=account, security_id=security_id,
        quantity=abs(lot.signed_quantity), quantity_direction=("Long" if lot.signed_quantity > 0 else "Short"),
        average_open_price=lot.average_open_price, average_close_price=close_price, realized_pnl=lot.realized_pnl,
        fees=lot.fees, pnl_net=lot.realized_pnl - lot.fees,
        opening_order_id=lot.opening_order_id, closing_order_id=closing_order_id,
        opened_at=lot.opened_at, closed_at=closed_at,
        holding_period_days=_holding_period(lot.opened_at, closed_at),
    )


def _holding_period(opened_at: datetime | None, closed_at: datetime | None) -> int | None:
    if opened_at is None or closed_at is None:
        return None
    return (closed_at - opened_at).days


def _replay_security(
    account: str, security_id: str, rows: "list[ActivityRow]", multiplier: int, existing: "PositionRow | None",
) -> "tuple[PositionRow, list[tuple[str, bool, ClosedPositionRow | None]]]":
    lot = _Lot()
    last_direction = "Long"
    plan: list[tuple[str, bool, ClosedPositionRow | None]] = []

    for row in rows:
        delta, price = _effective_delta_price(row, lot)
        if delta == 0 or price is None:
            plan.append((row.tt_transaction_id, False, None))
            continue

        old_signed = lot.signed_quantity
        new_signed = old_signed + delta

        if old_signed == 0:
            lot = _Lot(
                signed_quantity=new_signed, average_open_price=price, opened_at=row.executed_at,
                opening_order_id=row.order_id, fees=_fees_of(row),
            )
            plan.append((row.tt_transaction_id, True, None))
            continue

        if old_signed * delta > 0:
            # adding to the existing lot, same direction -- recompute the weighted-average cost
            total_qty = abs(old_signed) + abs(delta)
            lot.average_open_price = (abs(old_signed) * lot.average_open_price + abs(delta) * price) / total_qty
            lot.signed_quantity = new_signed
            lot.fees += _fees_of(row)
            plan.append((row.tt_transaction_id, True, None))
            continue

        # reducing -- partial close, full close, or a flip through zero
        closing_qty = min(abs(delta), abs(old_signed))
        sign = Decimal(1) if old_signed > 0 else Decimal(-1)
        lot.realized_pnl += (price - lot.average_open_price) * closing_qty * multiplier * sign
        lot.fees += _fees_of(row)  # a flip's fees stay with the lifecycle it closes (module docstring)

        if new_signed == 0:
            closed = _closed_row(account, security_id, lot, close_price=price, closing_order_id=row.order_id, closed_at=row.executed_at)
            last_direction = closed.quantity_direction
            plan.append((row.tt_transaction_id, False, closed))
            lot = _Lot()
        elif (new_signed > 0) != (old_signed > 0):
            closed = _closed_row(account, security_id, lot, close_price=price, closing_order_id=row.order_id, closed_at=row.executed_at)
            last_direction = closed.quantity_direction
            lot = _Lot(signed_quantity=new_signed, average_open_price=price, opened_at=row.executed_at, opening_order_id=row.order_id)
            plan.append((row.tt_transaction_id, True, closed))
        else:
            lot.signed_quantity = new_signed
            plan.append((row.tt_transaction_id, True, None))

    quantity_direction = "Long" if lot.signed_quantity > 0 else "Short" if lot.signed_quantity < 0 else last_direction
    position_row = PositionRow(
        account=account, security_id=security_id,
        quantity=abs(lot.signed_quantity), quantity_direction=quantity_direction,
        average_open_price=lot.average_open_price, multiplier=multiplier,
        opening_order_id=lot.opening_order_id, position_opened_at=lot.opened_at,
        # market-data + operator-attribution fields: replay has no opinion on these, they belong
        # to sync_positions (broker snapshot) / remap respectively -- preserve, never reset to None.
        mark_price=existing.mark_price if existing else None,
        close_price=existing.close_price if existing else None,
        unrealized_pnl=existing.unrealized_pnl if existing else None,
        realized_day_gain=existing.realized_day_gain if existing else None,
        expires_at=existing.expires_at if existing else None,
        strategy_id=existing.strategy_id if existing else None,
        trade_group_id=existing.trade_group_id if existing else None,
    )
    return position_row, plan
