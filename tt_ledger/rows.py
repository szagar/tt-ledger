"""Lightweight transfer/row shapes used by the store, repositories, and SDK.

Stubs — fields are illustrative; fill them out per docs/schema.md and docs/api.md.
Internal rows are plain dataclasses (no dependency); API DTOs (Pydantic) live in
``tt_ledger/api`` and map onto these.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from .enums import Ingest, Origin, ReviewStatus

# --- write shapes -----------------------------------------------------------------


@dataclass
class SecurityRow:
    security_id: str  # resolver output (default: the raw vendor symbol)
    product_type: str
    underlying: str | None = None
    tt_symbol: str | None = None
    streamer_symbol: str | None = None
    # TODO: expiry, strike, option_type, multiplier, exchange, currency, metadata


@dataclass
class OrderRow:
    tt_order_id: str | None
    account: str  # nickname
    origin: Origin
    ingest: Ingest
    security_id: str | None = None
    # TODO: full columns per docs/schema.md (orders)


@dataclass
class LegRow:
    order_id: int
    leg_index: int
    security_id: str
    action: str
    # TODO: quantity, fill_price, …


@dataclass
class FillRow:
    fill_id: str
    tt_order_id: str
    quantity: Decimal | None = None
    fill_price: Decimal | None = None
    filled_at: datetime | None = None
    # TODO: order_id, order_leg_id, venue, ext ids


@dataclass
class TxnRow:
    tt_transaction_id: str
    tt_order_id: str | None
    account: str
    security_id: str | None = None
    # TODO: type/sub_type, action, qty, price, value, fees, executed_at, …


@dataclass
class PositionRow:
    account: str
    security_id: str
    # TODO: quantity, direction, avg_open, mark, unrealized_pnl, attribution


@dataclass
class TradeGroupRow:
    group_id: str
    account: str
    origin: Origin
    review_status: ReviewStatus = ReviewStatus.NEEDS_REVIEW
    manually_attributed: bool = False
    # TODO: strategy_type, premium, realized_pnl, status, attribution, …


@dataclass
class EventRow:
    trade_group_id: int
    event_type: str
    # TODO: quantity_change, premium_change, realized_pnl, refs, event_at


# --- read shapes ------------------------------------------------------------------


@dataclass
class TradeRow:
    """A row of v_trades_unified."""

    group_id: str
    origin: Origin
    # TODO: account, strategy_type, realized/unrealized pnl, review_status, …


@dataclass
class ActivityRow:
    """A row of v_account_activity (transaction joined to order + group)."""

    tt_transaction_id: str
    # TODO: origin, fees, net_value, order_id, trade_group_id, …


# --- inputs / filters / results ---------------------------------------------------


@dataclass
class OrderInput:
    """An order recorded at submission (oms_submit path)."""

    account: str
    # TODO: legs, type/TIF, price, correlation ids


@dataclass
class FillEvent:
    """A fill arriving on the push (stream) path."""

    tt_order_id: str
    # TODO: status, qty, price, time


@dataclass
class OrderFilter:
    origin: Origin | None = None
    account: str | None = None
    status: str | None = None
    underlying: str | None = None
    start: date | None = None
    end: date | None = None


@dataclass
class TradeFilter:
    origin: Origin | None = None
    review_status: ReviewStatus | None = None
    account: str | None = None
    underlying: str | None = None
    start: date | None = None
    end: date | None = None


@dataclass
class ActivityFilter:
    account: str | None = None
    start: date | None = None
    end: date | None = None
    unreconciled_only: bool = False


@dataclass
class SyncResult:
    orders: int = 0
    transactions: int = 0
    fills: int = 0
    trade_groups: int = 0
    errors: list[str] = field(default_factory=list)
