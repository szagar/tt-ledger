"""Lightweight transfer/row shapes used by the store, repositories, and SDK.

Field sets mirror docs/schema.md 1:1 so a row maps directly onto its ORM model's columns
(minus surrogate ``id``/``created_at``/``updated_at``, which the store owns). Internal rows are
plain dataclasses (no dependency); API DTOs (Pydantic) live in ``tt_ledger/api`` and map onto these.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from .enums import Ingest, Origin, ReviewStatus

# --- write shapes -----------------------------------------------------------------


@dataclass
class AccountRow:
    """One accounts-dimension row (natural key: nickname). Seeded automatically by the SDK /
    StreamConsumer from the injected AccountMapper before any fact write -- every fact table
    FKs ``account -> accounts.nickname``."""

    nickname: str
    account_number: str
    login: str | None = None
    env: str = "live"
    source_system: str = "tastytrade"


@dataclass
class SecurityRow:
    security_id: str  # resolver output (default: the raw vendor symbol)
    product_type: str
    underlying: str | None = None
    expiry: date | None = None
    strike: Decimal | None = None
    option_type: str | None = None  # P | C
    multiplier: int | None = None
    exchange: str | None = None
    currency: str | None = None
    tt_symbol: str | None = None
    occ_symbol: str | None = None
    streamer_symbol: str | None = None
    source_system: str = "tastytrade"
    metadata: dict | None = None


@dataclass
class OrderRow:
    tt_order_id: str | None
    account: str  # nickname
    origin: Origin
    ingest: Ingest
    oms_order_id: str | None = None
    client_order_id: str | None = None
    account_number: str | None = None  # audit
    source_system: str = "tastytrade"
    security_id: str | None = None
    underlying: str | None = None
    order_type: str | None = None
    time_in_force: str | None = None
    gtc_date: str | None = None
    price: Decimal | None = None
    stop_trigger: Decimal | None = None
    price_effect: str | None = None
    average_fill_price: Decimal | None = None
    is_complex: bool = False
    complex_order_type: str | None = None
    oms_status: str | None = None
    tt_status: str | None = None  # raw broker status
    status_message: str | None = None
    filled_quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    signal_id: str | None = None
    trace_id: str | None = None
    strategy_id: int | None = None
    trade_group_id: int | None = None
    market_context_id: int | None = None
    received_at: datetime | None = None
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    terminal_at: datetime | None = None


@dataclass
class LegRow:
    order_id: int
    leg_index: int
    security_id: str
    action: str | None = None
    quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    quantity_direction: str | None = None
    price: Decimal | None = None
    fill_price: Decimal | None = None


@dataclass
class FillRow:
    fill_id: str
    order_id: int | None = None
    order_leg_id: int | None = None
    tt_order_id: str | None = None
    quantity: Decimal | None = None
    fill_price: Decimal | None = None
    filled_at: datetime | None = None
    destination_venue: str | None = None
    ext_exec_id: str | None = None
    ext_group_fill_id: str | None = None


@dataclass
class TxnRow:
    tt_transaction_id: str
    tt_order_id: str | None
    account: str
    account_number: str | None = None  # audit
    source_system: str = "tastytrade"
    transaction_type: str | None = None
    transaction_sub_type: str | None = None
    action: str | None = None
    security_id: str | None = None
    underlying: str | None = None
    quantity: Decimal | None = None
    price: Decimal | None = None
    value: Decimal | None = None
    value_effect: str | None = None
    net_value: Decimal | None = None
    net_value_effect: str | None = None
    commission: Decimal | None = None
    clearing_fees: Decimal | None = None
    regulatory_fees: Decimal | None = None
    proprietary_index_option_fees: Decimal | None = None
    is_estimated_fee: bool | None = None
    description: str | None = None
    order_id: int | None = None
    order_leg_id: int | None = None
    position_id: int | None = None
    closed_position_id: int | None = None
    trade_group_id: int | None = None
    executed_at: datetime | None = None
    transaction_date: date | None = None


@dataclass
class PositionRow:
    account: str
    security_id: str
    quantity: Decimal
    quantity_direction: str
    average_open_price: Decimal | None = None
    mark_price: Decimal | None = None
    close_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    realized_day_gain: Decimal | None = None
    multiplier: int = 1
    expires_at: datetime | None = None
    strategy_id: int | None = None
    opening_order_id: int | None = None
    trade_group_id: int | None = None
    position_opened_at: datetime | None = None


@dataclass
class ClosedPositionRow:
    """A completed open->close round-trip, produced by replaying transaction history
    (``ingest/replay.py``) — the durable historical record ``positions`` itself can't hold, since
    that table only ever reflects the account+security's CURRENT (or most recently reopened) lot."""

    account: str
    security_id: str
    quantity: Decimal
    quantity_direction: str
    average_open_price: Decimal | None = None
    average_close_price: Decimal | None = None
    realized_pnl: Decimal | None = None  # gross: price moves × qty × multiplier, no fees
    fees: Decimal | None = None  # commissions + clearing + regulatory + index-option fees, whole lifecycle
    pnl_net: Decimal | None = None  # realized_pnl - fees (use this for R-multiples)
    opening_order_id: int | None = None
    closing_order_id: int | None = None
    trade_group_id: int | None = None
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    holding_period_days: int | None = None


@dataclass
class BalanceSnapshotRow:
    """One point in an account's balance time series (``balance_snapshots``) — written throttled
    from the stream and once per REST ``sync()``. Append-only; the natural key is
    ``(account, captured_at, source)``."""

    account: str
    captured_at: datetime
    source: str = "stream"  # stream | rest_sync
    net_liquidating_value: Decimal | None = None
    cash_balance: Decimal | None = None
    equity_buying_power: Decimal | None = None
    derivative_buying_power: Decimal | None = None
    maintenance_requirement: Decimal | None = None
    pending_cash: Decimal | None = None
    day_trading_buying_power: Decimal | None = None
    raw: dict | None = None


@dataclass
class TradeGroupRow:
    group_id: str
    account: str
    origin: Origin
    source_system: str = "tastytrade"
    review_status: ReviewStatus = ReviewStatus.NEEDS_REVIEW
    manually_attributed: bool = False
    reviewed_at: datetime | None = None
    reviewed_by: str | None = None
    underlying: str | None = None
    security_id: str | None = None
    strategy_type: str | None = None
    leg_count: int = 1
    total_premium: Decimal | None = None
    quantity: Decimal | None = None
    total_fees: Decimal | None = None
    status: str = "open"
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    max_profit: Decimal | None = None
    max_loss: Decimal | None = None
    initial_risk: Decimal | None = None  # planned 1R at open, frozen (see models.TradeGroup)
    profit_target: str | None = None
    stop_loss: str | None = None
    exit_strategy: str | None = None
    structure: dict | None = None  # host-written submit-time structure descriptor (opaque JSON)
    order_id: int | None = None
    strategy_id: int | None = None
    bot_name: str | None = None
    signal_id: str | None = None
    executed_at: datetime | None = None
    closed_at: datetime | None = None


@dataclass
class EventRow:
    trade_group_id: int
    event_type: str
    quantity_change: Decimal = Decimal("0")
    premium_change: Decimal = Decimal("0")
    realized_pnl: Decimal | None = None
    event_at: datetime | None = None
    notes: str | None = None
    rolled_to_group_id: int | None = None
    transaction_id: int | None = None
    order_id: int | None = None


# --- read shapes ------------------------------------------------------------------


@dataclass
class TradeRow:
    """A row of v_trades_unified (== trade_groups, exposed with its own read shape)."""

    group_id: str
    account: str
    origin: Origin
    source_system: str = "tastytrade"
    review_status: ReviewStatus = ReviewStatus.NEEDS_REVIEW
    manually_attributed: bool = False
    underlying: str | None = None
    security_id: str | None = None
    strategy_type: str | None = None
    leg_count: int = 1
    total_premium: Decimal | None = None
    quantity: Decimal | None = None
    total_fees: Decimal | None = None
    status: str = "open"
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    max_profit: Decimal | None = None
    max_loss: Decimal | None = None
    initial_risk: Decimal | None = None  # planned 1R at open, frozen (see models.TradeGroup)
    structure: dict | None = None  # host-written submit-time structure descriptor (opaque JSON)
    order_id: int | None = None
    strategy_id: int | None = None
    bot_name: str | None = None
    signal_id: str | None = None
    executed_at: datetime | None = None
    closed_at: datetime | None = None

    @property
    def pnl_net(self) -> Decimal | None:
        """``realized_pnl - total_fees`` — the R-multiple numerator (same convention as
        ``ClosedPositionRow.pnl_net``). Derived, never stored: reconcile revises
        ``realized_pnl``/``total_fees``, so a stored copy would go silently stale.
        ``None`` until the group has a realized PnL; missing fees count as zero."""
        if self.realized_pnl is None:
            return None
        return self.realized_pnl - (self.total_fees or Decimal("0"))


def trade_group_to_row(tg: TradeGroupRow) -> TradeRow:
    """A stored ``TradeGroupRow`` -> its ``TradeRow`` read shape (drops operator-only fields
    like ``reviewed_at``/``reviewed_by``/``profit_target`` that ``TradeRow`` doesn't expose)."""
    return TradeRow(**{f: getattr(tg, f) for f in TradeRow.__dataclass_fields__ if hasattr(tg, f)})


@dataclass
class LegDetailRow:
    """A stored order_legs row WITH its surrogate id (fills join on ``order_leg_id``) — the read
    twin of the write-side ``LegRow``, which deliberately omits ``id`` (the store owns it)."""

    id: int
    order_id: int
    leg_index: int
    security_id: str
    action: str | None = None
    quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    quantity_direction: str | None = None
    price: Decimal | None = None
    fill_price: Decimal | None = None


@dataclass
class OrderDetail:
    """One order of a trade group with its legs + fills — ``trade_structure()``'s read shape.
    ``fills`` is flat (each ``FillRow`` carries ``order_leg_id`` matching a leg's ``id``): fills
    from the stream path may not be leg-attributed, so nesting them under legs would drop them."""

    order_pk: int
    order: OrderRow
    legs: list["LegDetailRow"] = field(default_factory=list)
    fills: list[FillRow] = field(default_factory=list)


@dataclass
class TransactionDetailRow:
    """A transactions row WITH its surrogate id + the joined order's correlation ids — the
    paged read shape for host transaction views (``LedgerClient.transactions``)."""

    id: int
    tt_transaction_id: str
    account: str
    transaction_type: str | None = None
    transaction_sub_type: str | None = None
    action: str | None = None
    description: str | None = None
    security_id: str | None = None
    underlying: str | None = None
    quantity: Decimal | None = None
    price: Decimal | None = None
    net_value: Decimal | None = None
    net_value_effect: str | None = None  # "Credit" | "Debit" | "None" -- net_value is a magnitude
    commission: Decimal | None = None
    executed_at: datetime | None = None
    order_id: int | None = None
    tt_order_id: str | None = None
    trade_group_id: int | None = None
    signal_id: str | None = None  # from the joined order; None when unreconciled/broker-origin


@dataclass
class ActivityRow:
    """A row of v_account_activity (transaction joined to order + trade_group)."""

    tt_transaction_id: str
    account: str
    transaction_type: str | None = None
    transaction_sub_type: str | None = None
    action: str | None = None
    security_id: str | None = None
    underlying: str | None = None
    quantity: Decimal | None = None
    price: Decimal | None = None
    net_value: Decimal | None = None
    net_value_effect: str | None = None  # "Credit" | "Debit" | "None" -- net_value is a magnitude
    commission: Decimal | None = None
    clearing_fees: Decimal | None = None
    regulatory_fees: Decimal | None = None
    proprietary_index_option_fees: Decimal | None = None
    executed_at: datetime | None = None
    order_id: int | None = None
    tt_order_id: str | None = None
    trade_group_id: int | None = None
    origin: Origin | None = None  # from the joined order; None when unreconciled
    review_status: ReviewStatus | None = None  # from the joined trade_group
    order_trade_group_id: int | None = None  # the joined ORDER's group -- submit-time intent


# --- inputs / filters / results ---------------------------------------------------


@dataclass
class OrderLegInput:
    """One leg of an order recorded at submission — the host OMS knows its legs synchronously,
    so a resting (working) order renders with structure immediately instead of waiting for the
    pull path to backfill ``order_legs``. Carries the broker-native ``symbol`` (like
    ``BrokerTransaction``) — ``record_order`` resolves it through the injected resolver and
    upserts the securities dimension row, exactly as the pull path does. Fill fields are never
    set here; sync/push enrichment owns those."""

    symbol: str  # broker-native leg symbol; resolved via the injected SecurityResolver
    instrument_type: str | None = None  # resolver hint (broker instrument-type vocab)
    action: str | None = None  # broker or proto vocab ("Buy to Open" / "BUY_TO_OPEN")
    quantity: Decimal | None = None
    price: Decimal | None = None  # per-leg limit price, when the host has one


@dataclass
class OrderInput:
    """An order recorded at submission (oms_submit path). ``tt_order_id`` may be set when the
    host records right after the broker's submit response (the id is known synchronously there);
    left None, it arrives later via push/pull enrichment (docs/ingestion.md). ``trade_group``
    (the public UUID from ``open_trade_group``) pre-attributes the order — its fills/transactions
    then attach to that group in reconcile instead of clustering into a new one. ``legs``
    (optional) writes ``order_legs`` at record time; the pull path later enriches the same
    (order_id, leg_index) rows with fill data."""

    account: str
    tt_order_id: str | None = None
    oms_order_id: str | None = None  # the host OMS's own order id, for cross-referencing
    security_id: str | None = None
    underlying: str | None = None
    order_type: str | None = None
    time_in_force: str | None = None
    price: Decimal | None = None
    price_effect: str | None = None
    is_complex: bool = False
    complex_order_type: str | None = None
    signal_id: str | None = None
    trace_id: str | None = None
    strategy_id: int | None = None
    market_context_id: int | None = None
    trade_group: str | None = None  # trade_groups.group_id (UUID), not the surrogate pk
    legs: list[OrderLegInput] = field(default_factory=list)


@dataclass
class FillEvent:
    """A fill/status update arriving on the push (stream) path — enriches an existing order by
    ``tt_order_id`` only; never creates one (docs/ingestion.md: a stream fill with no matching
    local order is not authoritative, sync_orders is)."""

    tt_order_id: str
    status: str | None = None  # raw broker status string
    average_fill_price: Decimal | None = None
    filled_quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    filled_at: datetime | None = None


@dataclass
class OrderFilter:
    origin: Origin | None = None
    account: str | None = None
    status: str | None = None
    underlying: str | None = None
    trade_group_id: int | None = None
    oms_order_id: str | None = None  # host OMS's order id (unique per order)
    unlinked: bool = False  # only orders with NO trade_group_id (manual-attribution queue)
    start: date | None = None
    end: date | None = None


@dataclass
class TradeFilter:
    origin: Origin | None = None
    review_status: ReviewStatus | None = None
    status: str | None = None  # TradeGroupStatus: open|closed|expired|assigned|exercised|mixed
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
class TransactionQuery:
    """Paged transactions read (``query_transactions``). Ordered newest-first
    (``executed_at DESC, id DESC``); ``accounts`` scopes to a nickname set (host scope filters)."""

    account: str | None = None
    accounts: list[str] | None = None
    start: date | None = None
    end: date | None = None
    underlying: str | None = None
    transaction_type: str | None = None
    trade_group_id: int | None = None
    limit: int = 100
    offset: int = 0


@dataclass
class SyncResult:
    orders: int = 0
    transactions: int = 0
    positions: int = 0
    balances: int = 0
    fills: int = 0
    trade_groups: int = 0
    healed_groups: int = 0  # reconcile self-heal: fully-closed-content groups whose status flipped
    errors: list[str] = field(default_factory=list)
