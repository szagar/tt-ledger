"""The broker-native wire shape + client contract (docs/ingestion.md → Pull).

These dataclasses mirror TastyTrade's own REST JSON (kebab-case keys translated to snake_case
attributes, as any client SDK would expose them) — the raw, **broker-native** boundary layer.
Nothing here is internal: ``ingest/pull.py`` translates every one of these into the ledger's own
``rows.py`` shapes via ``AccountMapper`` (account_number -> nickname) and ``SecurityResolver``
(vendor symbol -> security_id) before anything touches the store. Broker-native identifiers must
never leak past that boundary (Rule 1 / Rule 2, docs/identity.md).

``BrokerClient`` is the Protocol ``ingest/pull.py`` depends on — a real TastyTrade REST client (the
``[tastytrade]`` extra: ``httpx``/``websockets``) and ``MockTastyTradeClient`` (``mock_broker.py``,
for tests) both implement it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable


@dataclass
class PlacedFill:
    """One execution against a leg (order-history ``legs[].fills[]``)."""

    fill_id: str
    quantity: Decimal
    fill_price: Decimal
    filled_at: datetime
    destination_venue: str | None = None
    ext_exec_id: str | None = None
    ext_group_fill_id: str | None = None


@dataclass
class PlacedLeg:
    """One leg of a placed order (order-history ``legs[]``)."""

    instrument_type: str  # TastyTrade instrument-type, e.g. "Equity Option", "Future"
    symbol: str  # broker-native (vendor) symbol
    action: str  # "Buy to Open" | "Sell to Close" | …
    quantity: Decimal
    remaining_quantity: Decimal
    fills: list[PlacedFill] = field(default_factory=list)


@dataclass
class PlacedOrder:
    """One order from ``GET /accounts/{account}/orders`` (order-history)."""

    id: str  # the broker order id -> tt_order_id
    account_number: str  # broker-native account number, NOT the nickname
    received_at: datetime
    legs: list[PlacedLeg] = field(default_factory=list)
    underlying_symbol: str | None = None
    underlying_instrument_type: str | None = None
    order_type: str | None = None
    time_in_force: str | None = None
    gtc_date: str | None = None
    price: Decimal | None = None
    stop_trigger: Decimal | None = None
    price_effect: str | None = None  # "Credit" | "Debit"
    status: str | None = None  # raw broker status string -> tt_status
    reject_reason: str | None = None  # -> OrderRow.status_message
    # TastyTrade's real Order object has no "is complex" boolean — only complex_order_id
    # (non-null when this order belongs to a complex order) and complex_order_tag. There is
    # also no order-level average-fill-price / filled-quantity / remaining-quantity field on
    # their Order schema (verified against developer.tastytrade.com's OpenAPI spec) — only
    # each leg's own quantity / remaining_quantity, which is why OrderRepository derives the
    # order-level aggregates itself for single-leg orders instead of expecting the broker to
    # supply them.
    complex_order_id: str | None = None
    complex_order_tag: str | None = None
    updated_at: datetime | None = None
    terminal_at: datetime | None = None

    @property
    def is_complex(self) -> bool:
        return self.complex_order_id is not None


@dataclass
class BrokerTransaction:
    """One row from the broker's transaction history — cash-truth."""

    id: str  # -> tt_transaction_id
    account_number: str
    order_id: str | None = None  # -> tt_order_id (the deterministic txn->order link key)
    underlying_symbol: str | None = None
    symbol: str | None = None
    instrument_type: str | None = None
    transaction_type: str | None = None
    transaction_sub_type: str | None = None
    action: str | None = None
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
    executed_at: datetime | None = None
    transaction_date: date | None = None


@dataclass
class BrokerPosition:
    """One row from the broker's current positions snapshot."""

    account_number: str
    symbol: str
    quantity: Decimal
    quantity_direction: str
    underlying_symbol: str | None = None
    instrument_type: str | None = None
    average_open_price: Decimal | None = None
    mark_price: Decimal | None = None
    close_price: Decimal | None = None
    # TastyTrade's real CurrentPosition has no unrealized-pnl field either (verified against
    # their OpenAPI spec) — only mark/mark-price and average-open-price. PositionRepository
    # derives unrealized_pnl itself from those, same reasoning as the Order fields above.
    #
    # realized_day_gain is the *magnitude* (non-negative) — TastyTrade splits it into a magnitude
    # + a Credit/Debit/None effect string, the same value/value-effect convention BrokerTransaction
    # already uses. PositionRepository combines the two into one signed value (the internal
    # ``positions`` table has a single realized_day_gain column, no separate effect column).
    realized_day_gain: Decimal | None = None
    realized_day_gain_effect: str | None = None  # "Credit" | "Debit" | "None"
    multiplier: int = 1
    expires_at: datetime | None = None


@dataclass
class BalanceMessage:
    """An account-balance snapshot — from the broker account-stream, the host platform's pub/sub,
    or a REST pull (``get_balances``).

    The typed fields are parsed by whichever source produced the message (each source owns its own
    wire format: dasherized broker JSON vs the host's snake_case envelope); ``raw`` keeps the
    source's original payload for audit. ``StreamConsumer`` persists these to ``balance_snapshots``
    (throttled) and still forwards to the optional ``on_balance`` hook."""

    account_number: str
    raw: dict
    net_liquidating_value: Decimal | None = None
    cash_balance: Decimal | None = None
    equity_buying_power: Decimal | None = None
    derivative_buying_power: Decimal | None = None
    maintenance_requirement: Decimal | None = None
    pending_cash: Decimal | None = None
    day_trading_buying_power: Decimal | None = None
    captured_at: datetime | None = None


@runtime_checkable
class BrokerClient(Protocol):
    """What ``ingest/pull.py`` needs from a broker. ``account_number`` is broker-native — the
    caller (``ingest/pull.py``) is responsible for translating a nickname via ``AccountMapper``
    before calling in; this Protocol never sees a nickname."""

    async def get_order_history(
        self, account_number: str, start: date, end: date,
    ) -> list[PlacedOrder]:
        """GET /accounts/{account}/orders — page-offset pagination, fully paged internally."""
        ...

    async def get_transaction_history(
        self, account_number: str, start: date, end: date,
    ) -> list[BrokerTransaction]:
        """GET /accounts/{account}/transactions — page-offset pagination, fully paged internally."""
        ...

    async def get_positions(self, account_number: str) -> list[BrokerPosition]:
        """GET /accounts/{account}/positions — the current snapshot."""
        ...

    async def get_balances(self, account_number: str) -> BalanceMessage:
        """GET /accounts/{account}/balances — the current balance snapshot."""
        ...
