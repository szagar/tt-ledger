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
    is_complex: bool = False
    complex_order_type: str | None = None
    # broker-reported aggregates (order-level, not derived from legs/fills client-side — a
    # multi-leg spread's net execution price isn't a plain average of its legs' fill prices).
    average_fill_price: Decimal | None = None
    filled_quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    updated_at: datetime | None = None
    terminal_at: datetime | None = None


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
    # broker-reported, not derived client-side (sign conventions for short positions and options/
    # futures multipliers make this easy to get subtly wrong; the broker already computes it).
    unrealized_pnl: Decimal | None = None
    realized_day_gain: Decimal | None = None
    multiplier: int = 1
    expires_at: datetime | None = None


@dataclass
class BalanceMessage:
    """A live account-balance update from the broker account-stream (docs/ingestion.md -> Push).
    No schema home in tt-ledger — it's an order/txn/fill/position ledger, not a full account/margin
    snapshot store — so this is never persisted; ``StreamConsumer`` only forwards it to an
    optional caller-supplied hook."""

    account_number: str
    raw: dict


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
