"""ORM models — the complete schema from docs/schema.md.

Conventions:
  * ``Money`` (tt_ledger.money) on every price/value/fee/pnl column — exact on both backends.
  * ``DateTime(timezone=True)`` for timestamps (UTC, Python-side defaults for cross-dialect).
  * generic ``JSON`` (maps to JSONB on PG, JSON/TEXT on SQLite) — never PG-only JSONB.
  * enum columns stored as ``String`` (the enum ``.value``) to avoid native-ENUM dialect churn.
  * integer surrogate PK + a natural unique key per table for idempotent upsert.

FK notes for the *standalone* schema:
  * ``strategy_id`` / ``market_context_id`` are plain indexed ints (those tables live in a host
    platform, not here — see docs/integration-zts.md).
  * ``orders.trade_group_id`` is a plain indexed int (no DB FK) to break the orders↔trade_groups
    cycle, which SQLite cannot express via ALTER. ``trade_groups.order_id`` keeps the real FK.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from ..money import Money
from ._base import Base


def _now() -> datetime:
    return datetime.now(UTC)


# ----------------------------------------------------------------------------- dimensions


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(primary_key=True)
    nickname: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    account_number: Mapped[str] = mapped_column(String(32), unique=True)
    login: Mapped[str] = mapped_column(String(64), index=True)
    env: Mapped[str] = mapped_column(String(8), default="live")  # live | paper
    source_system: Mapped[str] = mapped_column(String(32), default="tastytrade")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class Security(Base):
    __tablename__ = "securities"

    id: Mapped[int] = mapped_column(primary_key=True)
    security_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)  # resolver output (default: vendor symbol)
    product_type: Mapped[str] = mapped_column(String(2))  # S/I/F/OS/OI/OF/CR
    underlying: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)
    expiry: Mapped[date | None] = mapped_column(Date, nullable=True)
    strike: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    option_type: Mapped[str | None] = mapped_column(String(1), nullable=True)  # P | C
    multiplier: Mapped[int | None] = mapped_column(Integer, nullable=True)
    exchange: Mapped[str | None] = mapped_column(String(16), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    tt_symbol: Mapped[str | None] = mapped_column(String(100), index=True, nullable=True)
    occ_symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    streamer_symbol: Mapped[str | None] = mapped_column(String(50), index=True, nullable=True)
    source_system: Mapped[str] = mapped_column(String(32), default="tastytrade")
    meta_json: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    last_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


# ----------------------------------------------------------------------------- facts


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        Index(
            "uq_orders_tt_order_id", "tt_order_id", unique=True,
            sqlite_where=text("tt_order_id IS NOT NULL"),
            postgresql_where=text("tt_order_id IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tt_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    oms_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    client_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    account: Mapped[str] = mapped_column(ForeignKey("accounts.nickname"), index=True)
    account_number: Mapped[str | None] = mapped_column(String(32), nullable=True)  # audit
    origin: Mapped[str] = mapped_column(String(8))  # zts | broker
    ingest: Mapped[str] = mapped_column(String(16))  # oms_submit | order_history | import
    source_system: Mapped[str] = mapped_column(String(32), default="tastytrade")
    security_id: Mapped[str | None] = mapped_column(ForeignKey("securities.security_id"), nullable=True)
    underlying: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    order_type: Mapped[str | None] = mapped_column(String(24), nullable=True)
    time_in_force: Mapped[str | None] = mapped_column(String(16), nullable=True)
    gtc_date: Mapped[str | None] = mapped_column(String(10), nullable=True)
    price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    stop_trigger: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    price_effect: Mapped[str | None] = mapped_column(String(8), nullable=True)
    average_fill_price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    is_complex: Mapped[bool] = mapped_column(Boolean, default=False)
    complex_order_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    oms_status: Mapped[str | None] = mapped_column(String(24), nullable=True)
    tt_status: Mapped[str | None] = mapped_column(String(24), nullable=True)
    status_message: Mapped[str | None] = mapped_column(String(256), nullable=True)
    filled_quantity: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    remaining_quantity: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    signal_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    trace_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    strategy_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)  # soft ref
    trade_group_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)  # soft ref (cycle break)
    market_context_id: Mapped[int | None] = mapped_column(Integer, nullable=True)  # soft ref
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class OrderLeg(Base):
    __tablename__ = "order_legs"
    __table_args__ = (Index("uq_order_legs_order_index", "order_id", "leg_index", unique=True),)

    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), index=True)
    leg_index: Mapped[int] = mapped_column(Integer, default=0)
    security_id: Mapped[str] = mapped_column(ForeignKey("securities.security_id"), index=True)
    action: Mapped[str | None] = mapped_column(String(20), nullable=True)
    quantity: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    remaining_quantity: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    quantity_direction: Mapped[str | None] = mapped_column(String(8), nullable=True)
    price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    fill_price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class OrderFill(Base):
    __tablename__ = "order_fills"

    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), index=True)
    order_leg_id: Mapped[int | None] = mapped_column(ForeignKey("order_legs.id"), nullable=True, index=True)
    tt_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    fill_id: Mapped[str] = mapped_column(String(64), unique=True)
    quantity: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    fill_price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    destination_venue: Mapped[str | None] = mapped_column(String(32), nullable=True)
    ext_exec_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ext_group_fill_id: Mapped[str | None] = mapped_column(String(64), nullable=True)


class Position(Base):
    __tablename__ = "positions"
    __table_args__ = (Index("uq_positions_account_security", "account", "security_id", unique=True),)

    id: Mapped[int] = mapped_column(primary_key=True)
    account: Mapped[str] = mapped_column(ForeignKey("accounts.nickname"), index=True)
    security_id: Mapped[str] = mapped_column(ForeignKey("securities.security_id"), index=True)
    quantity: Mapped[Decimal] = mapped_column(Money())
    quantity_direction: Mapped[str] = mapped_column(String(8))
    average_open_price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    mark_price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    close_price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    unrealized_pnl: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    realized_day_gain: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    multiplier: Mapped[int] = mapped_column(Integer, default=1)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    strategy_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)  # soft ref
    opening_order_id: Mapped[int | None] = mapped_column(ForeignKey("orders.id"), nullable=True, index=True)
    trade_group_id: Mapped[int | None] = mapped_column(ForeignKey("trade_groups.id"), nullable=True, index=True)
    position_opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    last_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class ClosedPosition(Base):
    __tablename__ = "closed_positions"

    id: Mapped[int] = mapped_column(primary_key=True)
    account: Mapped[str] = mapped_column(ForeignKey("accounts.nickname"), index=True)
    security_id: Mapped[str] = mapped_column(ForeignKey("securities.security_id"), index=True)
    quantity: Mapped[Decimal] = mapped_column(Money())
    quantity_direction: Mapped[str] = mapped_column(String(8))
    average_open_price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    average_close_price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    realized_pnl: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    opening_order_id: Mapped[int | None] = mapped_column(ForeignKey("orders.id"), nullable=True)
    closing_order_id: Mapped[int | None] = mapped_column(ForeignKey("orders.id"), nullable=True)
    trade_group_id: Mapped[int | None] = mapped_column(ForeignKey("trade_groups.id"), nullable=True, index=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    holding_period_days: Mapped[int | None] = mapped_column(Integer, nullable=True)


class TradeGroup(Base):
    __tablename__ = "trade_groups"

    id: Mapped[int] = mapped_column(primary_key=True)
    group_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    account: Mapped[str] = mapped_column(ForeignKey("accounts.nickname"), index=True)
    origin: Mapped[str] = mapped_column(String(8))  # zts | broker
    source_system: Mapped[str] = mapped_column(String(32), default="tastytrade")
    review_status: Mapped[str] = mapped_column(String(16), default="needs_review", index=True)
    manually_attributed: Mapped[bool] = mapped_column(Boolean, default=False)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reviewed_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    underlying: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    security_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    strategy_type: Mapped[str | None] = mapped_column(String(24), nullable=True)
    leg_count: Mapped[int] = mapped_column(Integer, default=1)
    total_premium: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    quantity: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    total_fees: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="open")
    realized_pnl: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    unrealized_pnl: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    max_profit: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    max_loss: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    profit_target: Mapped[str | None] = mapped_column(String(32), nullable=True)
    stop_loss: Mapped[str | None] = mapped_column(String(32), nullable=True)
    exit_strategy: Mapped[str | None] = mapped_column(String(100), nullable=True)
    order_id: Mapped[int | None] = mapped_column(ForeignKey("orders.id"), nullable=True, index=True)
    strategy_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)  # soft ref
    bot_name: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    signal_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class TradeGroupEvent(Base):
    __tablename__ = "trade_group_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    trade_group_id: Mapped[int] = mapped_column(ForeignKey("trade_groups.id"), index=True)
    event_type: Mapped[str] = mapped_column(String(16))
    quantity_change: Mapped[Decimal] = mapped_column(Money(), default=Decimal("0"))
    premium_change: Mapped[Decimal] = mapped_column(Money(), default=Decimal("0"))
    realized_pnl: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    event_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    rolled_to_group_id: Mapped[int | None] = mapped_column(ForeignKey("trade_groups.id"), nullable=True, index=True)
    transaction_id: Mapped[int | None] = mapped_column(ForeignKey("transactions.id"), nullable=True, index=True)
    order_id: Mapped[int | None] = mapped_column(ForeignKey("orders.id"), nullable=True, index=True)


class Transaction(Base):
    __tablename__ = "transactions"
    __table_args__ = (
        Index("ix_transactions_account_executed", "account", "executed_at"),
        Index("ix_transactions_account_date", "account", "transaction_date"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tt_transaction_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    tt_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    account: Mapped[str] = mapped_column(ForeignKey("accounts.nickname"), index=True)
    account_number: Mapped[str | None] = mapped_column(String(32), nullable=True)  # audit
    source_system: Mapped[str] = mapped_column(String(32), default="tastytrade")
    transaction_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    transaction_sub_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    action: Mapped[str | None] = mapped_column(String(30), nullable=True)
    security_id: Mapped[str | None] = mapped_column(ForeignKey("securities.security_id"), nullable=True)
    underlying: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    quantity: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    price: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    value: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    value_effect: Mapped[str | None] = mapped_column(String(10), nullable=True)
    net_value: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    net_value_effect: Mapped[str | None] = mapped_column(String(10), nullable=True)
    commission: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    clearing_fees: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    regulatory_fees: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    proprietary_index_option_fees: Mapped[Decimal | None] = mapped_column(Money(), nullable=True)
    is_estimated_fee: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    order_id: Mapped[int | None] = mapped_column(ForeignKey("orders.id"), nullable=True, index=True)
    order_leg_id: Mapped[int | None] = mapped_column(ForeignKey("order_legs.id"), nullable=True, index=True)
    position_id: Mapped[int | None] = mapped_column(ForeignKey("positions.id"), nullable=True, index=True)
    closed_position_id: Mapped[int | None] = mapped_column(ForeignKey("closed_positions.id"), nullable=True, index=True)
    trade_group_id: Mapped[int | None] = mapped_column(ForeignKey("trade_groups.id"), nullable=True, index=True)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    transaction_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


__all__ = [
    "Account", "Security", "Order", "OrderLeg", "OrderFill", "Transaction",
    "Position", "ClosedPosition", "TradeGroup", "TradeGroupEvent",
]
