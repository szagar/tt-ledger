"""Pydantic DTOs over the ``*Row`` shapes (docs/api.md -> HTTP server).

Deliberately a separate layer from ``rows.py`` (see that module's own docstring) — the wire
contract stays stable even if the internal row dataclasses' shape changes. Only imported once
``create_app`` actually runs (the ``[api]`` extra owns the ``fastapi``/``pydantic`` model
machinery import cost), never at package import time.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from ..enums import Ingest, Origin, ReviewStatus


class OrderDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    tt_order_id: str | None
    account: str
    origin: Origin
    ingest: Ingest
    oms_order_id: str | None = None
    client_order_id: str | None = None
    account_number: str | None = None
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
    tt_status: str | None = None
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


class TradeDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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
    order_id: int | None = None
    strategy_id: int | None = None
    bot_name: str | None = None
    signal_id: str | None = None
    executed_at: datetime | None = None
    closed_at: datetime | None = None


class ActivityDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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
    commission: Decimal | None = None
    clearing_fees: Decimal | None = None
    regulatory_fees: Decimal | None = None
    executed_at: datetime | None = None
    order_id: int | None = None
    tt_order_id: str | None = None
    trade_group_id: int | None = None
    origin: Origin | None = None
    review_status: ReviewStatus | None = None


class TradeDetailDTO(TradeDTO):
    """``TradeDTO`` + its orders and transactions (legs/fills/events aren't included -- no
    query-by-order/query-by-group method exists for those yet)."""

    orders: list[OrderDTO] = []
    transactions: list[ActivityDTO] = []


class RemapRequest(BaseModel):
    strategy: int | None = None
    bot: str | None = None
    signal: str | None = None
    strategy_type: str | None = None
    reviewed_by: str


class RegroupRequest(BaseModel):
    txn_ids: list[int]
    target: str | None = None
    reviewed_by: str


class DismissRequest(BaseModel):
    reviewed_by: str


__all__ = [
    "OrderDTO", "TradeDTO", "ActivityDTO", "TradeDetailDTO",
    "RemapRequest", "RegroupRequest", "DismissRequest",
]
