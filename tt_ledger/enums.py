"""Domain enums. See docs/schema.md → Enums. Implemented for real (concrete values)."""

from __future__ import annotations

from enum import StrEnum


class Origin(StrEnum):
    ZTS = "zts"          # initiated by the automated host system
    BROKER = "broker"    # placed directly at the broker / any non-automated source


class Ingest(StrEnum):
    OMS_SUBMIT = "oms_submit"
    ORDER_HISTORY = "order_history"
    IMPORT = "import"


class ReviewStatus(StrEnum):
    NEEDS_REVIEW = "needs_review"
    CONFIRMED = "confirmed"
    IGNORED = "ignored"


class OrderStatus(StrEnum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    WORKING = "working"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TradeGroupStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    EXPIRED = "expired"
    ASSIGNED = "assigned"
    EXERCISED = "exercised"
    MIXED = "mixed"


class StrategyType(StrEnum):
    SINGLE = "single"
    VERTICAL = "vertical"
    CALENDAR = "calendar"
    DIAGONAL = "diagonal"
    IRON_CONDOR = "iron_condor"
    IRON_BUTTERFLY = "iron_butterfly"
    STRADDLE = "straddle"
    STRANGLE = "strangle"
    BUTTERFLY = "butterfly"
    CONDOR = "condor"
    RATIO = "ratio"
    COVERED = "covered"
    COLLAR = "collar"
    CUSTOM = "custom"
    FUTURE = "future"
    FUTURE_SPREAD = "future_spread"


class TradeGroupEventType(StrEnum):
    ENTRY = "entry"
    PARTIAL_EXIT = "partial_exit"
    FULL_EXIT = "full_exit"
    ROLL = "roll"
    ADJUSTMENT = "adjustment"
    EXPIRATION = "expiration"
    ASSIGNMENT = "assignment"
    EXERCISE = "exercise"


class ProductType(StrEnum):
    S = "S"    # stock
    I = "I"    # index
    F = "F"    # future
    OS = "OS"  # option on stock
    OI = "OI"  # option on index
    OF = "OF"  # option on future
    CR = "CR"  # crypto
