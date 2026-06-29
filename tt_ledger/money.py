"""The ``Money`` SQLAlchemy type — exact decimals on both backends.

See docs/storage.md. The application always works in ``Decimal``; this decorator
delegates per dialect:

* Postgres → native ``NUMERIC(18, scale)`` (pass-through, exact, readable SQL).
* SQLite   → scaled ``INTEGER`` micro-units (Decimal↔int in bind/result) — SQLite has
             no DECIMAL and a bare ``Numeric`` round-trips through float and drifts.

This is the one fully-specified piece of the package and is implemented for real.
"""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Integer
from sqlalchemy.types import NUMERIC, TypeDecorator


class Money(TypeDecorator):
    """Exact monetary value. Apply to every price/value/fee/pnl column."""

    cache_ok = True
    impl = NUMERIC  # default; overridden per-dialect in load_dialect_impl

    def __init__(self, scale: int = 6) -> None:
        super().__init__()
        self.scale = scale

    def load_dialect_impl(self, dialect):  # noqa: ANN001
        if dialect.name == "postgresql":
            return dialect.type_descriptor(NUMERIC(18, self.scale))
        return dialect.type_descriptor(Integer())  # sqlite (and others): micro-units

    def process_bind_param(self, value, dialect):  # noqa: ANN001
        if value is None or dialect.name == "postgresql":
            return value
        return int((Decimal(str(value)) * (10**self.scale)).to_integral_value())

    def process_result_value(self, value, dialect):  # noqa: ANN001
        if value is None or dialect.name == "postgresql":
            return value
        return Decimal(value) / (10**self.scale)
