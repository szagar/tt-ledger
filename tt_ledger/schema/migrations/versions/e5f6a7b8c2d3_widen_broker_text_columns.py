"""widen broker-supplied string columns to Text

Broker-supplied values (order/transaction vocab, status/reject text, venue names,
vendor symbols) have no width we control — a guessed varchar length truncates when
TT expands its vocabulary. This first bit the ``orders.complex_order_type`` column
(String(16)) on the first real complex order: the broker complex-order TAG
``"OTOCO::trigger-order"`` (20 chars) overflowed and aborted every ``sync_orders``
cycle with StringDataRightTruncationError. Convert the whole class to Text (in PG,
Text and varchar are the same storage; varchar->text is a catalog-only change, no
table rewrite, no index rebuild).

Revision ID: e5f6a7b8c2d3
Revises: d4e5f6a7c9b1
Create Date: 2026-07-13 00:00:00.000000
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'e5f6a7b8c2d3'
down_revision: str | None = 'd4e5f6a7c9b1'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# (table, column, original varchar length) — broker-supplied, variable/expandable.
_COLUMNS: tuple[tuple[str, str, int], ...] = (
    ("orders", "order_type", 24),
    ("orders", "time_in_force", 16),
    ("orders", "complex_order_type", 16),
    ("orders", "tt_status", 24),
    ("orders", "status_message", 256),
    ("order_fills", "destination_venue", 32),
    ("transactions", "transaction_type", 50),
    ("transactions", "transaction_sub_type", 50),
    ("transactions", "action", 30),
    ("transactions", "description", 500),
    ("securities", "tt_symbol", 100),
    ("securities", "occ_symbol", 32),
    ("securities", "streamer_symbol", 50),
)

# SQLite has no ``ALTER COLUMN ... TYPE`` (the statement is a syntax error there), and it
# would not need one: VARCHAR(n) carries TEXT affinity and SQLite never enforces the
# length, so the truncation this migration exists to prevent cannot happen on that
# dialect. Widening is therefore a genuine no-op on SQLite, not a skipped step — the
# cross-dialect END STATE is identical. Guarding here keeps the chain runnable on both
# backends (the dual-dialect guarantee CI asserts) without a table-rebuild batch
# migration that would drop and recreate this column's indexes.
def _needs_widening() -> bool:
    return op.get_bind().dialect.name != "sqlite"


def upgrade() -> None:
    if not _needs_widening():
        return
    for table, column, length in _COLUMNS:
        op.alter_column(
            table, column,
            existing_type=sa.String(length=length),
            type_=sa.Text(),
            existing_nullable=True,
        )


def downgrade() -> None:
    # varchar -> text is lossless; going back can truncate, so cast explicitly.
    if not _needs_widening():
        return
    for table, column, length in _COLUMNS:
        op.alter_column(
            table, column,
            existing_type=sa.Text(),
            type_=sa.String(length=length),
            existing_nullable=True,
            postgresql_using=f"substring({column} for {length})",
        )
