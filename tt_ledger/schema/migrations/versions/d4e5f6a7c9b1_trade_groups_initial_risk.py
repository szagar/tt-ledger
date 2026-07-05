"""trade_groups initial_risk — planned 1R in dollars, frozen at open

The R-multiple denominator (Tharp): where the host intends to stop, which may be
tighter than the structural worst case (max_loss). Written once by
open_trade_group; no write path recomputes it.

Revision ID: d4e5f6a7c9b1
Revises: c3d4e5f6a8b9
Create Date: 2026-07-05 00:00:00.000000
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa  # noqa: F401 -- conventional in migration files

from alembic import op
from tt_ledger.money import Money

# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7c9b1'
down_revision: str | None = 'c3d4e5f6a8b9'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('trade_groups', schema=None) as batch_op:
        batch_op.add_column(sa.Column('initial_risk', Money(scale=6), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('trade_groups', schema=None) as batch_op:
        batch_op.drop_column('initial_risk')
