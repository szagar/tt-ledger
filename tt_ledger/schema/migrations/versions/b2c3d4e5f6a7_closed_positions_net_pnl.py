"""closed_positions net P&L — fees + pnl_net columns

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-07-02 00:00:00.000000
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa  # noqa: F401 -- conventional in migration files

from alembic import op
from tt_ledger.money import Money

# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: str | None = 'a1b2c3d4e5f6'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('closed_positions', schema=None) as batch_op:
        batch_op.add_column(sa.Column('fees', Money(scale=6), nullable=True))
        batch_op.add_column(sa.Column('pnl_net', Money(scale=6), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('closed_positions', schema=None) as batch_op:
        batch_op.drop_column('pnl_net')
        batch_op.drop_column('fees')
