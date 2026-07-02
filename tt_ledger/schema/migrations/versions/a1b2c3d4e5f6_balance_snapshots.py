"""balance_snapshots — account balance time series

Revision ID: a1b2c3d4e5f6
Revises: 8f3a1c5e9b02
Create Date: 2026-07-02 00:00:00.000000
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from tt_ledger.money import Money

# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: str | None = '8f3a1c5e9b02'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table('balance_snapshots',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('account', sa.String(length=50), nullable=False),
    sa.Column('captured_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('source', sa.String(length=16), nullable=False),
    sa.Column('net_liquidating_value', Money(scale=6), nullable=True),
    sa.Column('cash_balance', Money(scale=6), nullable=True),
    sa.Column('equity_buying_power', Money(scale=6), nullable=True),
    sa.Column('derivative_buying_power', Money(scale=6), nullable=True),
    sa.Column('maintenance_requirement', Money(scale=6), nullable=True),
    sa.Column('pending_cash', Money(scale=6), nullable=True),
    sa.Column('day_trading_buying_power', Money(scale=6), nullable=True),
    sa.Column('raw', sa.JSON(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['account'], ['accounts.nickname'], ),
    sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('balance_snapshots', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_balance_snapshots_account'), ['account'], unique=False)
        batch_op.create_index(
            'uq_balance_snapshots_account_time_source', ['account', 'captured_at', 'source'], unique=True,
        )


def downgrade() -> None:
    op.drop_table('balance_snapshots')
