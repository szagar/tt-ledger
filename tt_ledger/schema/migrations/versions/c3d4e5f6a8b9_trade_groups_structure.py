"""trade_groups structure — submit-time structure descriptor (opaque host JSON)

Revision ID: c3d4e5f6a8b9
Revises: b2c3d4e5f6a7
Create Date: 2026-07-02 00:00:00.000000
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a8b9'
down_revision: str | None = 'b2c3d4e5f6a7'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('trade_groups', schema=None) as batch_op:
        batch_op.add_column(sa.Column('structure', sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('trade_groups', schema=None) as batch_op:
        batch_op.drop_column('structure')
