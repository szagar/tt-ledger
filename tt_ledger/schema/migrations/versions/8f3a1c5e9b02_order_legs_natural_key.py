"""order_legs natural key (order_id, leg_index)

Revision ID: 8f3a1c5e9b02
Revises: 0126b2d5742a
Create Date: 2026-07-01 00:00:00.000000
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '8f3a1c5e9b02'
down_revision: str | None = '0126b2d5742a'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('order_legs', schema=None) as batch_op:
        batch_op.create_index(
            'uq_order_legs_order_index', ['order_id', 'leg_index'], unique=True,
        )


def downgrade() -> None:
    with op.batch_alter_table('order_legs', schema=None) as batch_op:
        batch_op.drop_index('uq_order_legs_order_index')
