"""trade_groups.status index — the hot analytic filter

Closed-trade analytics (host cockpit PnL / bot-performance queries) all lead with
``WHERE status = 'closed'`` on trade_groups. Only ``review_status`` was indexed;
plain ``status`` was not, so those queries seq-scan. Invisible while the table is
small, but it degrades every dashboard query as closed groups accumulate. Mirrors
the existing ``ix_trade_groups_review_status`` (plain btree, cross-dialect).

Revision ID: f6a7b8c9d4e5
Revises: e5f6a7b8c2d3
Create Date: 2026-07-19 00:00:00.000000
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa  # noqa: F401 -- conventional in migration files

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'f6a7b8c9d4e5'
down_revision: str | None = 'e5f6a7b8c2d3'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('trade_groups', schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f('ix_trade_groups_status'), ['status'], unique=False
        )


def downgrade() -> None:
    with op.batch_alter_table('trade_groups', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_trade_groups_status'))
