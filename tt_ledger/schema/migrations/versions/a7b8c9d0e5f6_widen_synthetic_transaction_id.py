"""widen transactions.tt_transaction_id to Text

``tt_transaction_id`` holds two different kinds of value. Broker-supplied ids are
short and bounded. But the ids WE synthesize for settlements are composed —
``lapse-<account>-<group_pk>-<security_id>`` and
``paper-settle-<account>-<security_id>-<expiry>`` — from parts with no width we
control, and String(64) is already exhausted: the longest row in production is
exactly 64 characters, and a futures-option security_id
(``future_option:ES:M6:2026-06-15:put:7355``, 39 chars) lands a lapse id right on
the boundary.

It went over the edge when ``paper-settle-`` was account-scoped (2026-08-14): that
key had to carry the account because the UNIQUE index on ``tt_transaction_id`` is
global, so one account's settlement was silently overwriting every other
account's. The scoped key is 69 characters and every insert died with
StringDataRightTruncationError — a column width blocking a correctness fix.

Same reasoning and same mechanics as ``e5f6a7b8c2d3`` (widen broker text columns):
in PostgreSQL, Text and varchar share storage, so varchar->text is a catalog-only
change — no table rewrite and, importantly here, no rebuild of the UNIQUE index
this column carries.

Revision ID: a7b8c9d0e5f6
Revises: f6a7b8c9d4e5
Create Date: 2026-08-14 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'a7b8c9d0e5f6'
down_revision: str | None = 'f6a7b8c9d4e5'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "transactions",
        "tt_transaction_id",
        existing_type=sa.String(length=64),
        type_=sa.Text(),
        existing_nullable=False,
    )


def downgrade() -> None:
    # Lossy in principle (scoped settlement ids exceed 64); truncate explicitly
    # rather than let the server refuse the whole ALTER.
    op.alter_column(
        "transactions",
        "tt_transaction_id",
        existing_type=sa.Text(),
        type_=sa.String(length=64),
        existing_nullable=False,
        postgresql_using="substring(tt_transaction_id for 64)",
    )
