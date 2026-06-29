"""Declarative base + shared metadata for the ORM models."""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """All tt-ledger ORM models inherit from this; ``Base.metadata`` drives Alembic."""
