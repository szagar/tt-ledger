"""SQLAlchemy schema (docs/schema.md). Import side-effect registers all models on
``Base.metadata`` so Alembic autogenerate and ``create_all`` see them."""

from __future__ import annotations

from . import models  # noqa: F401  (registers tables on Base.metadata)
from ._base import Base

metadata = Base.metadata

__all__ = ["Base", "metadata", "models"]
