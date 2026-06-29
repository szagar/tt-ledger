"""Pluggable store (docs/storage.md)."""

from __future__ import annotations

from .base import LedgerStore
from .memory import InMemoryStore
from .sql import SqlLedgerStore


def make_store(url: str = "sqlite+aiosqlite:///ledger.db") -> LedgerStore:
    """Default factory: a SQL store bound to ``url`` (SQLite default, Postgres opt-in)."""
    return SqlLedgerStore(url)


__all__ = ["LedgerStore", "SqlLedgerStore", "InMemoryStore", "make_store"]
