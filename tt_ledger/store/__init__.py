"""Pluggable store (docs/storage.md)."""

from __future__ import annotations

from .base import LedgerStore
from .memory import InMemoryStore
from .sql import SqlLedgerStore


def make_store(
    url: str = "sqlite+aiosqlite:///ledger.db",
    *,
    pool_size: int | None = None,
    max_overflow: int | None = None,
    pool_timeout: int | None = None,
) -> LedgerStore:
    """Default factory: a SQL store bound to ``url`` (SQLite default, Postgres opt-in).

    The pool bounds are forwarded to ``SqlLedgerStore``; see its docstring for why a
    host running many stores against one server should set them.
    """
    return SqlLedgerStore(
        url, pool_size=pool_size, max_overflow=max_overflow, pool_timeout=pool_timeout
    )


__all__ = ["LedgerStore", "SqlLedgerStore", "InMemoryStore", "make_store"]
