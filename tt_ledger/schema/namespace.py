"""Postgres schema namespace for the ledger tables.

On Postgres every ledger table (and the Alembic version table) lives in a dedicated
schema — default ``ledger``, overridable via ``TT_LEDGER_PG_SCHEMA`` — so a host
platform can share one database with its own tables without name collisions
(``accounts``, ``orders``, ``positions``, ``transactions`` are generic names).

The models themselves stay schema-less; the namespace is applied per-connection via
SQLAlchemy's ``schema_translate_map`` (``{None: PG_SCHEMA}``), which keeps the SQLite
backend — where schemas don't exist — completely untouched. The two places that create
engines (``SqlLedgerStore`` and the Alembic env) both route through the helpers here.
"""

from __future__ import annotations

import os

__all__ = ["pg_schema", "translate_map_for", "uses_pg_schema"]


def pg_schema() -> str:
    """The Postgres schema name ledger tables live in (env-overridable)."""
    return os.getenv("TT_LEDGER_PG_SCHEMA", "ledger")


def uses_pg_schema(url: str) -> bool:
    return url.startswith("postgresql")


def translate_map_for(url: str) -> dict[None, str] | None:
    """The ``schema_translate_map`` for ``url`` — ``{None: <schema>}`` on Postgres, else None."""
    return {None: pg_schema()} if uses_pg_schema(url) else None
