"""The installed-package migration entry point (schema/migrate.py) and the Postgres
schema namespace (schema/namespace.py).

SQLite cases always run; the Postgres case needs ``TT_LEDGER_TEST_PG`` (same opt-in as
``store_url``) and uses a throwaway ``TT_LEDGER_PG_SCHEMA`` so it never collides with the
fixture-created tables in the default ``ledger`` schema.
"""

from __future__ import annotations

import asyncio
import os

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.ext.asyncio import create_async_engine

from tt_ledger.schema.migrate import upgrade_to_head
from tt_ledger.schema.namespace import pg_schema, translate_map_for

_PG_URL = os.getenv("TT_LEDGER_TEST_PG")

_EXPECTED_TABLES = {
    "accounts",
    "securities",
    "orders",
    "order_legs",
    "order_fills",
    "positions",
    "closed_positions",
    "trade_groups",
    "trade_group_events",
    "transactions",
    "balance_snapshots",
}


# ------------------------------------------------------------------ namespace helpers


def test_translate_map_only_on_postgres():
    assert translate_map_for("sqlite+aiosqlite:///x.db") is None
    assert translate_map_for("postgresql+asyncpg://u@h/db") == {None: "ledger"}


def test_pg_schema_env_override(monkeypatch):
    monkeypatch.setenv("TT_LEDGER_PG_SCHEMA", "custom_ns")
    assert pg_schema() == "custom_ns"
    assert translate_map_for("postgresql+asyncpg://u@h/db") == {None: "custom_ns"}


# ------------------------------------------------------------------ SQLite migrations


def test_upgrade_to_head_sqlite_idempotent(tmp_path):
    db_path = tmp_path / "migrate.db"
    upgrade_to_head(f"sqlite+aiosqlite:///{db_path}")
    upgrade_to_head(f"sqlite+aiosqlite:///{db_path}")  # a second run must be a no-op, not an error

    engine = create_engine(f"sqlite:///{db_path}")  # stdlib sqlite3 driver, inspection only
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    assert _EXPECTED_TABLES <= tables
    # host-coexistence guarantee: the version table is namespaced, not "alembic_version"
    assert "tt_ledger_alembic_version" in tables
    assert "alembic_version" not in tables


# ------------------------------------------------------------------ Postgres namespace


@pytest.mark.skipif(not _PG_URL, reason="TT_LEDGER_TEST_PG not set")
async def test_upgrade_to_head_postgres_lands_in_ledger_schema(monkeypatch):
    schema = "tt_ledger_migrate_test"
    monkeypatch.setenv("TT_LEDGER_PG_SCHEMA", schema)

    def _tables(conn, target_schema: str) -> set[str]:
        return set(inspect(conn).get_table_names(schema=target_schema))

    engine = create_async_engine(_PG_URL)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))

        # upgrade_to_head runs its own event loop (Alembic env) -> off-thread from async tests
        await asyncio.to_thread(upgrade_to_head, _PG_URL)
        await asyncio.to_thread(upgrade_to_head, _PG_URL)  # idempotent

        async with engine.connect() as conn:
            tables = await conn.run_sync(_tables, schema)
            public_tables = await conn.run_sync(_tables, "public")
        assert _EXPECTED_TABLES <= tables
        assert "tt_ledger_alembic_version" in tables
        # nothing leaked into public
        assert not (_EXPECTED_TABLES & public_tables)
        assert "tt_ledger_alembic_version" not in public_tables

        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    finally:
        await engine.dispose()
