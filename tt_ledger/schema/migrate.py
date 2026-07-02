"""Programmatic Alembic entry point — migrate an installed tt-ledger without the repo checkout.

``alembic.ini``'s ``script_location`` is repo-relative, so ``alembic upgrade head`` only works
from a source checkout. Hosts that install tt-ledger as a wheel/git dependency use this module
(or the ``tt-ledger db upgrade`` CLI) instead: the script location is resolved from the
installed package.

These functions are synchronous — the Alembic env runs its own ``asyncio.run()`` — so call
them from a sync context (or via ``asyncio.to_thread`` inside a running event loop).
"""

from __future__ import annotations

from pathlib import Path

__all__ = ["upgrade_to_head"]


def _alembic_config(url: str):
    from alembic.config import Config

    cfg = Config()
    cfg.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
    cfg.set_main_option("path_separator", "os")
    cfg.set_main_option("sqlalchemy.url", url)  # env.py prefers this over TT_LEDGER_DATABASE_URL
    return cfg


def upgrade_to_head(url: str) -> None:
    """Apply all pending migrations to the database at ``url`` (idempotent).

    On Postgres this creates the ledger schema (see ``schema/namespace.py``) and keeps the
    version table as ``<schema>.tt_ledger_alembic_version``, so it coexists with a host
    platform's own Alembic chain on the same database.
    """
    from alembic import command

    command.upgrade(_alembic_config(url), "head")
