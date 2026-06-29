"""tt-ledger — portable broker order/transaction/fill/position ledger.

Pluggable store (SQLite bundled, Postgres opt-in). See docs/ for the full design.

The base package imports with only the base dependencies (sqlalchemy, pydantic).
Optional extras (tastytrade, postgres, api, redis, cli) are imported lazily by the
modules that need them, so `import tt_ledger` works without them installed.
"""

from __future__ import annotations

__version__ = "0.1.0"

from .money import Money
from .sdk import LedgerClient

__all__ = ["LedgerClient", "Money", "__version__"]
