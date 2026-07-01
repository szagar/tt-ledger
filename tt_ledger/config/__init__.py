"""Config loaders. See docs/identity.md.

Accounts: ``AccountMapper.from_toml(path)`` (in ``tt_ledger.identity``) loads
``config/accounts.toml``. Securities: ``load_securities_toml`` loads the human-curated
symbology rules + universes from ``config/securities.toml`` (NOT a per-instrument map —
that is algorithmic and lives in the ``securities`` table).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from .._toml import load as _load_toml


@dataclass
class SecurityRules:
    """Human-curated symbology config (docs/identity.md → Rule 2)."""

    index_option_roots: dict[str, list[str]] = field(default_factory=dict)  # {"SPX": ["SPX","SPXW"]}
    futures: dict[str, dict] = field(default_factory=dict)                  # {"ES": {"exchange":..,"multiplier":..}}
    universes: dict[str, list[str]] = field(default_factory=dict)          # {"core_equity": ["S|SPY", ...]}


def load_securities_toml(path: str | os.PathLike[str]) -> SecurityRules:
    """Parse config/securities.toml into SecurityRules.

    Only consumed by a resolver you inject (docs/identity.md → Rule 2) — the default
    ``PassthroughResolver`` ignores this file entirely.
    """
    data = _load_toml(path)
    return SecurityRules(
        index_option_roots=data.get("index_option_roots", {}),
        futures=data.get("futures", {}),
        universes=data.get("universes", {}),
    )


__all__ = ["SecurityRules", "load_securities_toml"]
