"""Config loaders. See docs/identity.md.

Accounts: ``AccountMapper.from_toml(path)`` (in ``tt_ledger.identity``) loads
``config/accounts.toml``. Securities: ``load_securities_toml`` loads the human-curated
symbology rules + universes from ``config/securities.toml`` (NOT a per-instrument map —
that is algorithmic and lives in the ``securities`` table).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SecurityRules:
    """Human-curated symbology config (docs/identity.md → Rule 2)."""

    index_option_roots: dict[str, list[str]] = field(default_factory=dict)  # {"SPX": ["SPX","SPXW"]}
    futures: dict[str, dict] = field(default_factory=dict)                  # {"ES": {"exchange":..,"multiplier":..}}
    universes: dict[str, list[str]] = field(default_factory=dict)          # {"core_equity": ["S|SPY", ...]}


def load_securities_toml(path: str) -> SecurityRules:
    """Parse config/securities.toml into SecurityRules. TODO: implement."""
    raise NotImplementedError("load_securities_toml — see docs/identity.md")


__all__ = ["SecurityRules", "load_securities_toml"]
