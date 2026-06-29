"""Identity subsystems (docs/identity.md):

* **accounts** — nickname↔account-number (``AccountMapper``).
* **securities** — an *injectable* broker-symbol→canonical-``security_id`` resolver. Default is
  :class:`PassthroughResolver` (canonical == vendor symbol). Optional adapters:
  :class:`SecurityUniverseResolver` and ``CanonicalSymbolResolver`` (lazy-imported on use).
"""

from __future__ import annotations

from .accounts import AccountMapper, LoginConfig
from .securities import (
    PassthroughResolver,
    ResolvedSecurity,
    SecurityResolver,
    SecurityUniverseResolver,
)

__all__ = [
    "AccountMapper",
    "LoginConfig",
    "SecurityResolver",
    "ResolvedSecurity",
    "PassthroughResolver",
    "SecurityUniverseResolver",
]
