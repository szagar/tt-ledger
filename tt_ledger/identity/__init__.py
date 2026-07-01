"""Identity subsystems (docs/identity.md):

* **accounts** — nickname↔account-number (``AccountMapper``).
* **securities** — an *injectable* broker-symbol→canonical-``security_id`` resolver. Default is
  :class:`PassthroughResolver` (canonical == vendor symbol). Optional adapters:
  :class:`SecurityUniverseResolver` (``security-universe`` is lazy-imported on use) and
  :class:`CanonicalSymbolResolver` (ZTS-style structured scheme, no extra dependency).
"""

from __future__ import annotations

from .accounts import AccountMapper, LoginConfig
from .canonical import CanonicalSymbol, CanonicalSymbolResolver
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
    "CanonicalSymbol",
    "CanonicalSymbolResolver",
]
