"""Optional FastAPI read/ingest server (docs/api.md). Requires the ``[api]`` extra.

``fastapi`` is imported lazily inside ``create_app`` so ``import tt_ledger`` works without it.
"""

from __future__ import annotations

from .app import create_app

__all__ = ["create_app"]
