"""FastAPI app factory (docs/api.md → HTTP server). Requires the ``[api]`` extra.

Endpoints: GET /orders, GET /trades, GET /trades/{group_id},
GET /accounts/{nickname}/activity, POST /trades/{group_id}/{remap,regroup,dismiss},
POST /ingest/{source_system} (reserved, 501). DTOs are Pydantic models over the consolidated
views (``api/schemas.py``); routes live in ``api/routes.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..sdk import LedgerClient


def create_app(client: "LedgerClient"):
    """Build the FastAPI app bound to a LedgerClient. ``fastapi`` imported lazily."""
    try:
        from fastapi import FastAPI
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("FastAPI server needs the [api] extra: pip install tt-ledger[api]") from exc

    from .routes import router

    app = FastAPI(title="tt-ledger", version="0.1.0")
    app.state.client = client
    app.include_router(router)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
