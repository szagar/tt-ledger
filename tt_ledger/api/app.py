"""FastAPI app factory (docs/api.md → HTTP server). Requires the ``[api]`` extra.

Endpoints (to implement): GET /orders, GET /trades, GET /trades/{group_id},
GET /accounts/{nickname}/activity, POST /trades/{group_id}/{remap,regroup,dismiss},
POST /ingest/{source_system}. DTOs are Pydantic models over the consolidated views.
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

    app = FastAPI(title="tt-ledger", version="0.1.0")

    # TODO: register routers (orders, trades, activity, remap, ingest) over `client`.

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
