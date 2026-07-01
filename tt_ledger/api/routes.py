"""HTTP routes (docs/api.md -> HTTP server) — thin wrappers over ``LedgerClient``.

Only imported by ``create_app`` (lazily), so ``fastapi`` is never imported at package-import time.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Query, Request

from .schemas import (
    ActivityDTO,
    DismissRequest,
    OrderDTO,
    RegroupRequest,
    RemapRequest,
    TradeDetailDTO,
    TradeDTO,
)

if TYPE_CHECKING:
    from ..sdk import LedgerClient

router = APIRouter()


def _client(request: Request) -> "LedgerClient":
    return request.app.state.client


# --- reads -------------------------------------------------------------------------------


@router.get("/orders", response_model=list[OrderDTO])
async def list_orders(
    request: Request,
    origin: str | None = None,
    account: str | None = None,
    status: str | None = None,
    underlying: str | None = None,
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
):
    f = {"origin": origin, "account": account, "status": status, "underlying": underlying, "start": date_from, "end": date_to}
    return await _client(request).orders(**{k: v for k, v in f.items() if v is not None})


@router.get("/trades", response_model=list[TradeDTO])
async def list_trades(
    request: Request,
    origin: str | None = None,
    review_status: str | None = None,
    status: str | None = None,
    account: str | None = None,
    underlying: str | None = None,
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
):
    f = {
        "origin": origin, "review_status": review_status, "status": status,
        "account": account, "underlying": underlying, "start": date_from, "end": date_to,
    }
    return await _client(request).trades(**{k: v for k, v in f.items() if v is not None})


@router.get("/trades/{group_id}", response_model=TradeDetailDTO)
async def get_trade(request: Request, group_id: str):
    client = _client(request)
    trade = await client.trade(group_id)
    if trade is None:
        raise HTTPException(status_code=404, detail=f"trade group {group_id!r} not found")
    orders, transactions = await client.trade_detail(group_id)
    return TradeDetailDTO(**vars(trade), orders=orders, transactions=transactions)


@router.get("/accounts/{nickname}/activity", response_model=list[ActivityDTO])
async def account_activity(
    request: Request,
    nickname: str,
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
    unreconciled_only: bool = False,
):
    f = {"start": date_from, "end": date_to, "unreconciled_only": unreconciled_only}
    return await _client(request).account_activity(nickname, **{k: v for k, v in f.items() if v not in (None, False)})


# --- write / remap -------------------------------------------------------------------------


@router.post("/trades/{group_id}/remap", response_model=TradeDTO)
async def remap_trade(request: Request, group_id: str, body: RemapRequest):
    try:
        return await _client(request).remap_trade(
            group_id, strategy=body.strategy, bot=body.bot, signal=body.signal,
            strategy_type=body.strategy_type, reviewed_by=body.reviewed_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/trades/{group_id}/regroup", response_model=list[TradeDTO])
async def regroup_trade(request: Request, group_id: str, body: RegroupRequest):  # noqa: ARG001 - group_id is contextual (URL nesting); regroup itself operates on txn_ids
    try:
        return await _client(request).regroup(body.txn_ids, target=body.target, reviewed_by=body.reviewed_by)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/trades/{group_id}/dismiss", response_model=TradeDTO)
async def dismiss_trade(request: Request, group_id: str, body: DismissRequest):
    try:
        return await _client(request).dismiss_trade(group_id, reviewed_by=body.reviewed_by)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# --- inbound ingest (reserved for future non-TT sources) -----------------------------------


@router.post("/ingest/{source_system}")
async def ingest(source_system: str):
    """Broker-neutral order/transaction ingest -- reserved for a second (non-TastyTrade) source.
    TastyTrade ingest goes through the pull/push adapters, not this endpoint; nothing implements
    this seam yet, so it's honestly a 501 rather than silently accepting and dropping data."""
    raise HTTPException(
        status_code=501,
        detail=f"POST /ingest/{source_system} is reserved for a future non-TastyTrade source and is not implemented yet.",
    )
