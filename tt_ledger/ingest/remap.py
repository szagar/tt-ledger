"""Operator-driven attribution / remap (docs/ingestion.md → Remap).

Each writes an ADJUSTMENT event and flips ``manually_attributed`` / ``review_status`` so the
reconciler never overwrites the edit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..rows import TradeRow
    from ..store import LedgerStore


async def remap_trade_group(
    store: "LedgerStore",
    group_id: str,
    *,
    strategy: str | None = None,
    bot: str | None = None,
    signal: str | None = None,
    strategy_type: str | None = None,
    reviewed_by: str,
) -> "TradeRow":
    """Set attribution; cascade to orders/positions/transactions; CONFIRMED + ADJUSTMENT. TODO."""
    raise NotImplementedError


async def regroup_transactions(
    store: "LedgerStore",
    txn_ids: list[int],
    *,
    target_group_id: str | None,  # None -> new group
    reviewed_by: str,
) -> "list[TradeRow]":
    """Split/merge; recompute both groups' P&L; ADJUSTMENT on both. TODO."""
    raise NotImplementedError


async def dismiss_trade_group(store: "LedgerStore", group_id: str, *, reviewed_by: str) -> "TradeRow":
    """review_status=IGNORED (transfers / non-trades). TODO."""
    raise NotImplementedError
