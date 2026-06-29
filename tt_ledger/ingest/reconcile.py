"""Reconcile broker activity into trade_groups (docs/ingestion.md → Reconcile).

Idempotent; NEVER touches a ``manually_attributed`` group. Steps: link transactions→order by
tt_order_id; group ungrouped by (account, executed_at); classify strategy_type; create the
trade_group (origin=broker, review_status=NEEDS_REVIEW, ENTRY event, realized P&L).
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..rows import SyncResult
    from ..store import LedgerStore


async def reconcile(
    store: "LedgerStore",
    account: str | None = None,
    *,
    since: date | None = None,
    dry_run: bool = False,
) -> "SyncResult":
    """Link → group → classify → create trade_groups. TODO: implement."""
    raise NotImplementedError


def detect_strategy_type(legs: list) -> str:  # noqa: ANN001
    """Classify legs into a StrategyType value (port from host platform). TODO."""
    raise NotImplementedError
