"""Repositories — domain operations over the LedgerStore (docs/storage.md, docs/schema.md).

Each repository takes a ``LedgerStore`` and exposes intent-level methods; it owns the
invariants (idempotent upsert keys, the consolidated-view query shapes). Stubs below.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..identity import ResolvedSecurity
    from ..rows import OrderFilter, OrderRow, TradeFilter, TradeGroupRow, TradeRow
    from ..store import LedgerStore


class _Repo:
    def __init__(self, store: "LedgerStore") -> None:
        self._store = store


class SecurityRepository(_Repo):
    async def upsert(self, resolved: "ResolvedSecurity", *, tt_symbol: str | None = None) -> None:
        """Upsert the securities dimension row from a resolver result (+ the vendor symbol). TODO."""
        raise NotImplementedError


class OrderRepository(_Repo):
    async def upsert_from_history(self, placed_orders: list) -> int:  # noqa: ANN001
        """sync_orders core: upsert orders+legs+fills; enrich ZTS rows, create broker rows. TODO."""
        raise NotImplementedError

    async def query(self, f: "OrderFilter") -> "list[OrderRow]":
        raise NotImplementedError


class TransactionRepository(_Repo):
    async def upsert(self, txns: list) -> int:  # noqa: ANN001
        """sync_transactions core: upsert on tt_transaction_id, capture broker order-id. TODO."""
        raise NotImplementedError

    async def link_to_orders(self, account: str) -> int:
        raise NotImplementedError


class PositionRepository(_Repo):
    async def upsert(self, positions: list) -> int:  # noqa: ANN001
        raise NotImplementedError


class TradeGroupRepository(_Repo):
    async def get(self, group_id: str) -> "TradeGroupRow | None":
        raise NotImplementedError

    async def unified(self, f: "TradeFilter") -> "list[TradeRow]":
        raise NotImplementedError


__all__ = [
    "SecurityRepository", "OrderRepository", "TransactionRepository",
    "PositionRepository", "TradeGroupRepository",
]
