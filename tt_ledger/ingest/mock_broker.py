"""``MockTastyTradeClient`` — an in-memory fake ``BrokerClient`` (docs/ingestion.md → Pull).

No network, no ``[tastytrade]`` extra. Seed it with ``PlacedOrder``/``BrokerTransaction``/
``BrokerPosition`` fixtures (or the ``fill()`` convenience for the common single-order case) and
hand it to ``ingest/pull.py`` in place of the real REST client — same shape, zero infra, matching
the store's own "SQLite by default" zero-infra story. Internally paginates like the real
page-offset endpoints so pagination-reassembly logic can be exercised without a live API.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from ..rows import FillEvent
from .broker import BalanceMessage, BrokerPosition, BrokerTransaction, PlacedFill, PlacedLeg, PlacedOrder


class MockTastyTradeClient:
    def __init__(self, *, page_size: int = 250) -> None:
        self.page_size = page_size
        self._orders: dict[str, list[PlacedOrder]] = {}
        self._transactions: dict[str, list[BrokerTransaction]] = {}
        self._positions: dict[str, list[BrokerPosition]] = {}
        self.calls: list[tuple[str, str, date | None, date | None]] = []
        self.last_page_count = 0

    # --- seeding -------------------------------------------------------------------

    def add_order(self, order: PlacedOrder) -> None:
        self._orders.setdefault(order.account_number, []).append(order)

    def add_transaction(self, txn: BrokerTransaction) -> None:
        self._transactions.setdefault(txn.account_number, []).append(txn)

    def set_positions(self, account_number: str, positions: list[BrokerPosition]) -> None:
        self._positions[account_number] = list(positions)

    def fill(
        self,
        *,
        account_number: str,
        order_id: str,
        symbol: str,
        instrument_type: str,
        action: str,
        quantity: Decimal,
        fill_price: Decimal,
        filled_at: datetime,
        underlying_symbol: str | None = None,
        price: Decimal | None = None,
        price_effect: str | None = None,
        order_type: str = "Limit",
        status: str = "Filled",
    ) -> PlacedOrder:
        """Convenience: a single-leg, fully-filled order + its matching cash transaction — the
        common case for simple fixtures. Multi-leg / partial-fill scenarios: build ``PlacedOrder``
        directly and pass it to ``add_order``."""
        order = PlacedOrder(
            id=order_id, account_number=account_number, received_at=filled_at,
            underlying_symbol=underlying_symbol or symbol, order_type=order_type,
            price=price, price_effect=price_effect, status=status, terminal_at=filled_at,
            average_fill_price=fill_price, filled_quantity=quantity, remaining_quantity=Decimal("0"),
            legs=[
                PlacedLeg(
                    instrument_type=instrument_type, symbol=symbol, action=action,
                    quantity=quantity, remaining_quantity=Decimal("0"),
                    fills=[PlacedFill(fill_id=f"{order_id}-1", quantity=quantity, fill_price=fill_price, filled_at=filled_at)],
                )
            ],
        )
        self.add_order(order)
        self.add_transaction(
            BrokerTransaction(
                id=f"TXN-{order_id}", account_number=account_number, order_id=order_id,
                underlying_symbol=underlying_symbol or symbol, symbol=symbol, instrument_type=instrument_type,
                transaction_type="Trade", action=action, quantity=quantity, price=fill_price,
                executed_at=filled_at, transaction_date=filled_at.date(),
            )
        )
        return order

    # --- BrokerClient Protocol -------------------------------------------------------

    async def get_order_history(self, account_number: str, start: date, end: date) -> list[PlacedOrder]:
        self.calls.append(("get_order_history", account_number, start, end))
        matches = [o for o in self._orders.get(account_number, []) if start <= o.received_at.date() <= end]
        matches.sort(key=lambda o: o.received_at)
        return self._paginate(matches)

    async def get_transaction_history(self, account_number: str, start: date, end: date) -> list[BrokerTransaction]:
        self.calls.append(("get_transaction_history", account_number, start, end))
        matches = [
            t for t in self._transactions.get(account_number, [])
            if t.executed_at is not None and start <= t.executed_at.date() <= end
        ]
        matches.sort(key=lambda t: t.executed_at)
        return self._paginate(matches)

    async def get_positions(self, account_number: str) -> list[BrokerPosition]:
        self.calls.append(("get_positions", account_number, None, None))
        return list(self._positions.get(account_number, []))

    def _paginate(self, items: list) -> list:
        """Walk ``items`` in ``page_size`` chunks and reassemble — mirrors the real page-offset
        endpoints' shape without a network round trip per page. ``last_page_count`` lets tests
        assert pagination actually happened for a large seed set."""
        out = []
        page_count = 0
        for i in range(0, len(items), self.page_size):
            page_count += 1
            out.extend(items[i : i + self.page_size])
        self.last_page_count = max(page_count, 1)  # even an empty result is "one empty page"
        return out


class MockMessageSource:
    """An in-memory fake ``MessageSource`` (``ingest/push.py``) — no WebSocket/Redis. Queue
    messages with ``push()``; ``messages()`` yields and drains them, ending ``run()`` naturally
    once empty (a real transport would instead run until disconnected or told to ``stop()``)."""

    def __init__(self, queued: list[FillEvent | BrokerPosition | BalanceMessage] | None = None) -> None:
        self._queue: list[FillEvent | BrokerPosition | BalanceMessage] = list(queued or [])

    def push(self, msg: FillEvent | BrokerPosition | BalanceMessage) -> None:
        self._queue.append(msg)

    async def messages(self):
        while self._queue:
            yield self._queue.pop(0)
