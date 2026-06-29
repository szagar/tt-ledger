# Pluggable store

The persistence layer is a typed seam — `LedgerStore` — that repositories depend on. A single
SQLAlchemy implementation covers every SQL dialect (SQLite default, Postgres opt-in); an in-memory
fake serves tests. A non-SQL backend is *possible* later (implement the Protocol) but is not built.

## The `LedgerStore` Protocol

```python
from typing import Protocol

class LedgerStore(Protocol):
    # idempotent writes — keyed on a broker id
    async def upsert_orders(self, rows: list[OrderRow]) -> None: ...        # conflict key: tt_order_id
    async def upsert_legs(self, rows: list[LegRow]) -> None: ...
    async def upsert_fills(self, rows: list[FillRow]) -> None: ...          # conflict key: fill_id
    async def upsert_transactions(self, rows: list[TxnRow]) -> None: ...    # conflict key: tt_transaction_id
    async def upsert_security(self, sec: SecurityRow) -> None: ...          # conflict key: security_id
    async def upsert_positions(self, rows: list[PositionRow]) -> None: ...  # conflict key: (account, security_id)

    # linking + grouping
    async def link_transactions_to_orders(self, account: str) -> int: ...   # by tt_order_id
    async def upsert_trade_group(self, tg: TradeGroupRow) -> None: ...
    async def add_trade_group_event(self, ev: EventRow) -> None: ...

    # reads (the consolidated views, as methods)
    async def get_trade_group(self, group_id: str) -> TradeGroupRow | None: ...
    async def query_orders(self, f: OrderFilter) -> list[OrderRow]: ...
    async def unified_trades(self, f: TradeFilter) -> list[TradeRow]: ...
    async def account_activity(self, f: ActivityFilter) -> list[ActivityRow]: ...
```

### Implementations

- **`SqlLedgerStore`** — async SQLAlchemy over any SQL dialect; the backend is chosen entirely by the
  connection URL. The one place that branches on dialect is the upsert helper:

  ```python
  if dialect == "postgresql":
      from sqlalchemy.dialects.postgresql import insert
  else:  # sqlite (and others that support ON CONFLICT)
      from sqlalchemy.dialects.sqlite import insert
  stmt = insert(table).values(rows)
  stmt = stmt.on_conflict_do_update(index_elements=[key], set_={...})
  ```

- **`InMemoryStore`** — pure-Python dicts implementing the same Protocol; used for fast unit tests of
  repositories, ingestion, and reconciliation without a database.

## `Money` type (locked decision)

A single `TypeDecorator` is applied to **every** monetary/price/fee/pnl column. Application code always
works in `Decimal`; the decorator delegates per dialect via `load_dialect_impl`:

- **Postgres** → native `NUMERIC(18, 6)`. `Decimal` passes straight through (asyncpg returns `Decimal`).
  Exact, human-readable, and supports native SQL `SUM` / decimal arithmetic in queries and views.
- **SQLite** → scaled **INTEGER** in micro-units (scale `1e6`). `process_bind_param` converts
  `Decimal → int` on write; `process_result_value` converts back on read. This is the only place the
  SQLite float-drift workaround lives.

```python
class Money(TypeDecorator):
    cache_ok = True
    def __init__(self, scale: int = 6) -> None:
        super().__init__()
        self.scale = scale
    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(NUMERIC(18, self.scale))
        return dialect.type_descriptor(Integer())               # sqlite: micro-units
    def process_bind_param(self, value, dialect):
        if value is None or dialect.name == "postgresql":
            return value
        return int((Decimal(value) * (10 ** self.scale)).to_integral_value())
    def process_result_value(self, value, dialect):
        if value is None or dialect.name == "postgresql":
            return value
        return Decimal(value) / (10 ** self.scale)
```

> **Why this matters:** SQLite has no `DECIMAL`; a plain `Numeric` column round-trips through float on
> SQLite and drifts at the cent — unacceptable for a financial ledger. On Postgres the decorator is a
> thin pass-through to native exact decimals, so the system-of-record keeps clean SQL ergonomics.
>
> Trade-off accepted: the on-disk representation differs between backends (PG `1.2345` vs SQLite
> `1234500`). This only matters if you byte-diff a dump across stores; it does not affect any value the
> application reads (always `Decimal`).

## Migrations

- Alembic, with **`render_as_batch=True`** so SQLite's limited `ALTER TABLE` is handled (batch mode
  rebuilds the table). The *same* revisions apply to Postgres.
- `alembic upgrade head` runs against whichever URL is configured — no per-dialect migration sets.
- Write each migration mindful of batch mode (avoid operations batch mode can't express).
