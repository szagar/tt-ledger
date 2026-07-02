# Implementation notes

For a developer building `tt-ledger` from these docs, with no prior context on the host platform.

## Project scaffold

`pyproject.toml` (already in the repo root): uv/hatchling, package `tt_ledger`.

- **Base deps:** `sqlalchemy`, `aiosqlite`, `alembic`, `pydantic`, `security-universe`.
- **Extras:** `[tastytrade]` = `httpx`,`websockets`; `[postgres]` = `asyncpg`; `[api]` =
  `fastapi`,`uvicorn`; `[redis]` = `redis`; `[cli]` = `typer`,`rich`; `[dev]` = test/lint.
- Base install needs no broker/infra deps — SQLite + schema + SDK work standalone.

Package layout (see `design.md` §B):

```
tt_ledger/
  config/        accounts.toml + securities.toml loaders
  identity/      AccountMapper, SecurityResolver
  money.py       Money TypeDecorator
  schema/        SQLAlchemy models + Alembic env (batch mode)
  store/         LedgerStore Protocol + SqlLedgerStore + InMemoryStore
  repositories/  Order / Transaction / TradeGroup / Position / Security
  ingest/        pull/  push/  reconcile/  remap/
  sdk.py         LedgerClient
  api/           FastAPI app  ([api] extra)
  cli.py         typer CLI  ([cli] extra)
tests/
```

## Build milestones

1. **Foundation** — `schema/` models + `money.py` + Alembic (batch mode); `SqlLedgerStore` +
   `InMemoryStore`. CI runs the suite on **both** SQLite and Postgres.
2. **Identity** — port `AccountMapper`; implement the `SecurityResolver` Protocol + `PassthroughResolver`
   (default) + the optional `SecurityUniverseResolver` / `CanonicalSymbolResolver` adapters; `securities`
   upsert from `ResolvedSecurity`; tests for both rules (incl. passthrough == vendor symbol).
3. **Pull** — broker `get_order_history` + `sync_orders` / `sync_transactions` / `sync_positions`;
   idempotency tests.
4. **Reconcile + remap** — link-by-`tt_order_id`, group, classify; remap primitives; iron-condor fixture.
5. **Surfaces** — `sdk.py` `LedgerClient`; then `[api]` FastAPI + `cli`.
6. **Push** — stream consumer (broker WS direct; Redis variant for host-platform deployment).

## Testing matrix

- **Every** repository/ingest test runs against `sqlite+aiosqlite` (`:memory:` and a file) **and**
  `postgresql+asyncpg`. Parametrize the store fixture over both URLs.
- **Money round-trip (mandatory):** push known fees/credits/strikes through both backends and assert
  zero drift — this guards the SQLite float landmine.
- **Idempotency:** re-run each importer → 0 new rows; a ZTS order keeps `origin=zts` + correlation
  after enrichment; a broker fill creates no row until the pull.
- **Reconcile fixture:** seed a manual iron condor (order-history + transactions) → one `origin=broker`
  trade_group, 4 legs linked to fills with correct VWAP, `strategy_type=iron_condor`,
  `review_status=needs_review`; re-run is idempotent; a `manually_attributed` group is untouched.
- **Resolver:** default (`PassthroughResolver`) → `security_id == vendor symbol`; an injected resolver
  changes only the `security_id` value, not the ledger's behavior.

## Gotchas

1. **Money on SQLite** — use the `Money` decorator on *every* monetary column; never a bare `Numeric`
   (it round-trips through float on SQLite).
2. **SQLite is single-writer** — fine for embedded/dev; concurrent multi-writer production = Postgres.
3. **Alembic batch mode** (`render_as_batch=True`) for SQLite's limited `ALTER TABLE`; write migrations
   with that in mind.
4. **Symbology is injected, not imposed** — core ships only `PassthroughResolver`; don't hard-wire a
   canonical scheme into the core. Custom schemes are resolver adapters.
5. **Dialect-specific upsert import** — `postgresql.insert` vs `sqlite.insert`; isolate to one helper
   in `SqlLedgerStore`.
6. **`security-universe` is sync** — the `SecurityUniverseResolver` resolves at ingest and stores the
   string; never call it inside hot async paths.

## Reference implementations (status)

These originally existed only as mature code in the host platform and needed porting; all of them
are now implemented directly in this repo (not ported — built fresh against the verified
TastyTrade docs, see `tt_ledger/ingest/tastytrade_client.py`/`tastytrade_stream.py`'s own
docstrings for what was confirmed vs. inferred):
`AccountMapper` + TOML loader (`identity/accounts.py`), the TastyTrade REST + WebSocket clients
(`ingest/tastytrade_client.py`, `ingest/tastytrade_stream.py`), and the trade-grouping /
strategy-detection / P&L logic (`ingest/reconcile.py`, `ingest/replay.py`). A structured canonical
symbology (`CanonicalSymbol` + OCC parsing) is also implemented (`identity/canonical.py`) as an
**optional** `CanonicalSymbolResolver` alternative to the default vendor passthrough. See
`integration-zts.md` for provenance.
