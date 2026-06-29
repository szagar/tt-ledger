# Design — context, goals, architecture

## A. Context & goals

`tt-ledger` (import package `tt_ledger`) is a portable Python module that **captures a broker's order /
transaction / fill / position data (pull + push), stores it in a pluggable backend (SQLite bundled by
default, Postgres or other SQL stores opt-in), and exposes a Python SDK + optional HTTP API** for
integration and viewing.

TastyTrade is the first (and only built) broker adapter; the schema and ingest contract are
broker-neutral (a `source_system` dimension) so a second source is additive.

It unifies, in one consolidated ledger, activity that originated in an automated system vs. activity
placed directly at the broker (`origin = zts | broker`), and tracks trades at the **trade_group**
level — the human-meaningful "a trade" (all legs + rolls + adjustments + exits of, say, one iron
condor) — with realized P&L and strategy attribution.

### Goals

1. **One owner** of broker order/txn/fill/position persistence — no dual writers.
2. Run **embedded on SQLite with zero infra**; run on **Postgres** for concurrent/production use —
   *same schema, same repositories, two deployment profiles*, chosen by the connection URL.
3. **Two identity subsystems**: account-number↔nickname (config-driven), and an **injectable**
   broker-symbol→canonical `security_id` resolver (default: the vendor symbol) — broker-native ids
   confined to the edge.
4. **Deterministic, idempotent ingest** (every row keyed on a broker id).
5. **Reconcile** broker-placed trades into structured, reviewable, remappable `trade_group`s.

### Non-goals

Imposing a canonical symbology (it's an injected resolver; default = vendor symbol); **named sets /
universes / watchlists** (a selection/intent concern that lives upstream of a post-trade ledger);
point-in-time symbology / corporate-action history (ticker renames, contract rolls); multi-broker
adapters beyond TastyTrade; being a market-data store.

## B. Architecture

Layered; dependencies point downward. Each layer is a directory under `tt_ledger/`.

```
api/ (FastAPI, [api] extra)        cli/ (typer, [cli] extra)
        \                           /
            sdk.py   LedgerClient — the in-process Python API
                          |
     ingest/   pull · push · reconcile · remap
                          |
       repositories/   Order · Transaction · TradeGroup · Position · Security
                          |
        store/   LedgerStore Protocol  ──►  SqlLedgerStore (sqlite+pg)  |  InMemoryStore (tests)
                          |
   identity/ (AccountMapper, SecurityResolver)   schema/ (SQLAlchemy models + Alembic)   money.py
                          |
            config/ (accounts.toml, securities.toml loaders)
```

- **config / identity** — the two identity rules (see `identity.md`).
- **schema / store / repositories** — the dialect-agnostic tables and the pluggable persistence seam
  (see `schema.md`, `storage.md`).
- **ingest** — pull (REST) + push (stream) capture, reconciliation, remap (see `ingestion.md`).
- **sdk / api / cli** — integration + view surfaces (see `api.md`).

## Deployment profiles

*Same schema + same repositories; the only difference is the connection URL.*

| Profile | URL | Use | Concurrency |
|---|---|---|---|
| **Embedded-SQLite** (bundled default) | `sqlite+aiosqlite:///ledger.db` | dev / CI / notebooks / standalone single-operator | single writer (WAL) |
| **Networked-Postgres** | `postgresql+asyncpg://…` | production, multiple writers | concurrent |

SQLite is **not** the concurrent production backend — it is the zero-infra default for single-process
and embedded use. Prod with multiple writers uses Postgres. No code changes between profiles.
