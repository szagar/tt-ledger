# tt-ledger

A portable Python module that captures a broker's **order / transaction / fill / position** data
(pull + push), stores it in a **pluggable backend** (SQLite bundled by default; Postgres or other SQL
stores opt-in), and exposes a Python SDK + optional HTTP API for integration and viewing.

TastyTrade is the first (and only built) broker adapter; the schema and ingest contract are
broker-neutral (a `source_system` dimension) so a second source is additive.

> Status: **implemented** — schema/store, identity, real TastyTrade pull (REST) + push
> (WebSocket) adapters, reconcile/remap, position history rebuilt from transactions, and all
> three surfaces (SDK, FastAPI, CLI). Import package: `tt_ledger`.

## Why

- **One owner** of broker order/txn/fill/position persistence — no dual writers.
- **Zero-infra by default**: runs embedded on SQLite; the *same schema + repositories* run on Postgres
  for concurrent/production use — chosen purely by the connection URL.
- **Two identity rules**: account-number↔nickname, and an **injectable** broker-symbol→canonical
  `security_id` resolver (defaults to the vendor symbol) — broker-native identifiers confined to the edge.
- **Deterministic, idempotent ingest** (every row keyed on a broker id).
- **Reconciles** broker-placed trades into structured, reviewable, remappable `trade_group`s, unifying
  automated (`origin=zts`) and directly-placed (`origin=broker`) activity in one ledger.

## Quickstart

### CLI (`pip install tt-ledger[tastytrade,cli]`)

```sh
cp config/accounts.toml.example config/accounts.toml   # fill in real OAuth creds -- never commit it
tt-ledger sync --account main --since 2026-01-01        # pull (orders+transactions+positions) + reconcile
tt-ledger rebuild-positions --account main               # position/closed-position history from transactions
tt-ledger trades list --needs-review
tt-ledger positions --account main
```

SQLite is the default store (`./ledger.db`); point elsewhere with `--url` or `TT_LEDGER_DATABASE_URL`.

### SDK

```python
from datetime import date
from tt_ledger import LedgerClient
from tt_ledger.identity import AccountMapper, LoginConfig
from tt_ledger.ingest import TastyTradeClient

# accounts.toml uses placeholder logins/accounts — never commit real ones
accounts = AccountMapper.from_toml("config/accounts.toml")
broker = TastyTradeClient.from_login_config(LoginConfig.from_toml("trader1", "config/accounts.toml"))

client = LedgerClient.open("sqlite+aiosqlite:///ledger.db", accounts=accounts, client=broker)
try:
    await client.sync("main", since=date(2026, 1, 1))              # pull + reconcile
    trades = await client.trades(origin="broker", review_status="needs_review")
finally:
    await client.close()                                            # also closes the broker's httpx session
```

Read-only work (`trades`, `orders`, `positions`, remap, …) needs no broker client at all —
`LedgerClient.open(url, accounts=accounts)` is enough once data has been synced.

Switch to Postgres by changing one URL: `postgresql+asyncpg://…`. No code change.

## Documentation

| Doc | Contents |
|---|---|
| [docs/design.md](docs/design.md) | Context, goals, architecture, deployment profiles |
| [docs/glossary.md](docs/glossary.md) | Domain terminology (account, security_id, trade_group, …) |
| [docs/schema.md](docs/schema.md) | Complete schema — tables, types, enums, Money type, views |
| [docs/identity.md](docs/identity.md) | The two identity rules: accounts (nicknames) + securities (injectable resolver) |
| [docs/symbology.md](docs/symbology.md) | Injectable security resolver — vendor-symbol default; security-universe option |
| [docs/storage.md](docs/storage.md) | Pluggable store: `LedgerStore` Protocol, SQL impl, dialects, migrations |
| [docs/ingestion.md](docs/ingestion.md) | Pull (REST) + push (stream) + idempotency + reconciliation |
| [docs/api.md](docs/api.md) | In-process SDK + FastAPI view/ingest + CLI |
| [docs/implementation-notes.md](docs/implementation-notes.md) | Scaffold, build milestones, testing matrix, gotchas |
| [docs/integration-zts.md](docs/integration-zts.md) | Appendix: adopting as a host platform's canonical store |

## Deployment profiles

- **Embedded-SQLite** — single process, WAL; dev / CI / notebooks / standalone single-operator. Zero
  infra. *Not* for concurrent multi-writer use.
- **Networked-Postgres** — concurrent writers, production.

## Non-goals

Point-in-time symbology / corporate-action history (ticker renames, contract rolls); multi-broker
adapters beyond TastyTrade; being a market-data store.
