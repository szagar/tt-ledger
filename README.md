# tt-ledger

A portable Python module that captures a broker's **order / transaction / fill / position** data
(pull + push), stores it in a **pluggable backend** (SQLite bundled by default; Postgres or other SQL
stores opt-in), and exposes a Python SDK + optional HTTP API for integration and viewing.

TastyTrade is the first (and only built) broker adapter; the schema and ingest contract are
broker-neutral (a `source_system` dimension) so a second source is additive.

> Status: **design / handoff documentation** (no implementation yet). Import package: `tt_ledger`.

## Why

- **One owner** of broker order/txn/fill/position persistence — no dual writers.
- **Zero-infra by default**: runs embedded on SQLite; the *same schema + repositories* run on Postgres
  for concurrent/production use — chosen purely by the connection URL.
- **Two identity rules**: account-number↔nickname, and an **injectable** broker-symbol→canonical
  `security_id` resolver (defaults to the vendor symbol) — broker-native identifiers confined to the edge.
- **Deterministic, idempotent ingest** (every row keyed on a broker id).
- **Reconciles** broker-placed trades into structured, reviewable, remappable `trade_group`s, unifying
  automated (`origin=zts`) and directly-placed (`origin=broker`) activity in one ledger.

## Quickstart (SQLite, zero infra)

```python
from datetime import date
from tt_ledger import LedgerClient
from tt_ledger.identity import AccountMapper

# accounts.toml uses placeholder logins/accounts — never commit real ones
client = LedgerClient.open(
    "sqlite+aiosqlite:///ledger.db",
    accounts=AccountMapper.from_toml("config/accounts.toml"),
)

await client.sync("main", since=date(2026, 1, 1))                 # pull + reconcile
trades = await client.trades(origin="broker", review_status="needs_review")
```

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
