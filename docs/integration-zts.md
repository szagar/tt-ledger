# Appendix — adopting tt-ledger as a host platform's canonical store

This appendix is only relevant when integrating `tt-ledger` into an existing automated trading platform
(the project it was extracted from). A standalone build can ignore it.

## Goal

Make `tt-ledger` the **single canonical store** for the platform's order / leg / fill / transaction /
position / closed_position / trade_group / trade_group_event data — replacing the equivalent tables in
the platform's current schema package. The platform runs `tt-ledger` on its Postgres backend; SQLite
remains available for dev/standalone.

## Provenance of the ported primitives

The reference implementations the standalone build ports (see `implementation-notes.md`) come from the
host platform:
- `AccountMapper` + `accounts.toml` loader — the nickname↔account-number subsystem (Rule 1).
- `CanonicalSymbol` + OCC parsing + the symbol registry — wrapped in a `CanonicalSymbolResolver` and
  **injected** as the host's `security_id` resolver (Rule 2); tt-ledger's default is vendor passthrough.
- The TastyTrade REST/stream client (incl. the new `get_order_history`).
- Trade-grouping, strategy detection, and trade-group P&L logic — the reconciliation core.

## Logging

tt-ledger logs through the standard-library ``tt_ledger.*`` logger hierarchy and never
configures handlers — the host's logging setup (e.g. a structured-JSON root handler) applies
as-is. Sync step failures and stream lifecycle/reconnect events log at WARNING/INFO;
``SyncResult.errors`` still carries the same strings programmatically.

## Database coexistence

The ledger shares the host's Postgres database: all ledger tables live in the dedicated **`ledger`**
schema (see `docs/storage.md` — Postgres schema namespace), and the Alembic version table is
`ledger.tt_ledger_alembic_version`, so the host's own Alembic chain (default `alembic_version`, its
tables in `public`) is never touched. The host applies ledger migrations via `tt-ledger db upgrade`
or `tt_ledger.schema.migrate.upgrade_to_head(url)` from its own migrate flow.

## Migration steps (stageable)

1. **Stand up** `tt-ledger` on the platform Postgres alongside the existing schema (mirror mode).
2. **Backfill** once from the existing tables → ledger: map the old `source` enums to `origin` +
   `ingest`, populate the `securities` dimension + `security_id`, and capture the broker `order-id` onto
   transactions (`tt_order_id`).
3. **Repoint writers/readers** to `LedgerClient`:
   - the order-management service → record/submit + fill updates via the SDK;
   - the account-stream service → **publish-only** (drop its DB-write path, the synthetic external-order
     adoption, and the synthetic id minting);
   - delete the fuzzy description/timestamp transaction matcher — linkage is deterministic on
     `tt_order_id`;
   - strategy engine, dashboards, and reporting read through the SDK / consolidated views.
4. **Drop** the migrated tables + dead enums from the old schema package (non-ledger tables —
   strategies, market context, bot runs, snapshots, earnings, webhook archive, bot state — stay where
   they are; the ledger FKs to strategy/market-context by id).
5. **Run** `tt-ledger` as a service (push consumer + scheduled REST sync) under the platform's process
   supervisor.

## Why this is a large, intentional change

It collapses three overlapping write paths (stream-side DB writes, fill-event DB writes, and REST sync
scripts) into one owner, removes the Postgres-coupling of the old schema, and unifies ZTS-originated and
broker-originated activity behind `origin`. It is repo-wide churn by design — stage it: mirror first,
cut writers over, then drop the old tables.

## Host-platform stream variant

In the platform, the push consumer reads the existing `acct:order` / `acct:position` / `acct:balance`
Redis pub/sub (published by the account-stream service) rather than connecting the broker WebSocket
directly — `RedisMessageSource` (`ingest/redis_source.py`, `[redis]` extra):

```python
source = RedisMessageSource(redis_url, accounts=mapper, nicknames={"individual", "roth"})
await client.stream_consumer(source).run()
```

The host publishes nicknames; the source restores broker account numbers via the injected
`AccountMapper` so the `MessageSource` contract (broker-native shapes) holds. Everything else
(idempotent pull, reconcile, SDK, views) is identical to the standalone deployment.
