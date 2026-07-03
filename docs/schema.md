# Complete schema

Dialect-agnostic (SQLite + Postgres). Conventions:

- All timestamps are `DateTime(timezone=True)` (UTC).
- All monetary / price / fee / pnl columns use the **`Money`** type (see [storage.md](storage.md) →
  native `NUMERIC(18,6)` on Postgres, scaled-integer micro-units on SQLite; the app always works in
  `Decimal`).
- `JSON` is the generic SQLAlchemy type (maps to `JSONB` on Postgres, `TEXT`/`JSON` on SQLite) — never
  PG-only `JSONB` in the model definitions.
- Integer surrogate PK **plus** a natural unique key per table for idempotent upsert.
- Partial unique indexes are used (supported by both SQLite ≥3.8 and Postgres).

## Dimensions

### accounts
Synced from `accounts.toml`; the config is the source of truth, this table mirrors it for FK integrity
+ metadata.

`id` PK · `nickname` UNIQUE · `account_number` UNIQUE · `login` · `env` (live|paper) ·
`source_system` (default `tastytrade`) · `is_active` · `created_at` · `updated_at`.

### securities
The security-master; populated at ingest from broker symbols via the `SecurityResolver`. The durable
form of a canonical↔broker symbol registry.

`id` PK · `security_id` UNIQUE (= `CanonicalSymbol` string) · `product_type` (S/I/F/OS/OI/OF/CR) ·
`underlying` · `expiry` · `strike` (Money) · `option_type` (P/C) · `multiplier` · `exchange` ·
`currency` · `tt_symbol` · `occ_symbol` · `streamer_symbol` · `source_system` · `metadata` (JSON) ·
`first_seen_at` · `last_updated_at`.
Indexes: `underlying`, `tt_symbol`, `streamer_symbol`.

## Facts

### orders
`id` PK · `tt_order_id` UNIQUE (partial, WHERE NOT NULL) · `oms_order_id` (nullable — host-internal id;
no synthetic placeholder for broker orders) · `client_order_id` · `account` (FK accounts.nickname) ·
`account_number` (audit) · `origin` (zts|broker) · `ingest` (oms_submit|order_history|import) ·
`source_system` · `security_id`/`underlying` (FK securities) · `order_type` · `time_in_force` ·
`gtc_date` · `price` / `stop_trigger` / `price_effect` (Money) · `average_fill_price` (Money) ·
`is_complex` · `complex_order_type` · `oms_status` · `tt_status` (raw broker status) · `status_message`
· `filled_quantity` / `remaining_quantity` (Money) · `signal_id` / `trace_id` (correlation; NULL for
broker orders) · `strategy_id` / `trade_group_id` / `market_context_id` (FK) · `received_at` /
`submitted_at` / `filled_at` / `terminal_at` / `created_at` / `updated_at`.

### order_legs
`id` PK · `order_id` FK · `leg_index` · `security_id` FK · `action` ·
`quantity` / `remaining_quantity` (Money) · `quantity_direction` · `price` / `fill_price` (Money) ·
`created_at` / `updated_at`.

### order_fills
Per-leg executions (from the order-history `fills[]`).

`id` PK · `order_id` FK · `order_leg_id` FK · `tt_order_id` · `fill_id` UNIQUE ·
`quantity` / `fill_price` (Money) · `filled_at` · `destination_venue` · `ext_exec_id` ·
`ext_group_fill_id`.

### transactions
Cash-truth; the authoritative source for fees and net value.

`id` PK · `tt_transaction_id` UNIQUE · `tt_order_id` (deterministic txn→order link key) · `account` ·
`account_number` (audit) · `source_system` · `transaction_type` / `transaction_sub_type` · `action` ·
`security_id` / `underlying` (FK) · `quantity` / `price` (Money) · `value` / `value_effect` ·
`net_value` / `net_value_effect` (Money) · `commission` / `clearing_fees` / `regulatory_fees` /
`proprietary_index_option_fees` (Money) · `is_estimated_fee` · `description` · `executed_at` /
`transaction_date` · FKs `order_id` / `order_leg_id` / `position_id` / `closed_position_id` /
`trade_group_id` · `created_at` / `updated_at`.
Composite indexes: `(account, executed_at)`, `(account, transaction_date)`.

### positions
`id` PK · `account` · `security_id` FK · **UNIQUE `(account, security_id)`** · `quantity` /
`quantity_direction` · `average_open_price` / `mark_price` / `close_price` (Money) · `unrealized_pnl` /
`realized_day_gain` (Money) · `multiplier` · `expires_at` · `strategy_id` / `opening_order_id` /
`trade_group_id` (FK; NULL attribution for broker positions) · `position_opened_at` / `first_seen_at` /
`last_updated_at`.

### closed_positions
`id` PK · `account` · `security_id` FK · `quantity` / `quantity_direction` · `average_open_price` /
`average_close_price` / `realized_pnl` (gross) / `fees` / `pnl_net` (= realized_pnl − fees; use for
R-multiples) (Money) · `opening_order_id` / `closing_order_id` / `trade_group_id` (FK) ·
`opened_at` / `closed_at` · `holding_period_days`.

### trade_groups
`id` PK · `group_id` UNIQUE · `account` · `origin` (zts|broker) · `source_system` · `review_status`
(needs_review|confirmed|ignored) · `manually_attributed` · `reviewed_at` / `reviewed_by` ·
`underlying` / `security_id` · `strategy_type` · `leg_count` · `total_premium` / `quantity` /
`total_fees` (Money) · `status` (open|closed|expired|assigned|exercised|mixed) · `realized_pnl` /
`unrealized_pnl` / `max_profit` / `max_loss` (Money) · `profit_target` / `stop_loss` / `exit_strategy`
· `structure` (JSON — opaque host-written submit-time structure descriptor: legs/strikes/expiry/…;
never derived from or mutated by the ledger) · `order_id` / `strategy_id` (FK) · `bot_name` /
`signal_id` · `executed_at` / `closed_at` / `created_at` / `updated_at`.

### trade_group_events
`id` PK · `trade_group_id` FK · `event_type` · `quantity_change` / `premium_change` / `realized_pnl`
(Money) · `event_at` · `notes` · `rolled_to_group_id` FK · `transaction_id` / `order_id` FK.

### balance_snapshots
`id` PK · `account` FK · `captured_at` · `source` (stream|rest_sync) · `net_liquidating_value` /
`cash_balance` / `equity_buying_power` / `derivative_buying_power` / `maintenance_requirement` /
`pending_cash` / `day_trading_buying_power` (Money) · `raw` JSON · `created_at`.
UNIQUE `(account, captured_at, source)`. Append-only time series (NLV history for sizing /
equity-curve analysis); the latest row per account is the live view. Written throttled by
`StreamConsumer` (one row per account per interval, NLV changes always persist) and once per REST
`sync()`.

## Enums

| Enum | Values |
|---|---|
| `Origin` | `zts`, `broker` |
| `Ingest` | `oms_submit`, `order_history`, `import` |
| `ReviewStatus` | `needs_review`, `confirmed`, `ignored` |
| `OrderStatus` | `pending`, `submitted`, `working`, `filled`, `partially_filled`, `cancelled`, `rejected`, `expired` |
| `TradeGroupStatus` | `open`, `closed`, `expired`, `assigned`, `exercised`, `mixed` |
| `StrategyType` | `single`, `vertical`, `calendar`, `diagonal`, `iron_condor`, `iron_butterfly`, `straddle`, `strangle`, `butterfly`, `condor`, `ratio`, `covered`, `collar`, `custom`, `future`, `future_spread` |
| `TradeGroupEventType` | `entry`, `partial_exit`, `full_exit`, `roll`, `adjustment`, `expiration`, `assignment`, `exercise` |
| `ProductType` | `S`, `I`, `F`, `OS`, `OI`, `OF`, `CR` |

## Consolidated read model

`v_orders_unified`, `v_trades_unified`, `v_account_activity` — implemented as **repository
query-methods** (not SQL `CREATE VIEW`) so they behave identically on SQLite and Postgres and are
unit-testable against the in-memory fake store. Each exposes `origin` so a ZTS/broker toggle is a
single filter.

`v_account_activity WHERE order_id IS NULL` = the unreconciled-coverage metric (broker transactions
not yet tied to an order).
