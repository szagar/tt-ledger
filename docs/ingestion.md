# Ingestion — pull, push, reconcile, replay

Two capture paths feed the same store; both are **idempotent** (every row keyed on a broker id, written
via the store's upserts). A reconciliation pass then structures broker-placed activity into trades.

## Pull (REST)

The authoritative source of order/leg/fill structure. Requires one broker method that the platform's
existing client does **not** ship and must be added:

```python
async def get_order_history(account, start, end) -> list[PlacedOrder]:
    """GET /accounts/{a}/orders — page-offset pagination.
    Each PlacedOrder carries legs[], and each leg carries fills[] (fill_id, price, qty, filled_at,
    venue, ext ids)."""
```

Importers:

- **`sync_orders`** → upsert `orders` on `tt_order_id`, `order_legs`, and `order_fills` on `fill_id`.
  **One importer serves both origins:**
  - an existing `origin=zts` row is *enriched* — only fill/status fields are written
    (`average_fill_price`, `tt_status`, `filled_quantity`, `fill_price`, `filled_at`); `origin`,
    `signal_id`, `trace_id`, `strategy_id` are never touched.
  - a `tt_order_id` with no row is *created* `origin=broker, ingest=order_history`.
- **`sync_transactions`** → upsert `transactions` on `tt_transaction_id`, capturing the broker's
  `order-id` into the `tt_order_id` column (the deterministic txn→order link key).
- **`sync_positions`** → upsert `positions` on `(account, security_id)`.

All importers resolve broker symbols → `security_id` via the `SecurityResolver` and accept nicknames
only (Rule 1/Rule 2 enforced at this boundary).

## Push (stream)

A consumer of the broker's account-stream (order / position / balance messages).

- **Standalone deployment:** connect the broker WebSocket directly
  (`TastyTradeMessageSource`, `[tastytrade]` extra).
- **Host-platform deployment:** consume the platform's existing `acct:*` Redis pub/sub via
  `RedisMessageSource` (`ingest/redis_source.py`, `[redis]` extra; see `integration-zts.md`).
  It subscribes `acct:order` / `acct:position` / `acct:balance`, translates the host's
  nickname-keyed snake_case envelopes into the same `FillEvent` / `BrokerPosition` /
  `BalanceMessage` shapes (account numbers restored via the injected `AccountMapper`),
  optionally filters to one login's nicknames, and reconnects with capped exponential backoff.

The stream provides **real-time visibility** and live order-status updates. It is **not** the source of
order structure: a broker fill whose `tt_order_id` has no local order does **not** create a row from the
stream — `sync_orders` creates the authoritative row (with legs + fills) on its next pass, keyed on
`tt_order_id`. This avoids thin, structureless "adopted" rows.

## Reconcile

Turns ungrouped broker activity into reviewable trades.

1. **Link** transactions → order deterministically: `transactions.tt_order_id = orders.tt_order_id`
   sets `order_id` (then leg linkage by `security_id`). No fuzzy/heuristic matching.
2. **Synthesize lapsed settlements** (`synthesize_lapsed_settlements`): a lot past expiry that an
   OPEN group still holds, whose broker settlement row never arrived (futures options that just
   vanish), gets the missing `Receive Deliver / Expiration` transaction recreated — ONE ROW PER
   (open group, security), deterministic id `lapse-<account>-<group_pk>-<security_id>`, price 0
   at expiry 21:15Z, quantity = that group's own net capped by the account-level net. Rows are
   pre-attributed and applied straight to their group via the exit machinery, NOT routed through
   clustering: the same contract is routinely held open by several groups, and first-match
   routing would hand a single whole-quantity settlement to one group and leave the rest stuck.
   Open lots come from `net_open_quantities` over the account's full transaction history
   (replay's lot rules, except price-less action rows still count — matching group accounting's
   view — and never the positions table, which replay's lapse backstop has already flattened);
   the clock is the account's own latest transaction, never wall-clock. A real (or prior
   synthetic) settlement nets the lot to zero, so re-runs and late-arriving broker truth no-op;
   only groups whose net direction matches the account net participate (mismatches are regroup
   material), and a not-yet-grouped entry synthesizes on the next pass, once its group exists.
3. **Group** ungrouped transactions by `(account, executed_at)` (tolerance window joins multi-order
   strategies executed together).
4. **Route** each cluster against the account's OPEN groups:
   - **closing** rows (`* to Close` trades, `Receive Deliver` expiration/assignment/exercise —
     the latter carry no order-id and are admitted on sub-type) that offset an open group's legs
     **attach to that group** with the matching lifecycle event (`partial_exit` / `full_exit` /
     `expiration` / `assignment` / `exercise`). A fully-offset group's `status` flips
     (closed/expired/assigned/exercised; `mixed` when causes differ), `closed_at` is stamped, and
     cash-basis `realized_pnl` (signed net across all member transactions) is written.
   - **rolls**: closes + opens in one cluster on the same underlying, or a close-cluster and an
     open-cluster within 60s (same underlying/option type/quantity), add a `roll` event with
     `rolled_to_group_id` on the old group.
5. **Classify** `strategy_type` from the remaining (opening) legs.
6. **Create** the `trade_group` with `origin=broker`, `review_status=NEEDS_REVIEW`, an `ENTRY` event,
   premium / max-profit-loss; set `orders.trade_group_id` + `transactions.trade_group_id`.
7. **Heal fully-closed groups** (`heal_fully_closed_groups`): an OPEN group whose member
   transactions already net to zero gets its overdue status flip — status from the close causes
   (`mixed` when they differ), `closed_at` = last activity, cash-basis `realized_pnl` — plus an
   `adjustment` event recording the repair. Targets legacy data where closes attached on a path
   that skipped the exit machinery's flip; a healed group leaves the open set, so re-runs no-op.

**Idempotent**, and it **never re-attributes a `manually_attributed` group's membership** — so
re-running after every sync is safe and never clobbers an operator's edits. (Closing activity may
attach to a manually-attributed group: that records the group's own lifecycle.)

### Remap (operator-driven)

- `remap_trade_group(group_id, *, strategy=None, bot=None, signal=None, strategy_type=None, reviewed_by)`
  — set attribution, cascade to the group's orders/positions/transactions, flip
  `manually_attributed=true` + `review_status=CONFIRMED`, write an `ADJUSTMENT` event.
- `regroup_transactions(txn_ids, target_group_id | new)` — split/merge when grouping was wrong;
  recompute both groups' P&L; `ADJUSTMENT` events on both.
- `dismiss_trade_group(group_id)` — `review_status=IGNORED` (transfers / non-trades) so it leaves the
  review queue without attribution.

### Edge cases

- Multi-leg strategy spanning several `tt_order_id`s executed together → joined by the grouping window.
- `Receive Deliver` (assignment / expiration / exercise) → closing events on an existing group, not a
  new group.
- A trade also placed via the automated system → enriched in place by `tt_order_id`, never
  double-created as `broker`.
- Stock + option legs mixed → `strategy_type = custom`; still grouped.

## Replay (position history)

`sync_positions` only ever gives the broker's **current** snapshot — there's no historical
equivalent of `/positions`. `transactions` is the only endpoint with a real event log, so
**`rebuild_positions_from_transactions`** (`ingest/replay.py`) reconstructs quantity, cost basis,
and every completed open→close lifecycle by replaying it forward.

- **Full rebuild, not incremental** — walks every position-affecting transaction for an account
  from scratch, in `executed_at` order, maintaining a running weighted-average-cost lot per
  security. Idempotent: `positions` upserts on `(account, security_id)`, `closed_positions`
  "upserts" (app-level key, no DB unique constraint) on `(account, security_id, opened_at,
  closed_at)`, and `transactions.position_id` / `closed_position_id` re-link to the same ids
  every run.
- `positions` stays the CURRENT (or most-recently-reopened) lot; `closed_positions` is the durable
  record of each completed round trip (`average_open_price`/`average_close_price`/`realized_pnl`/
  `holding_period_days`) that `positions` itself can't hold once a lot fully closes and reopens.
- Replay owns `quantity` / `quantity_direction` / `average_open_price` / `opening_order_id` /
  `position_opened_at` on the `positions` row; it never touches `mark_price` / `close_price` /
  `unrealized_pnl` / `realized_day_gain` (broker-owned, from `sync_positions`) or `strategy_id` /
  `trade_group_id` (operator-owned, from remap).
- A direction-flip transaction (e.g. long 10, sell 15 → short 5) sets **both** `position_id`
  (opens the new lot) and `closed_position_id` (closes the old one) on that one row.
- **Known limitations:** gross P&L only (fees/commissions aren't netted out, matching
  `unrealized_pnl`'s existing convention); a position whose opening predates the visible
  transaction history (e.g. an ACAT transfer, or a sync window starting after inception) has its
  cost basis/opening date understated to the visible window; a transaction with a `security_id` +
  `quantity` but no `action` (some corporate actions) is applied as a signed delta using the
  transaction's own quantity sign, not an inferred Buy/Sell direction.
