# Ingestion — pull, push, reconcile

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

- **Standalone deployment:** connect the broker WebSocket directly.
- **Host-platform deployment:** consume the platform's existing `acct:*` Redis pub/sub (see
  `integration-zts.md`).

The stream provides **real-time visibility** and live order-status updates. It is **not** the source of
order structure: a broker fill whose `tt_order_id` has no local order does **not** create a row from the
stream — `sync_orders` creates the authoritative row (with legs + fills) on its next pass, keyed on
`tt_order_id`. This avoids thin, structureless "adopted" rows.

## Reconcile

Turns ungrouped broker activity into reviewable trades.

1. **Link** transactions → order deterministically: `transactions.tt_order_id = orders.tt_order_id`
   sets `order_id` (then leg linkage by `security_id`). No fuzzy/heuristic matching.
2. **Group** ungrouped transactions by `(account, executed_at)` (tolerance window joins multi-order
   strategies executed together).
3. **Classify** `strategy_type` from the legs.
4. **Create** the `trade_group` with `origin=broker`, `review_status=NEEDS_REVIEW`, an `ENTRY` event,
   premium / max-profit-loss, and realized P&L; set `orders.trade_group_id` + `transactions.trade_group_id`.

**Idempotent**, and it **never touches a `manually_attributed` group** — so re-running after every sync
is safe and never clobbers an operator's edits.

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
