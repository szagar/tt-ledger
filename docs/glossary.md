# Glossary

The vocabulary used throughout `tt-ledger`. Read this first.

| Term | Meaning |
|---|---|
| **login** | A broker credential set (one OAuth identity). Owns ≥1 account. Top-level key in `accounts.toml`. |
| **account_number** | The broker's raw account id. **Edge-only** — appears in broker calls + audit columns, never in internal logic. |
| **nickname** | The human, internal account name (e.g. `main`, `main_paper` — placeholders). **All internal code uses this.** Defined in config; a paper account's nickname must contain the substring `"paper"`. |
| **env** | Per-account trading mode: `live` or `paper`. |
| **security_id** | The canonical internal instrument identifier = the `CanonicalSymbol` string (e.g. `OS\|AAPL\|20250117\|150\|C`). **All internal code uses this.** |
| **broker-native symbol** | A broker/vendor symbol for a security: TastyTrade symbol, OCC option symbol, DXLink streamer symbol. **Edge-only**; stored on the `securities` dimension. |
| **order** | A single submitted broker order (one `tt_order_id`). Has 1..N legs. `origin` records who initiated it. |
| **order_leg** | One instrument line within an order (a `security_id` + action + quantity). An iron condor = 4 legs. |
| **fill** | A single execution against a leg (`fill_id`, price, quantity, time, venue). A leg can have many fills. |
| **transaction** | The broker's cash-truth record of an executed event (`tt_transaction_id`): the authoritative source for fees, net value, and assignment/expiration. Carries the broker's `order-id`. |
| **position** | A currently-open holding (account + `security_id`), with mark + unrealized P&L. |
| **closed_position** | A holding that has been fully closed, with realized P&L and open/close prices. |
| **trade_group** | **A strategy-level grouping of the orders / legs / transactions that make up one trade** — all legs, plus any rolls / adjustments / exits over the trade's life. The unit at which realized P&L, max-profit/loss, and strategy attribution are tracked. This is the human-meaningful *"a trade."* Example: the four legs of an iron condor opened together, the partial exit that closes the call side, and the expiration of the put side are all one `trade_group`. |
| **trade_group_event** | A lifecycle event on a trade_group: `ENTRY` / `PARTIAL_EXIT` / `FULL_EXIT` / `ROLL` / `ADJUSTMENT` / `EXPIRATION` / `ASSIGNMENT` / `EXERCISE`. |
| **origin** | `zts` (initiated by the automated host system) or `broker` (placed directly at the broker, or any non-automated source). The single axis that distinguishes "ours" from "foreign." |
| **ingest** | How a row entered the ledger: `oms_submit` (recorded at submission), `order_history` (REST pull), or `import` (backfill). |
| **source_system** | Reserved dimension for a multi-broker future (`tastytrade` today). |
| **universe** | (from the `security-universes` library) a named, typed set of securities — watchlist / restricted / index membership / bot input — keyed on `security_id`. |
| **review_status** | A trade_group's reconciliation state: `NEEDS_REVIEW` / `CONFIRMED` / `IGNORED`. |
| **manually_attributed** | Guard flag. Once an operator remaps/attributes a group by hand, the reconciler never overwrites it. |

## Relationships at a glance

```
login ──< account (nickname) ──< order ──< order_leg ──< fill
                                   │           │
                                   │           └──< transaction (cash truth)
                                   ▼
                              trade_group ──< trade_group_event
                                   │
                  position / closed_position
security_id (the CanonicalSymbol string) identifies the instrument on every leg / txn / position.
```
