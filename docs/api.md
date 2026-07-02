# APIs — SDK, HTTP, CLI

Three surfaces over the same repositories. The in-process SDK is the canonical entry point; the HTTP
server and CLI are thin wrappers for external consumers and operators.

All surfaces accept **nicknames** and **`security_id`** only — never raw account numbers or
broker-native symbols (Rule 1/Rule 2).

## In-process SDK — `LedgerClient`

```python
from tt_ledger import LedgerClient

class LedgerClient:
    @classmethod
    def open(cls, url: str = "sqlite+aiosqlite:///ledger.db", *,
             accounts: "AccountMapper", securities: "SecurityResolver | None" = None,
             ) -> "LedgerClient": ...

    # capture
    async def sync(self, account: str, since: date | None = None) -> SyncResult   # pull + reconcile
    async def record_order(self, order: OrderInput) -> OrderRow                    # oms_submit path
    async def apply_fill(self, evt: FillEvent) -> None                             # push path

    # read (consolidated views)
    async def orders(self, **f) -> list[OrderRow]
    async def trades(self, **f) -> list[TradeRow]            # origin / review_status / status / underlying / date range
    async def trade(self, group_id: str) -> TradeRow | None
    async def account_activity(self, account: str, **f) -> list[ActivityRow]
    async def position(self, account: str, security_id: str) -> PositionRow | None
    async def positions(self, account: str, *, open_only: bool = True) -> list[PositionRow]
    async def closed_positions(self, account: str, security_id: str | None = None) -> list[ClosedPositionRow]

    # remap
    async def remap_trade(self, group_id: str, *, strategy=None, bot=None, signal=None,
                          strategy_type=None, reviewed_by: str) -> TradeRow
    async def regroup(self, txn_ids: list[int], *, target: str | None, reviewed_by: str) -> list[TradeRow]
    async def dismiss_trade(self, group_id: str, *, reviewed_by: str) -> TradeRow

    # maintenance (no broker pull)
    async def reconcile(self, account: str | None = None, *, since: date | None = None, dry_run: bool = False) -> SyncResult
    async def rebuild_positions(self, account: str | None = None) -> SyncResult   # docs/ingestion.md → Replay
```

This is what a host platform's services import directly (in-process, transactional). For a standalone
deployment it is also the primary programmatic interface.

## HTTP server — FastAPI (`[api]` extra, optional)

For external/dashboard consumers and inbound integration. Pydantic `BaseModel` DTOs over the
consolidated views.

**Read**
- `GET /orders` — filters: `origin`, `account`, `status`, `underlying`, `from`, `to`.
- `GET /trades` — filters: `origin`, `review_status`, `status`, `account`, `underlying`, `from`, `to`.
- `GET /trades/{group_id}` — a trade with its orders, legs, fills, transactions, events.
- `GET /accounts/{nickname}/activity` — the cash-level ledger (transactions joined to order + group).
- `GET /accounts/{nickname}/positions` — current positions; `?all=true` to include flat rows for
  securities once held (default: open, `quantity != 0`, only).
- `GET /accounts/{nickname}/positions/{security_id}` — a single position.
- `GET /accounts/{nickname}/closed-positions` — completed open→close lifecycles; `?security_id=`.

**Write / remap**
- `POST /trades/{group_id}/remap` · `POST /trades/{group_id}/regroup` · `POST /trades/{group_id}/dismiss`.

**Inbound ingest (reserved for future non-TT sources)**
- `POST /ingest/{source_system}` — broker-neutral order/transaction ingest; validated + idempotent.
  TastyTrade ingest goes through the pull/push adapters, not this endpoint; this is the seam for a
  second source.

DTOs mirror the `*Row` shapes returned by the SDK.

## CLI — `tt-ledger` (`[cli]` extra, typer)

```
tt-ledger sync --account main --since 2026-01-01      # pull + reconcile
tt-ledger trades list [--needs-review] [--origin broker]
tt-ledger trades show <group_id>
tt-ledger trades remap <group_id> --strategy spx_ic [--bot ...] [--signal ...]
tt-ledger trades regroup <group_id> --move <txn_ids> [--to <group_id> | --new]
tt-ledger trades dismiss <group_id>
tt-ledger reconcile [--account] [--since]
tt-ledger positions --account main [--all]
tt-ledger closed-positions --account main [--security-id]
tt-ledger rebuild-positions [--account]                # docs/ingestion.md → Replay
```

Async command bodies; Rich-formatted tables. The CLI calls the same `LedgerClient`.
