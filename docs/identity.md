# Two identity rules

`tt-ledger` has two symmetric identity subsystems. Both follow the same shape:

> **config-driven where a human curates · a durable dimension table for the rest · broker-native ids
> confined to the edge.**

The module **enforces translation at its boundary** — internal code only ever sees the internal
identifier; the broker-native form appears only in calls to the broker and in audit columns.

> ⚠️ All examples below use **placeholder** logins/accounts. Never commit real logins, account
> numbers, or secrets to this repo.

## Rule 1 — Accounts (number ↔ nickname)

**Internal code uses the `nickname`.** The raw `account_number` appears only at broker calls and in the
`account_number` audit columns.

`config/accounts.toml`:

```toml
[trader1]                       # login = a broker credential set (placeholder)
default = "ACCT0001"
client_id     = "…"             # broker OAuth — never commit real secrets
client_secret = "…"
refresh_token = "…"

  [trader1.accounts]
  ACCT0001 = { nickname = "main" }
  ACCT0002 = { nickname = "ira" }
  PAPER001 = { nickname = "main_paper", env = "paper" }   # paper nickname MUST contain "paper"
```

`AccountMapper` (the loaded mapping) public API:

```python
mapper.to_nickname("ACCT0002")        # -> "ira"
mapper.to_account_number("main")      # -> "ACCT0001"
mapper.env_for("main_paper")          # -> "paper"
mapper.list_nicknames(env="live")     # -> ["main", "ira"]
mapper.default_nickname               # -> "main"
```

Rules enforced:
- A `paper` account's nickname **must** contain `"paper"` (loader raises otherwise).
- `env` defaults to `live` when omitted.

**Broker interface config** (which broker environment to talk to):
- Global **sandbox vs production** (REST base URL + streamer WS URL) via a settings field
  (`TT_ENVIRONMENT=sandbox|production`).
- Per-account **live vs paper** via the account's `env`.

*(`AccountMapper` + the TOML loader are ~200 lines; port from the reference implementation in the host
platform's `shared/accounts/`, or reimplement.)*

## Rule 2 — Securities (broker symbol → canonical `security_id`, **injectable**)

**Internal code uses the `security_id`**; broker-native symbols (TastyTrade symbol, OCC, DXLink
streamer) appear only at broker calls and on the `securities` dimension row. But tt-ledger does **not
impose** what the `security_id` *is* — it is produced by an **injectable resolver**:

```python
@runtime_checkable
class SecurityResolver(Protocol):
    def resolve(self, vendor_symbol: str, instrument_type: str | None = None) -> ResolvedSecurity: ...
```

You pass a resolver to `LedgerClient.open(..., resolver=…)`. The resolver returns a `ResolvedSecurity`
(the canonical `security_id` + optional decomposed metadata — product_type / underlying / expiry /
strike / option_type) used to populate the `securities` row.

### Default — vendor symbology (zero config)

If you inject **nothing**, the `PassthroughResolver` is used and **`security_id` *is* the raw vendor
(TastyTrade) symbol**. tt-ledger is fully functional with no symbology config at all — canonical ==
vendor.

### Optional resolvers (translate vendor → your system's canonical scheme)

- **`SecurityUniverseResolver`** — delegates to the [`security-universe`](symbology.md) library
  (`pip install -e ".[securities]"`); a *desired, supported option*.
- **`CanonicalSymbolResolver`** — wrap your own structured scheme (e.g. a ZTS-style
  `S|AAPL` / `OS|AAPL|20250117|150|C` format); see `tt_ledger/identity/canonical.py`.
- **your own** — anything implementing the `SecurityResolver` Protocol.

See [symbology.md](symbology.md) for the resolver contract and the security-universe adapter.

### `config/securities.toml`

Only consumed by a resolver you inject (e.g. index-option-root or futures rules a custom resolver
reads). With the default passthrough resolver it is not needed.

```toml
[index_option_roots]          # example: a custom resolver could route SPX options & keep SPXW distinct
SPX = ["SPX", "SPXW"]

[futures]                     # example: product → exchange / multiplier a custom resolver might use
ES  = { exchange = "CME", multiplier = 50 }
```

The durable **`securities`** table is the persistent vendor↔canonical map, populated at ingest from
whatever the resolver returns (plus the vendor `tt_symbol` / `streamer_symbol`).

## Why two subsystems

*One internal id per domain, broker-native forms only at the edge.* Accounts are fixed (config-driven
nicknames); securities are **pluggable** (a resolver). Both isolate broker-native identifiers to the
ingestion boundary — so the choice of symbology, a paper account, or a contract roll never leaks raw
broker identifiers into the core. The default (vendor passthrough) means it all works out of the box;
inject a resolver only when you want your own canonical scheme.
