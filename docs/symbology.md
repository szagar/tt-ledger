# Symbology — the injectable security resolver

tt-ledger does **not impose** a symbology. The canonical `security_id` is whatever an **injected
resolver** returns. By default it is the **raw vendor (TastyTrade) symbol**; inject a resolver only
when you want to translate vendor symbols into your own canonical scheme.

## The contract

```python
@dataclass(frozen=True)
class ResolvedSecurity:
    security_id: str                    # the canonical internal id
    product_type: str | None = None     # S/I/F/OS/OI/OF/CR, or a vendor instrument-type
    underlying: str | None = None
    expiry: date | None = None
    strike: Decimal | None = None
    option_type: str | None = None      # "P" | "C"

@runtime_checkable
class SecurityResolver(Protocol):
    def resolve(self, vendor_symbol: str, instrument_type: str | None = None) -> ResolvedSecurity: ...
```

At ingest, every broker symbol is run through `resolver.resolve(...)`; the result populates the
`securities` dimension (`security_id` + the decomposed metadata + the vendor `tt_symbol` /
`streamer_symbol`). Inject via:

```python
client = LedgerClient.open(url, accounts=mapper, resolver=my_resolver)   # resolver optional
```

## Default — `PassthroughResolver` (zero config)

If you pass no resolver, `security_id` **is** the vendor symbol:

```python
PassthroughResolver().resolve("AAPL  250117C00150000")
# -> ResolvedSecurity(security_id="AAPL  250117C00150000", product_type=<instrument_type>)
```

tt-ledger is fully functional this way — capture, reconcile, views — with no symbology setup. The
canonical id is just the broker's string.

## Optional resolver — `security-universe` (the desired option)

[`security-universe`](https://github.com/szagar/security-universe) translates a vendor symbol into a
stable application `security_id`. tt-ledger adapts it:

```python
from tt_ledger.identity import SecurityUniverseResolver

resolver = SecurityUniverseResolver()                 # default delegate: ChainResolver.default()
client = LedgerClient.open(url, accounts=mapper, resolver=resolver)
# or inject a specific security_universe resolver:
#   SecurityUniverseResolver(OCCSecurityIdResolver.default())
```

With no argument the adapter uses `security_universe`'s **`ChainResolver.default()`** (options +
equities + futures) when available, else `OCCSecurityIdResolver.default()` (options only). Coverage:

| Vendor symbol | → `security_id` |
|---|---|
| `AAPL  250117C00150000` (option) | `option:AAPL:2025-01-17:call:150` |
| `AAPL` (equity) | `equity:AAPL` |
| `/ESM6` (future) | `future:ES:M6` |
| anything unclassified (e.g. crypto) | vendor symbol (passthrough) |

Install the adapter's dependency with the extra (not yet on PyPI):

```bash
pip install -e ".[securities]"     # security-universe @ git+https://github.com/szagar/security-universe
```

`security_universe` is imported **lazily** inside the adapter, so the core package imports without the
extra installed. The adapter reads `Security.security_id`, falling back to the vendor symbol when no
resolver classifies the instrument.

## Optional resolver — your own canonical scheme

If your "system canonical" is a structured format (e.g. ZTS-style `S|AAPL`,
`OS|AAPL|20250117|150|C`), port it into `tt_ledger/identity/canonical.py` and inject
`CanonicalSymbolResolver`. Or implement the `SecurityResolver` Protocol however you like.

## Not in scope: universes / named sets

`security-universe` also models **universes** (watchlists, restricted lists, index membership). tt-ledger
does **not** use that — universes are a *selection/intent* concern that lives upstream of a post-trade
ledger. tt-ledger uses `security-universe` only for **symbology translation** (the resolver). If you
want universes, use the library directly in your selection layer; because both sides key on the same
`security_id`, a universe member joins a ledger `securities` row 1:1.
