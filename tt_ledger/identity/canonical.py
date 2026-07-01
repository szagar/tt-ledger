"""OPTIONAL canonical-symbol resolver adapter.

tt-ledger does NOT require this — by default ``security_id`` is the raw vendor symbol
(see ``PassthroughResolver`` in ``identity/securities.py``). This module is for users whose
"system canonical symbology" is a structured scheme like the ZTS ``CanonicalSymbol`` format:

    <product_type>|<underlying>[|<YYYYMMDD>|<strike>|<P|C>]
    e.g.  S|AAPL · OS|AAPL|20250117|150|C · OI|SPXW|20260203|6810|P

``from_vendor`` parses TastyTrade's own vendor symbol conventions:
  * Equity           — the ticker as-is.
  * Equity Option     — the standard 21-char OCC symbol (root padded to 6 + YYMMDD + C/P + 8-digit
    strike in mills).
  * Future            — ``/<root><month code><1-2 digit year>`` (e.g. ``/ESM6``). The exact
    expiration day isn't recoverable from the symbol alone, so ``expiry`` is left ``None``.
  * Cryptocurrency     — the pair as-is (e.g. ``BTC/USD``).
  * Future Option and anything else unrecognized — no safe structural parse without a vendor
    spec; falls through to the raw vendor symbol as ``underlying``, same spirit as
    ``PassthroughResolver`` (see docs/symbology.md's own "anything unclassified" fallback).

Inject ``CanonicalSymbolResolver`` into ``LedgerClient.open(resolver=...)``. Pass
``index_option_roots`` (from ``load_securities_toml``) to classify weekly/index option roots
(e.g. ``SPXW``) as ``OI`` instead of the generic ``OS``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from .securities import ResolvedSecurity

# Standard OCC option symbol: root (left-justified, padded to 6 with spaces) + YYMMDD + C/P + 8-digit
# strike (price * 1000). The root is matched non-greedily; the fixed-length suffix pins the split.
_OCC_RE = re.compile(r"^(?P<root>[A-Z][A-Z0-9]{0,5}?)\s*(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})(?P<cp>[CP])(?P<strike>\d{8})$")

# TastyTrade future symbol: "/" + root + one CME month code + a 1-2 digit year (e.g. "/ESM6", "/ESM26").
_FUTURE_MONTH_CODES = "FGHJKMNQUVXZ"
_FUTURE_RE = re.compile(rf"^(?P<root>[A-Z0-9]+)(?P<month>[{_FUTURE_MONTH_CODES}])(?P<year>\d{{1,2}})$")


def _format_strike(strike: Decimal) -> str:
    s = format(strike, "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


def _is_index_root(root: str, index_option_roots: dict[str, list[str]] | None) -> bool:
    if not index_option_roots:
        return False
    return any(root == key or root in roots for key, roots in index_option_roots.items())


@dataclass(frozen=True)
class CanonicalSymbol:
    """A structured canonical instrument identity (ZTS-style ``product_type|underlying[|...]``)."""

    product_type: str
    underlying: str
    expiry: date | None = None
    strike: Decimal | None = None
    option_type: str | None = None

    def __str__(self) -> str:
        parts = [self.product_type, self.underlying]
        if self.option_type is not None:
            parts.append(self.expiry.strftime("%Y%m%d") if self.expiry else "")
            parts.append(_format_strike(self.strike) if self.strike is not None else "")
            parts.append(self.option_type)
        return "|".join(parts)

    @classmethod
    def from_vendor(
        cls,
        vendor_symbol: str,
        instrument_type: str | None = None,
        *,
        index_option_roots: dict[str, list[str]] | None = None,
    ) -> "CanonicalSymbol":
        it = (instrument_type or "").strip().lower()
        symbol = vendor_symbol.strip()

        if it == "equity":
            return cls(product_type="S", underlying=symbol)

        if it == "index":
            return cls(product_type="I", underlying=symbol)

        if it in ("cryptocurrency", "crypto"):
            return cls(product_type="CR", underlying=symbol)

        if it == "equity option":
            m = _OCC_RE.match(vendor_symbol.strip())
            if not m:
                raise ValueError(f"unrecognized OCC option symbol: {vendor_symbol!r}")
            root = m["root"]
            product_type = "OI" if _is_index_root(root, index_option_roots) else "OS"
            return cls(
                product_type=product_type,
                underlying=root,
                expiry=date(2000 + int(m["yy"]), int(m["mm"]), int(m["dd"])),
                strike=Decimal(m["strike"]) / 1000,
                option_type=m["cp"],
            )

        if it == "future":
            m = _FUTURE_RE.match(symbol.lstrip("/"))
            if not m:
                raise ValueError(f"unrecognized future symbol: {vendor_symbol!r}")
            return cls(product_type="F", underlying="/" + m["root"])

        # Future Option (multi-part, vendor-specific format) and anything else unrecognized: no
        # safe structural parse here — passthrough the raw symbol as the underlying.
        return cls(product_type="OF" if it == "future option" else it, underlying=symbol)


class CanonicalSymbolResolver:
    """SecurityResolver that maps a vendor symbol to a structured CanonicalSymbol string."""

    def __init__(self, index_option_roots: dict[str, list[str]] | None = None) -> None:
        self._index_option_roots = index_option_roots

    def resolve(self, vendor_symbol: str, instrument_type: str | None = None) -> ResolvedSecurity:
        cs = CanonicalSymbol.from_vendor(
            vendor_symbol, instrument_type, index_option_roots=self._index_option_roots,
        )
        return ResolvedSecurity(
            security_id=str(cs),
            product_type=cs.product_type,
            underlying=cs.underlying,
            expiry=cs.expiry,
            strike=cs.strike,
            option_type=cs.option_type,
        )
