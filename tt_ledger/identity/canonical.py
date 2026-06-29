"""OPTIONAL canonical-symbol resolver adapter.

tt-ledger does NOT require this — by default ``security_id`` is the raw vendor symbol
(see ``PassthroughResolver`` in ``identity/securities.py``). This module is for users whose
"system canonical symbology" is a structured scheme like the ZTS ``CanonicalSymbol`` format:

    <product_type>|<underlying>[|<YYYYMMDD>|<strike>|<P|C>]
    e.g.  S|AAPL · OS|AAPL|20250117|150|C · OI|SPXW|20260203|6810|P

Port your ``CanonicalSymbol`` (e.g. from a host platform's ``shared/symbol/``) into the stub
below, then inject ``CanonicalSymbolResolver`` into ``LedgerClient.open(resolver=...)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from .securities import ResolvedSecurity


@dataclass(frozen=True)
class CanonicalSymbol:
    """A structured canonical instrument identity. PORT the bodies from your own scheme."""

    product_type: str
    underlying: str
    expiry: date | None = None
    strike: Decimal | None = None
    option_type: str | None = None

    def __str__(self) -> str:
        raise NotImplementedError("CanonicalSymbol.__str__ — port your canonical format")

    @classmethod
    def from_vendor(cls, vendor_symbol: str, instrument_type: str | None = None) -> "CanonicalSymbol":
        raise NotImplementedError("CanonicalSymbol.from_vendor — port your vendor→canonical logic")


class CanonicalSymbolResolver:
    """SecurityResolver that maps a vendor symbol to a structured CanonicalSymbol string."""

    def resolve(self, vendor_symbol: str, instrument_type: str | None = None) -> ResolvedSecurity:
        cs = CanonicalSymbol.from_vendor(vendor_symbol, instrument_type)
        return ResolvedSecurity(
            security_id=str(cs),
            product_type=cs.product_type,
            underlying=cs.underlying,
            expiry=cs.expiry,
            strike=cs.strike,
            option_type=cs.option_type,
        )
