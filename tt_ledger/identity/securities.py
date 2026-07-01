"""Symbology resolution — broker (vendor) symbol → the user's canonical ``security_id``.

**Injectable.** tt-ledger does not impose a symbology. You pass a ``SecurityResolver`` to
``LedgerClient.open(...)``; if you pass none, the :class:`PassthroughResolver` is used and the
canonical ``security_id`` simply *is* the raw vendor (TastyTrade) symbol.

Resolvers translate a vendor symbol into a ``ResolvedSecurity`` (the canonical id + optional
decomposed metadata used to populate the ``securities`` dimension). Optional adapters ship for:

* :class:`SecurityUniverseResolver` — delegates to the ``security-universe`` library
  (``pip install -e ".[securities]"``).
* a ``CanonicalSymbolResolver`` — wrap your own canonical scheme (see ``identity/canonical.py``).

See docs/symbology.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class ResolvedSecurity:
    """What a resolver returns for one vendor symbol."""

    security_id: str                     # the canonical internal id (default: the vendor symbol)
    product_type: str | None = None      # e.g. S/I/F/OS/OI/OF/CR, or a vendor instrument-type
    underlying: str | None = None
    expiry: date | None = None
    strike: Decimal | None = None
    option_type: str | None = None       # "P" | "C"


@runtime_checkable
class SecurityResolver(Protocol):
    """Translate a broker-native symbol into a canonical ``ResolvedSecurity``."""

    def resolve(self, vendor_symbol: str, instrument_type: str | None = None) -> ResolvedSecurity: ...


# TastyTrade instrument-type -> the schema's short ProductType code (docs/schema.md: S/I/F/OS/OI/OF/CR).
# ``securities.product_type`` is a 2-char column — the raw instrument-type string (e.g. "Equity
# Option") does not fit it, so even the zero-config passthrough path must normalize this one field.
_TT_INSTRUMENT_TYPE_TO_PRODUCT: dict[str, str] = {
    "equity": "S",
    "index": "I",
    "future": "F",
    "equity option": "OS",
    "index option": "OI",
    "future option": "OF",
    "cryptocurrency": "CR",
    "crypto": "CR",
}


def _instrument_type_to_product_type(instrument_type: str | None) -> str | None:
    if instrument_type is None:
        return None
    return _TT_INSTRUMENT_TYPE_TO_PRODUCT.get(instrument_type.strip().lower())


class PassthroughResolver:
    """Default resolver — ``security_id`` *is* the vendor symbol; no decomposition.

    This makes tt-ledger work with **zero symbology config**: canonical == vendor. The vendor
    ``instrument_type`` is still normalized to the schema's short ``product_type`` code (an
    unrecognized/omitted instrument type maps to ``None``, not a truncated raw string).
    """

    def resolve(self, vendor_symbol: str, instrument_type: str | None = None) -> ResolvedSecurity:
        return ResolvedSecurity(
            security_id=vendor_symbol, product_type=_instrument_type_to_product_type(instrument_type),
        )


# TastyTrade instrument-type → security_universe SecurityType (value strings; resolved lazily).
_TT_TO_SU_TYPE: dict[str, str] = {
    "equity": "stock",
    "equity option": "option",
    "future": "future",
    "future option": "option",
    "cryptocurrency": "crypto",
    "crypto": "crypto",
}
# security_universe SecurityType → tt-ledger ProductType (best-effort; security-universe does not
# distinguish option-on-stock/index/future, so OPTION maps to the generic "OS").
_SU_TYPE_TO_PRODUCT: dict[str, str] = {
    "stock": "S",
    "etf": "S",
    "future": "F",
    "option": "OS",
    "crypto": "CR",
}


class SecurityUniverseResolver:
    """Adapter over the ``security-universe`` library (``[securities]`` extra; not yet on PyPI).

    Delegates to a ``security_universe`` ``SecurityIdResolver`` to turn a vendor symbol into a
    canonical ``security_id``. ``security_universe`` is imported lazily, so the core package imports
    without the extra installed.

    >>> from tt_ledger.identity import SecurityUniverseResolver
    >>> r = SecurityUniverseResolver()                       # defaults to OCCSecurityIdResolver.default()
    >>> r.resolve("AAPL  250117C00150000", "Equity Option").security_id
    'option:AAPL:2025-01-17:call:150'
    >>> r.resolve("AAPL", "Equity").security_id              # equity (via ChainResolver) -> "equity:AAPL"

    The default delegate is ``security_universe``'s ``ChainResolver.default()`` (options + equities +
    futures) when available, else its ``OCCSecurityIdResolver.default()`` (options only). Anything a
    resolver can't classify falls through to the vendor symbol. See docs/symbology.md.
    """

    def __init__(self, su_resolver=None) -> None:  # noqa: ANN001  (a security_universe SecurityIdResolver)
        self._su = su_resolver  # None -> lazily build the broadest available default resolver

    def _delegate(self):
        if self._su is None:
            try:
                # Prefer the composite resolver (options + equities + futures) when present.
                from security_universe.resolvers import ChainResolver

                self._su = ChainResolver.default()
            except ImportError:
                try:
                    from security_universe.resolvers.occ import OCCSecurityIdResolver
                except ModuleNotFoundError as exc:  # pragma: no cover
                    raise RuntimeError(
                        "SecurityUniverseResolver needs the [securities] extra: "
                        "pip install -e '.[securities]'"
                    ) from exc
                self._su = OCCSecurityIdResolver.default()
        return self._su

    def resolve(self, vendor_symbol: str, instrument_type: str | None = None) -> ResolvedSecurity:
        from security_universe import OptionType, Security, SecurityType  # lazy; needs [securities]

        su_type = SecurityType(_TT_TO_SU_TYPE.get((instrument_type or "").strip().lower(), "unknown"))
        resolved = self._delegate().resolve(Security(symbol=vendor_symbol, security_type=su_type))

        opt = resolved.option_type
        return ResolvedSecurity(
            # security-universe assigns security_id only for OCC options; else fall back to the vendor symbol
            security_id=resolved.security_id or resolved.symbol,
            product_type=_SU_TYPE_TO_PRODUCT.get(str(resolved.security_type)),
            underlying=resolved.underlying,
            expiry=resolved.expiry,
            strike=resolved.strike,
            option_type=("C" if opt == OptionType.CALL else "P" if opt == OptionType.PUT else None),
        )
