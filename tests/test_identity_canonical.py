"""``CanonicalSymbol`` / ``CanonicalSymbolResolver`` (docs/symbology.md -> "your own canonical scheme")."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from tt_ledger.identity.canonical import CanonicalSymbol, CanonicalSymbolResolver


def test_equity():
    cs = CanonicalSymbol.from_vendor("AAPL", "Equity")
    assert cs == CanonicalSymbol(product_type="S", underlying="AAPL")
    assert str(cs) == "S|AAPL"


def test_equity_option_occ_symbol():
    cs = CanonicalSymbol.from_vendor("AAPL  250117C00150000", "Equity Option")
    assert cs.product_type == "OS"
    assert cs.underlying == "AAPL"
    assert cs.expiry == date(2025, 1, 17)
    assert cs.strike == Decimal("150")
    assert cs.option_type == "C"
    assert str(cs) == "OS|AAPL|20250117|150|C"


def test_equity_option_no_padding_still_parses():
    # a hypothetical 6-char root needs no padding spaces before the date
    cs = CanonicalSymbol.from_vendor("ABCDEF250117C00150000", "Equity Option")
    assert cs.underlying == "ABCDEF"
    assert cs.expiry == date(2025, 1, 17)


def test_equity_option_fractional_strike():
    cs = CanonicalSymbol.from_vendor("AAPL  250117C00150500", "Equity Option")
    assert cs.strike == Decimal("150.5")
    assert str(cs).endswith("|150.5|C")


def test_equity_option_index_root_via_config():
    cs = CanonicalSymbol.from_vendor(
        "SPXW  260619P06100000", "Equity Option", index_option_roots={"SPX": ["SPX", "SPXW"]},
    )
    assert cs.product_type == "OI"
    assert cs.underlying == "SPXW"
    assert cs.strike == Decimal("6100")
    assert cs.option_type == "P"
    assert str(cs) == "OI|SPXW|20260619|6100|P"


def test_equity_option_without_index_config_is_os():
    cs = CanonicalSymbol.from_vendor("SPXW  260619P06100000", "Equity Option")
    assert cs.product_type == "OS"


def test_future():
    cs = CanonicalSymbol.from_vendor("/ESM6", "Future")
    assert cs == CanonicalSymbol(product_type="F", underlying="/ES")
    assert str(cs) == "F|/ES"  # no expiry (day unrecoverable from the symbol) -> no bracketed segment


def test_future_two_digit_year():
    cs = CanonicalSymbol.from_vendor("/ESM26", "Future")
    assert cs.underlying == "/ES"


def test_cryptocurrency():
    cs = CanonicalSymbol.from_vendor("BTC/USD", "Cryptocurrency")
    assert cs == CanonicalSymbol(product_type="CR", underlying="BTC/USD")
    assert str(cs) == "CR|BTC/USD"


def test_future_option_falls_through_to_raw_symbol():
    cs = CanonicalSymbol.from_vendor("./ESM6 EW1M6 251003P5900", "Future Option")
    assert cs.product_type == "OF"
    assert cs.underlying == "./ESM6 EW1M6 251003P5900"
    assert cs.expiry is None
    assert cs.option_type is None


def test_resolver_wraps_resolved_security():
    resolver = CanonicalSymbolResolver()
    rs = resolver.resolve("AAPL  250117C00150000", "Equity Option")
    assert rs.security_id == "OS|AAPL|20250117|150|C"
    assert rs.product_type == "OS"
    assert rs.underlying == "AAPL"
    assert rs.expiry == date(2025, 1, 17)
    assert rs.strike == Decimal("150")
    assert rs.option_type == "C"


def test_resolver_uses_configured_index_option_roots():
    resolver = CanonicalSymbolResolver(index_option_roots={"SPX": ["SPX", "SPXW"]})
    rs = resolver.resolve("SPXW  260619P06100000", "Equity Option")
    assert rs.product_type == "OI"
