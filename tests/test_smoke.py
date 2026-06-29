"""Smoke tests — the package imports and the fully-specified pieces work.

The stub methods raise NotImplementedError by design; these tests only check structure +
the two concrete pieces (enums, the Money type round-trip).
"""

from __future__ import annotations

from decimal import Decimal

import pytest


def test_package_imports() -> None:
    import tt_ledger
    from tt_ledger import LedgerClient, Money  # noqa: F401

    assert tt_ledger.__version__
    assert hasattr(LedgerClient, "open")


def test_enums_concrete() -> None:
    from tt_ledger.enums import Origin, ProductType, ReviewStatus

    assert Origin.BROKER == "broker"
    assert ReviewStatus.NEEDS_REVIEW == "needs_review"
    assert ProductType.OI == "OI"


def test_money_sqlite_roundtrip_is_exact() -> None:
    """The SQLite scaled-int path must not drift (docs/storage.md money landmine)."""
    from sqlalchemy.engine.default import DefaultDialect

    from tt_ledger.money import Money

    class _Sqlite(DefaultDialect):
        name = "sqlite"

    m, d = Money(scale=6), _Sqlite()
    for value in ("1234.5678", "0.0001", "-19.5", "20000.00"):
        stored = m.process_bind_param(Decimal(value), d)
        assert isinstance(stored, int)
        assert m.process_result_value(stored, d) == Decimal(value)


def test_passthrough_resolver_uses_vendor_symbol() -> None:
    """Default symbology: security_id IS the raw vendor symbol (docs/symbology.md)."""
    from tt_ledger.identity import PassthroughResolver

    rs = PassthroughResolver().resolve("/ESM6", "Future")
    assert rs.security_id == "/ESM6"


def test_security_universe_resolver_optional() -> None:
    """Optional adapter ([securities] extra): OCC options get a canonical id; else vendor passthrough."""
    pytest.importorskip("security_universe")
    from tt_ledger.identity import SecurityUniverseResolver

    r = SecurityUniverseResolver()  # default delegate: ChainResolver if available, else OCC
    assert r.resolve("AAPL  250117C00150000", "Equity Option").security_id == "option:AAPL:2025-01-17:call:150"
    assert r.resolve("SPXW  260619P06100000", "Equity Option").underlying == "SPX"  # index rule
    # equity id depends on the installed security-universe: OCC-only → vendor "AAPL";
    # ChainResolver (options + equities + futures) → "equity:AAPL". Accept either.
    assert r.resolve("AAPL", "Equity").security_id in ("AAPL", "equity:AAPL")


def test_money_postgres_is_passthrough() -> None:
    from sqlalchemy.engine.default import DefaultDialect

    from tt_ledger.money import Money

    class _PG(DefaultDialect):
        name = "postgresql"

    m, d = Money(scale=6), _PG()
    assert m.process_bind_param(Decimal("1.23"), d) == Decimal("1.23")
    assert m.process_result_value(Decimal("1.23"), d) == Decimal("1.23")
