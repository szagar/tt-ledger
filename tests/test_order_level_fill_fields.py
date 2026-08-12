"""order_level_fill_fields — the derived order-level fill aggregates.

TastyTrade's Order object has no order-level fill fields (verified against
their OpenAPI spec), so single-leg VWAP/Σ and the multi-leg per-structure-unit
aggregates are derived from ``legs[].fills``. The multi-leg case is what the
2026-08-11 live /ES iron-fly exposed: every multi-leg order row sat at
``filled_quantity`` 0/NULL with no net fill price.
"""

from datetime import UTC, datetime
from decimal import Decimal

from tt_ledger.ingest.broker import PlacedFill, PlacedLeg
from tt_ledger.repositories import order_level_fill_fields

_AT = datetime(2026, 8, 12, 1, 41, 55, tzinfo=UTC)


def _leg(action: str, qty: str, fills: list[tuple[str, str]], remaining: str = "0") -> PlacedLeg:
    return PlacedLeg(
        instrument_type="Future Option",
        symbol="./ESU6 E2CQ6 260812P7750",
        action=action,
        quantity=Decimal(qty),
        remaining_quantity=Decimal(remaining),
        fills=[
            PlacedFill(fill_id=f"f{i}", quantity=Decimal(q), fill_price=Decimal(p), filled_at=_AT)
            for i, (q, p) in enumerate(fills)
        ],
    )


def test_single_leg_unchanged() -> None:
    leg = _leg("Buy to Open", "2", [("1", "1.10"), ("1", "1.30")])
    avg, filled, remaining = order_level_fill_fields([leg])
    assert (avg, filled, remaining) == (Decimal("1.20"), Decimal("2"), Decimal("0"))


def test_empty_and_unfilled_single_leg() -> None:
    assert order_level_fill_fields([]) == (None, None, None)
    leg = _leg("Buy to Open", "1", [], remaining="1")
    avg, filled, remaining = order_level_fill_fields([leg])
    assert (avg, filled, remaining) == (None, Decimal("0"), Decimal("1"))


def test_iron_fly_credit_net() -> None:
    # The live /ES fly: sells 23.50 + 22.50, buys 11.00 + 11.50 → net credit 23.50.
    legs = [
        _leg("Sell to Open", "1", [("1", "23.50")]),
        _leg("Sell to Open", "1", [("1", "22.50")]),
        _leg("Buy to Open", "1", [("1", "11.00")]),
        _leg("Buy to Open", "1", [("1", "11.50")]),
    ]
    avg, filled, remaining = order_level_fill_fields(legs, price_effect="Credit")
    assert (avg, filled, remaining) == (Decimal("23.50"), Decimal("1"), Decimal("0"))


def test_debit_spread_reads_on_price_axis() -> None:
    # Buy 5.00 / sell 2.00 → net debit 3.00, positive under price_effect=Debit
    # (same axis as the order's limit price).
    legs = [
        _leg("Buy to Open", "1", [("1", "5.00")]),
        _leg("Sell to Open", "1", [("1", "2.00")]),
    ]
    avg, filled, remaining = order_level_fill_fields(legs, price_effect="Debit")
    assert (avg, filled, remaining) == (Decimal("3.00"), Decimal("1"), Decimal("0"))


def test_two_lot_fly_counts_structure_units() -> None:
    legs = [
        _leg("Sell to Open", "2", [("2", "23.50")]),
        _leg("Sell to Open", "2", [("2", "22.50")]),
        _leg("Buy to Open", "2", [("2", "11.00")]),
        _leg("Buy to Open", "2", [("2", "11.50")]),
    ]
    avg, filled, remaining = order_level_fill_fields(legs, price_effect="Credit")
    assert (avg, filled, remaining) == (Decimal("23.50"), Decimal("2"), Decimal("0"))


def test_ratio_spread_units_via_gcd() -> None:
    # 1×2 put ratio: buy 1 @ 10.00, sell 2 @ 4.00 → 1 unit, net debit 2.00.
    legs = [
        _leg("Buy to Open", "1", [("1", "10.00")]),
        _leg("Sell to Open", "2", [("2", "4.00")]),
    ]
    avg, filled, remaining = order_level_fill_fields(legs, price_effect="Debit")
    assert (avg, filled, remaining) == (Decimal("2.00"), Decimal("1"), Decimal("0"))


def test_partial_multi_leg_no_net_price() -> None:
    # One leg filled, one working: no net price (a partial net would compare a
    # half-executed spread against a whole-spread limit); 0 units complete.
    legs = [
        _leg("Sell to Open", "1", [("1", "23.50")]),
        _leg("Buy to Open", "1", [], remaining="1"),
    ]
    avg, filled, remaining = order_level_fill_fields(legs, price_effect="Credit")
    assert (avg, filled, remaining) == (None, Decimal("0"), Decimal("1"))


def test_non_integral_leg_quantity_stays_conservative() -> None:
    legs = [
        _leg("Sell to Open", "1.5", [("1.5", "23.50")]),
        _leg("Buy to Open", "1", [("1", "11.00")]),
    ]
    assert order_level_fill_fields(legs) == (None, None, None)
