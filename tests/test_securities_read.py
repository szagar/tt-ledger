"""``LedgerClient.securities_for`` — the securities-dimension batch read.

The read twin of ``record_order``'s per-leg securities upsert. Primary
consumer: trade-group order construction in the host platform (a closing
order needs the broker-native ``tt_symbol`` the position rows deliberately
don't carry).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.rows import OrderInput, OrderLegInput
from tt_ledger.sdk import LedgerClient
from tt_ledger.store.memory import InMemoryStore

PUT = "SPY   260821P00560000"
CALL = "SPY   260821C00585000"


@pytest.fixture
def client() -> LedgerClient:
    return LedgerClient(
        InMemoryStore(),
        accounts=AccountMapper({"main": "ACCT1"}),
        resolver=PassthroughResolver(),
    )


async def test_securities_for_returns_ingested_rows_keyed_by_security_id(client):
    await client.record_order(
        OrderInput(
            account="main",
            tt_order_id="O-1",
            underlying="SPY",
            legs=[
                OrderLegInput(
                    symbol=PUT, instrument_type="Equity Option",
                    action="Sell to Open", quantity=Decimal("1"),
                ),
                OrderLegInput(
                    symbol=CALL, instrument_type="Equity Option",
                    action="Sell to Open", quantity=Decimal("1"),
                ),
            ],
        )
    )

    # PassthroughResolver: security_id == the vendor symbol.
    rows = await client.securities_for([PUT, CALL])

    assert set(rows) == {PUT, CALL}
    assert rows[PUT].tt_symbol == PUT
    assert rows[CALL].tt_symbol == CALL


async def test_securities_for_omits_unknown_ids_and_dedups(client):
    await client.record_order(
        OrderInput(
            account="main",
            tt_order_id="O-2",
            legs=[
                OrderLegInput(
                    symbol=PUT, instrument_type="Equity Option",
                    action="Buy to Open", quantity=Decimal("1"),
                )
            ],
        )
    )

    rows = await client.securities_for([PUT, PUT, "never-ingested"])

    assert set(rows) == {PUT}


async def test_securities_for_empty_input(client):
    assert await client.securities_for([]) == {}
