"""FastAPI app (docs/api.md -> HTTP server) — thin wrappers over LedgerClient, exercised via
Starlette's ``TestClient``."""

from __future__ import annotations

from datetime import datetime, UTC
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from tt_ledger.api.app import create_app
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.sdk import LedgerClient
from tt_ledger.store.memory import InMemoryStore


@pytest.fixture
def broker() -> MockTastyTradeClient:
    return MockTastyTradeClient()


@pytest.fixture
def ledger_client(broker) -> LedgerClient:
    accounts = AccountMapper({"main": "ACCT1"})
    return LedgerClient(InMemoryStore(), accounts=accounts, resolver=PassthroughResolver(), client=broker)


@pytest.fixture
def api(ledger_client) -> TestClient:
    return TestClient(create_app(ledger_client))


async def _seed_single_leg_trade(ledger_client, broker, order_id="O-1", symbol="AAPL"):
    broker.fill(
        account_number="ACCT1", order_id=order_id, symbol=symbol, instrument_type="Equity",
        action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
        filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
    )
    return await ledger_client.sync("main")


def test_healthz(api):
    resp = api.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_list_orders(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)

    resp = api.get("/orders", params={"account": "main"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["tt_order_id"] == "O-1"
    assert body[0]["origin"] == "broker"


async def test_list_orders_filters_by_origin_string(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)

    resp = api.get("/orders", params={"account": "main", "origin": "zts"})
    assert resp.status_code == 200
    assert resp.json() == []


async def test_list_trades(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)

    resp = api.get("/trades", params={"account": "main"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["strategy_type"] == "single"
    assert body[0]["review_status"] == "needs_review"


async def test_list_trades_filters_by_status(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)

    resp = api.get("/trades", params={"account": "main", "status": "open"})
    assert len(resp.json()) == 1

    resp = api.get("/trades", params={"account": "main", "status": "closed"})
    assert resp.json() == []


async def test_get_trade_detail_includes_orders_and_transactions(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)
    group_id = (await ledger_client.trades(account="main"))[0].group_id

    resp = api.get(f"/trades/{group_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["group_id"] == group_id
    assert len(body["orders"]) == 1
    assert body["orders"][0]["tt_order_id"] == "O-1"
    assert len(body["transactions"]) == 1


def test_get_trade_detail_404_for_unknown(api):
    resp = api.get("/trades/does-not-exist")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


async def test_account_activity(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)

    resp = api.get("/accounts/main/activity")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["origin"] == "broker"


async def test_account_activity_unreconciled_only(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)

    resp = api.get("/accounts/main/activity", params={"unreconciled_only": "true"})
    assert resp.status_code == 200
    assert resp.json() == []  # already linked+grouped by sync()


async def test_list_positions_open_only_by_default(api, ledger_client, broker):
    broker.fill(account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity", action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled")
    broker.fill(account_number="ACCT1", order_id="O-2", symbol="AAPL", instrument_type="Equity", action="Sell to Close", quantity=Decimal("10"), fill_price=Decimal("170"), filled_at=datetime(2026, 1, 8, tzinfo=UTC), status="Filled")
    await ledger_client.sync("main")
    await ledger_client.rebuild_positions("main")

    resp = api.get("/accounts/main/positions")
    assert resp.status_code == 200
    assert resp.json() == []

    resp = api.get("/accounts/main/positions", params={"all": "true"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["security_id"] == "AAPL"
    assert body[0]["quantity"] == "0"


async def test_get_position(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)
    await ledger_client.rebuild_positions("main")

    resp = api.get("/accounts/main/positions/AAPL")
    assert resp.status_code == 200
    assert resp.json()["quantity"] == "10"


def test_get_position_404_for_unknown(api):
    resp = api.get("/accounts/main/positions/AAPL")
    assert resp.status_code == 404


async def test_list_closed_positions(api, ledger_client, broker):
    broker.fill(account_number="ACCT1", order_id="O-1", symbol="AAPL", instrument_type="Equity", action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled")
    broker.fill(account_number="ACCT1", order_id="O-2", symbol="AAPL", instrument_type="Equity", action="Sell to Close", quantity=Decimal("10"), fill_price=Decimal("170"), filled_at=datetime(2026, 1, 8, tzinfo=UTC), status="Filled")
    await ledger_client.sync("main")
    await ledger_client.rebuild_positions("main")

    resp = api.get("/accounts/main/closed-positions")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["realized_pnl"] == "200"

    resp = api.get("/accounts/main/closed-positions", params={"security_id": "MSFT"})
    assert resp.json() == []


async def test_remap_trade(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)
    group_id = (await ledger_client.trades(account="main"))[0].group_id

    resp = api.post(f"/trades/{group_id}/remap", json={"strategy": 42, "bot": "my-bot", "reviewed_by": "alice"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["strategy_id"] == 42
    assert body["bot_name"] == "my-bot"
    assert body["manually_attributed"] is True
    assert body["review_status"] == "confirmed"


def test_remap_trade_404_for_unknown_group(api):
    resp = api.post("/trades/does-not-exist/remap", json={"reviewed_by": "alice"})
    assert resp.status_code == 404


async def test_dismiss_trade(api, ledger_client, broker):
    await _seed_single_leg_trade(ledger_client, broker)
    group_id = (await ledger_client.trades(account="main"))[0].group_id

    resp = api.post(f"/trades/{group_id}/dismiss", json={"reviewed_by": "alice"})
    assert resp.status_code == 200
    assert resp.json()["review_status"] == "ignored"


async def test_regroup_trade(api, ledger_client, broker):
    t0 = datetime(2026, 1, 5, 10, 0, tzinfo=UTC)
    broker.fill(account_number="ACCT1", order_id="O-A", symbol="AAPL", instrument_type="Equity", action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"), filled_at=t0, status="Filled")
    broker.fill(account_number="ACCT1", order_id="O-B", symbol="MSFT", instrument_type="Equity", action="Buy to Open", quantity=Decimal("5"), fill_price=Decimal("300"), filled_at=t0, status="Filled")
    await ledger_client.sync("main")

    trades = await ledger_client.trades(account="main")
    assert len(trades) == 1
    group_id = trades[0].group_id

    store = ledger_client._store
    txn = next(row for _, row in store._transactions.all() if row.tt_order_id == "O-B")
    txn_id = store._transactions.id_of(txn.tt_transaction_id)

    resp = api.post(f"/trades/{group_id}/regroup", json={"txn_ids": [txn_id], "reviewed_by": "alice"})
    assert resp.status_code == 200
    assert len(resp.json()) == 2


def test_ingest_reserved_endpoint_returns_501(api):
    resp = api.post("/ingest/schwab", json={})
    assert resp.status_code == 501
    assert "schwab" in resp.json()["detail"]


def test_package_import_works_without_fastapi_installed():
    """``import tt_ledger.api`` must not eagerly pull in fastapi -- only create_app() does.
    Simulates fastapi being absent by blocking the import, independent of whether it's actually
    installed in this environment."""
    import builtins
    import importlib
    import sys

    real_import = builtins.__import__

    def _blocked_import(name, *args, **kwargs):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    for mod in list(sys.modules):
        if mod.startswith("tt_ledger.api"):
            del sys.modules[mod]

    builtins.__import__ = _blocked_import
    try:
        importlib.import_module("tt_ledger.api")  # must not raise
    finally:
        builtins.__import__ = real_import
        for mod in list(sys.modules):
            if mod.startswith("tt_ledger.api"):
                del sys.modules[mod]
        importlib.import_module("tt_ledger.api")  # restore a normal import for later tests
