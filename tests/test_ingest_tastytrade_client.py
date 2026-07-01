"""``TastyTradeClient`` (docs/ingestion.md -> Pull) — the real ``BrokerClient``.

No real network calls: ``httpx.MockTransport`` simulates the TastyTrade API using the actual
example JSON captured from developer.tastytrade.com during the "check against API docs" session
(the transactions example is copied verbatim from their docs), so these tests exercise the exact
response shapes TastyTrade documents, not shapes I invented.
"""

from __future__ import annotations

from datetime import date, datetime, UTC
from decimal import Decimal

import httpx
import pytest

from tt_ledger.ingest.broker import BrokerClient
from tt_ledger.ingest.tastytrade_client import TastyTradeApiError, TastyTradeClient

TOKEN_RESPONSE = {"access_token": "abc123", "token_type": "Bearer", "expires_in": 900}

# Verbatim from developer.tastytrade.com's "List Transactions" example (dividend reinvestment +
# the matching cash transaction), plus one synthetic Trade-type row (fees/order-id weren't shown
# in their dividend example) to exercise the fields their docs didn't happen to illustrate.
TRANSACTIONS_RESPONSE = {
    "data": {
        "items": [
            {
                "id": 252640963,
                "account-number": "5WT0001",
                "symbol": "KBWD",
                "instrument-type": "Equity",
                "underlying-symbol": "KBWD",
                "transaction-type": "Receive Deliver",
                "transaction-sub-type": "Dividend",
                "description": "Received 1.68074 Long KBWD via Dividend",
                "action": "Buy to Open",
                "quantity": "1.68074",
                "price": "16.46",
                "executed-at": "2023-07-28T21:00:00.000+00:00",
                "transaction-date": "2023-07-28",
                "value": "0.0",
                "value-effect": "None",
                "net-value": "0.0",
                "net-value-effect": "None",
                "is-estimated-fee": True,
            },
            {
                "id": 252640962,
                "account-number": "5WT0001",
                "symbol": "PGX",
                "instrument-type": "Equity",
                "underlying-symbol": "PGX",
                "transaction-type": "Money Movement",
                "transaction-sub-type": "Dividend",
                "description": "INVESCO EXCHANGE TRADED FD TR",
                "executed-at": "2023-07-28T21:00:00.000+00:00",
                "transaction-date": "2023-07-28",
                "value": "27.74",
                "value-effect": "Credit",
                "net-value": "27.74",
                "net-value-effect": "Credit",
                "is-estimated-fee": True,
            },
            {
                "id": 252700000,
                "account-number": "5WT0001",
                "order-id": 9988776,
                "symbol": "AAPL",
                "instrument-type": "Equity",
                "underlying-symbol": "AAPL",
                "transaction-type": "Trade",
                "action": "Buy to Open",
                "quantity": "10",
                "price": "150.25",
                "executed-at": "2026-01-05T15:00:00.000+00:00",
                "transaction-date": "2026-01-05",
                "net-value": "-1502.50",
                "net-value-effect": "Debit",
                "commission": "1.00",
                "clearing-fees": "0.05",
                "regulatory-fees": "0.02",
                "is-estimated-fee": False,
            },
        ]
    },
    "api-version": "v1",
    "context": "/accounts/5WT0001/transactions",
    "pagination": {
        "per-page": 250, "page-offset": 0, "item-offset": 0, "total-items": 3,
        "total-pages": 1, "current-item-count": 3, "previous-link": None, "next-link": None,
    },
}

# Shaped from the verified Order/CurrentPosition OpenAPI definitions.
ORDERS_RESPONSE = {
    "data": {
        "items": [
            {
                "id": "O-1",
                "account-number": "5WT0001",
                "received-at": "2026-01-05T15:00:00.000+00:00",
                "underlying-symbol": "AAPL",
                "order-type": "Limit",
                "time-in-force": "Day",
                "price": "150.25",
                "price-effect": "Debit",
                "status": "Filled",
                "terminal-at": "2026-01-05T15:00:01.000+00:00",
                "legs": [
                    {
                        "instrument-type": "Equity",
                        "symbol": "AAPL",
                        "action": "Buy to Open",
                        "quantity": "10",
                        "remaining-quantity": "0",
                        "fills": [
                            {
                                "fill-id": "F-1",
                                "quantity": "10",
                                "fill-price": "150.25",
                                "filled-at": "2026-01-05T15:00:01.000+00:00",
                                "destination-venue": "NASDAQ",
                            }
                        ],
                    }
                ],
            },
            {
                "id": "O-2",
                "account-number": "5WT0001",
                "received-at": "2026-01-06T15:00:00.000+00:00",
                "status": "Rejected",
                "reject-reason": "Insufficient buying power",
                "legs": [],
            },
        ]
    },
    "context": "/accounts/5WT0001/orders",
    "pagination": {
        "per-page": 250, "page-offset": 0, "item-offset": 0, "total-items": 2,
        "total-pages": 1, "current-item-count": 2, "previous-link": None, "next-link": None,
    },
}

POSITIONS_RESPONSE = {
    "data": {
        "items": [
            {
                "account-number": "5WT0001",
                "symbol": "AAPL",
                "underlying-symbol": "AAPL",
                "instrument-type": "Equity",
                "quantity": "100",
                "quantity-direction": "Long",
                "average-open-price": "150.00",
                "mark-price": "155.50",
                "realized-day-gain": "42.00",
                "realized-day-gain-effect": "Credit",
                "multiplier": 1,
            }
        ]
    },
    "context": "/accounts/5WT0001/positions",
}


def _json_response(status_code: int, body: dict) -> httpx.Response:
    return httpx.Response(status_code, json=body)


def _make_client(handler) -> TastyTradeClient:
    client = TastyTradeClient(client_id="id", client_secret="secret", refresh_token="refresh")
    client._http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=client._http.base_url,
        headers=dict(client._http.headers),
    )
    return client


def test_conforms_to_broker_client_protocol():
    assert isinstance(TastyTradeClient(client_id="a", client_secret="b", refresh_token="c"), BrokerClient)


def test_without_httpx_installed_raises_a_clear_error(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "httpx":
            raise ModuleNotFoundError("No module named 'httpx'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    with pytest.raises(RuntimeError, match=r"\[tastytrade\]"):
        TastyTradeClient(client_id="a", client_secret="b", refresh_token="c")


async def test_authenticates_and_fetches_transactions():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.url.path == "/oauth/token":
            assert request.headers["User-Agent"] == "tt-ledger/0.1.0"
            body = dict(x.split("=") for x in request.content.decode().split("&"))
            assert body["grant_type"] == "refresh_token"
            assert body["refresh_token"] == "refresh"
            return _json_response(200, TOKEN_RESPONSE)
        if request.url.path == "/accounts/5WT0001/transactions":
            assert request.headers["Authorization"] == "Bearer abc123"
            assert request.url.params["start-date"] == "2026-01-01"
            return _json_response(200, TRANSACTIONS_RESPONSE)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    client = _make_client(handler)
    txns = await client.get_transaction_history("5WT0001", date(2026, 1, 1), date(2026, 2, 1))

    assert ("POST", "/oauth/token") in calls
    assert len(txns) == 3

    dividend = txns[0]
    assert dividend.id == "252640963"
    assert dividend.account_number == "5WT0001"
    assert dividend.symbol == "KBWD"
    assert dividend.transaction_type == "Receive Deliver"
    assert dividend.transaction_sub_type == "Dividend"
    assert dividend.quantity == Decimal("1.68074")
    assert dividend.price == Decimal("16.46")
    assert dividend.value == Decimal("0.0")
    assert dividend.value_effect == "None"
    assert dividend.executed_at == datetime(2023, 7, 28, 21, 0, tzinfo=UTC)
    assert dividend.transaction_date == date(2023, 7, 28)
    assert dividend.is_estimated_fee is True
    assert dividend.order_id is None

    trade = txns[2]
    assert trade.order_id == "9988776"
    assert trade.net_value == Decimal("-1502.50")
    assert trade.net_value_effect == "Debit"
    assert trade.commission == Decimal("1.00")
    assert trade.clearing_fees == Decimal("0.05")
    assert trade.regulatory_fees == Decimal("0.02")
    assert trade.is_estimated_fee is False


async def test_fetches_orders_with_legs_fills_and_reject_reason():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/token":
            return _json_response(200, TOKEN_RESPONSE)
        if request.url.path == "/accounts/5WT0001/orders":
            return _json_response(200, ORDERS_RESPONSE)
        raise AssertionError(f"unexpected request: {request.url}")

    client = _make_client(handler)
    orders = await client.get_order_history("5WT0001", date(2026, 1, 1), date(2026, 2, 1))

    assert len(orders) == 2
    filled = orders[0]
    assert filled.id == "O-1"
    assert filled.status == "Filled"
    assert len(filled.legs) == 1
    leg = filled.legs[0]
    assert leg.symbol == "AAPL"
    assert leg.quantity == Decimal("10")
    assert leg.remaining_quantity == Decimal("0")
    assert len(leg.fills) == 1
    assert leg.fills[0].fill_price == Decimal("150.25")
    assert leg.fills[0].destination_venue == "NASDAQ"
    assert filled.is_complex is False  # no complex-order-id
    assert filled.complex_order_id is None

    rejected = orders[1]
    assert rejected.status == "Rejected"
    assert rejected.reject_reason == "Insufficient buying power"
    assert rejected.legs == []


async def test_fetches_positions_with_realized_day_gain_effect():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/token":
            return _json_response(200, TOKEN_RESPONSE)
        if request.url.path == "/accounts/5WT0001/positions":
            return _json_response(200, POSITIONS_RESPONSE)
        raise AssertionError(f"unexpected request: {request.url}")

    client = _make_client(handler)
    positions = await client.get_positions("5WT0001")

    assert len(positions) == 1
    p = positions[0]
    assert p.symbol == "AAPL"
    assert p.quantity == Decimal("100")
    assert p.quantity_direction == "Long"
    assert p.average_open_price == Decimal("150.00")
    assert p.mark_price == Decimal("155.50")
    assert p.realized_day_gain == Decimal("42.00")
    assert p.realized_day_gain_effect == "Credit"
    assert p.multiplier == 1


async def test_paginates_across_multiple_pages():
    page_1 = {
        "data": {"items": [{"id": "O-1", "account-number": "5WT0001", "received-at": "2026-01-01T00:00:00.000+00:00", "legs": []}]},
        "pagination": {"total-pages": 2, "page-offset": 0},
    }
    page_2 = {
        "data": {"items": [{"id": "O-2", "account-number": "5WT0001", "received-at": "2026-01-02T00:00:00.000+00:00", "legs": []}]},
        "pagination": {"total-pages": 2, "page-offset": 1},
    }
    seen_offsets = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/token":
            return _json_response(200, TOKEN_RESPONSE)
        offset = int(request.url.params["page-offset"])
        seen_offsets.append(offset)
        return _json_response(200, page_1 if offset == 0 else page_2)

    client = _make_client(handler)
    orders = await client.get_order_history("5WT0001", date(2026, 1, 1), date(2026, 2, 1))

    assert seen_offsets == [0, 1]
    assert [o.id for o in orders] == ["O-1", "O-2"]


async def test_retries_once_on_401_then_succeeds():
    auth_calls = 0
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal auth_calls, attempts
        if request.url.path == "/oauth/token":
            auth_calls += 1
            return _json_response(200, {"access_token": f"token-{auth_calls}", "expires_in": 900})
        if request.url.path == "/accounts/5WT0001/positions":
            attempts += 1
            if attempts == 1:
                return httpx.Response(401, json={"error": {"code": "invalid_token", "message": "expired"}})
            assert request.headers["Authorization"] == "Bearer token-2"
            return _json_response(200, POSITIONS_RESPONSE)
        raise AssertionError(f"unexpected request: {request.url}")

    client = _make_client(handler)
    positions = await client.get_positions("5WT0001")

    assert auth_calls == 2  # initial auth + re-auth after the 401
    assert len(positions) == 1


async def test_raises_on_error_envelope():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth/token":
            return _json_response(200, TOKEN_RESPONSE)
        return httpx.Response(403, json={"error": {"code": "not_permitted", "message": "User not permitted access"}})

    client = _make_client(handler)
    with pytest.raises(TastyTradeApiError, match="not_permitted"):
        await client.get_positions("5WT0001")


async def test_raises_a_clear_error_on_unexpected_oauth_response_shape():
    def handler(request: httpx.Request) -> httpx.Response:
        return _json_response(200, {"unexpected": "shape"})

    client = _make_client(handler)
    with pytest.raises(TastyTradeApiError, match="access_token"):
        await client.get_positions("5WT0001")


async def test_raises_on_oauth_http_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="invalid_grant")

    client = _make_client(handler)
    with pytest.raises(TastyTradeApiError, match="400"):
        await client.get_positions("5WT0001")


def test_from_login_config_builds_a_client():
    from tt_ledger.identity.accounts import LoginConfig
    from tt_ledger.identity import AccountMapper

    config = LoginConfig(
        login="trader1", client_id="id", client_secret="secret", refresh_token="refresh",
        default_account=None, account_mapper=AccountMapper({}),
    )
    client = TastyTradeClient.from_login_config(config)
    assert client._client_id == "id"
    assert client._refresh_token == "refresh"
