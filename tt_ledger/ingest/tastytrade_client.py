"""``TastyTradeClient`` — the real REST ``BrokerClient`` (docs/ingestion.md → Pull).

Requires the ``[tastytrade]`` extra (``httpx``). ``httpx`` is imported LAZILY inside
``TastyTradeClient.__init__`` (not at module level), so ``import tt_ledger`` — and even
``import tt_ledger.ingest`` — works without the extra installed; only *constructing* a
``TastyTradeClient`` needs it.

Endpoints, response envelope, error shape, pagination params, and every field name below were
verified against developer.tastytrade.com's own OpenAPI specs and prose docs (see the git history
around the "check against API docs" session) — not guessed. The one still-unverified piece is the
exact ``POST /oauth/token`` request/response field names: the endpoint's existence, the standard
OAuth2 refresh-token grant shape, and the 15-minute token lifetime are all doc-confirmed, but that
specific guide page didn't yield readable content through scraping. ``_authenticate`` raises a
clear, diagnosable error (the raw response body) if the response doesn't have the expected
``access_token`` key, rather than failing with a bare ``KeyError`` deep in some other call.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from .broker import BalanceMessage, BrokerPosition, BrokerTransaction, PlacedFill, PlacedLeg, PlacedOrder

if TYPE_CHECKING:
    import httpx

    from ..identity.accounts import LoginConfig

PRODUCTION_URL = "https://api.tastyworks.com"
SANDBOX_URL = "https://api.cert.tastyworks.com"  # aka "cert" -- use this for initial testing

_DEFAULT_PER_PAGE = 250  # transactions'/positions' own per-page maximum is 2000, per their OpenAPI specs
_ORDERS_PER_PAGE = 200   # orders' per-page maximum is only 200 (confirmed against its OpenAPI spec) --
                          # a shared 250 gets "does not have a valid value" from the real API
_TOKEN_LIFETIME_FALLBACK_SECONDS = 900  # 15 minutes, per docs, if expires_in is ever missing


class TastyTradeApiError(Exception):
    """A non-2xx (or unexpectedly-shaped) response from the TastyTrade API."""


class TastyTradeClient:
    """Implements the ``BrokerClient`` Protocol (``ingest/broker.py``) against the real
    TastyTrade REST API. One instance per login (a login's OAuth client owns the token)."""

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        base_url: str = PRODUCTION_URL,
        user_agent: str = "tt-ledger/0.1.0",
    ) -> None:
        try:
            import httpx
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError(
                "TastyTradeClient needs the [tastytrade] extra: pip install tt-ledger[tastytrade]"
            ) from exc

        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={
                "User-Agent": user_agent,  # required by TastyTrade -- rejected without one
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=30.0,
        )
        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None

    @classmethod
    def from_login_config(
        cls, config: "LoginConfig", *, base_url: str = PRODUCTION_URL, user_agent: str = "tt-ledger/0.1.0",
    ) -> "TastyTradeClient":
        """Build a client from an ``AccountMapper``/``accounts.toml``-loaded ``LoginConfig``."""
        return cls(
            client_id=config.client_id, client_secret=config.client_secret,
            refresh_token=config.refresh_token, base_url=base_url, user_agent=user_agent,
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def access_token(self) -> str:
        """The current (auto-refreshed) OAuth access token. Public so other transports that
        reuse this login's auth -- e.g. ``TastyTradeMessageSource``, the account-streamer,
        which needs a fresh token in its own connect/heartbeat messages too -- don't need their
        own OAuth implementation."""
        return await self._ensure_token()

    # --- auth ------------------------------------------------------------------------------

    async def _ensure_token(self) -> str:
        now = datetime.now(UTC)
        if self._access_token is None or self._token_expires_at is None or now >= self._token_expires_at:
            await self._authenticate()
        return self._access_token

    async def _authenticate(self) -> None:
        # JSON body, not form-encoded (data=) -- TastyTrade's whole API is JSON, including
        # /oauth/token (unlike the RFC 6749 form-encoded convention most OAuth2 servers use).
        # Confirmed against the real API: data= sent Content-Type: application/x-www-form-urlencoded
        # while this client's default headers already claim application/json, and the server
        # rejected the mismatched body with "malformed_json".
        resp = await self._http.post(
            "/oauth/token",
            json={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        if resp.status_code >= 400:
            raise TastyTradeApiError(f"OAuth token exchange failed: {resp.status_code} {resp.text}")
        body = resp.json()
        if "access_token" not in body:
            raise TastyTradeApiError(
                f"Unexpected OAuth token response shape (expected an 'access_token' key): {body!r}"
            )
        self._access_token = body["access_token"]
        expires_in = int(body.get("expires_in", _TOKEN_LIFETIME_FALLBACK_SECONDS))
        # refresh a little early so we never race the real expiry on a slow request
        self._token_expires_at = now_with_margin(expires_in)

    # --- HTTP + pagination -------------------------------------------------------------------

    async def _get(self, path: str, params: dict) -> dict:
        token = await self._ensure_token()
        clean = {k: v for k, v in params.items() if v is not None}
        resp = await self._http.get(path, params=clean, headers={"Authorization": f"Bearer {token}"})
        if resp.status_code == 401:
            # the token may have just expired or been revoked -- re-auth and retry once
            await self._authenticate()
            resp = await self._http.get(path, params=clean, headers={"Authorization": f"Bearer {self._access_token}"})
        _raise_for_error(resp)
        return resp.json()

    async def _get_all_pages(self, path: str, params: dict, *, per_page: int = _DEFAULT_PER_PAGE) -> list[dict]:
        items: list[dict] = []
        page_offset = 0
        while True:
            body = await self._get(path, {**params, "page-offset": page_offset, "per-page": per_page})
            page_items = body.get("data", {}).get("items", [])
            items.extend(page_items)
            total_pages = body.get("pagination", {}).get("total-pages", 1)
            page_offset += 1
            if not page_items or page_offset >= total_pages:
                return items

    # --- BrokerClient Protocol -----------------------------------------------------------------

    async def get_order_history(self, account_number: str, start: date, end: date) -> list[PlacedOrder]:
        items = await self._get_all_pages(
            f"/accounts/{account_number}/orders",
            {"start-date": start.isoformat(), "end-date": end.isoformat()},
            per_page=_ORDERS_PER_PAGE,
        )
        return [order_from_json(item) for item in items]

    async def get_transaction_history(self, account_number: str, start: date, end: date) -> list[BrokerTransaction]:
        items = await self._get_all_pages(
            f"/accounts/{account_number}/transactions",
            {"start-date": start.isoformat(), "end-date": end.isoformat()},
        )
        return [transaction_from_json(item) for item in items]

    async def get_positions(self, account_number: str) -> list[BrokerPosition]:
        body = await self._get(f"/accounts/{account_number}/positions", {})
        items = body.get("data", {}).get("items", [])
        return [position_from_json(item) for item in items]

    async def get_balances(self, account_number: str) -> BalanceMessage:
        """GET /accounts/{account}/balances — the current AccountBalance snapshot (verified
        against the balances-and-positions OpenAPI spec; a ``/balances/{currency}`` variant
        exists too, but the currency-less form is the canonical current snapshot)."""
        body = await self._get(f"/accounts/{account_number}/balances", {})
        return balance_from_json(body.get("data", {}))


def now_with_margin(expires_in_seconds: int) -> datetime:
    """"expires in N seconds" -> the wall-clock instant we should treat the token as expired,
    60s early so a slow in-flight request never races the real expiry."""
    return datetime.now(UTC) + timedelta(seconds=max(expires_in_seconds - 60, 60))


def _raise_for_error(resp: "httpx.Response") -> None:
    if resp.status_code < 400:
        return
    code, message = "unknown", resp.text
    try:
        error = resp.json().get("error", {})
        code = error.get("code", code)
        message = error.get("message", message)
    except ValueError:
        pass  # non-JSON error body -- fall back to the raw text already captured above
    raise TastyTradeApiError(f"TastyTrade API error {resp.status_code} [{code}]: {message}")


def parse_decimal(value) -> Decimal | None:  # noqa: ANN001
    return None if value is None else Decimal(str(value))


def parse_datetime(value: str | int | float | None) -> datetime | None:
    """Confirmed against the real API: most timestamp fields are ISO8601 strings (per the
    OpenAPI spec's own declared type), but ``Order.updated-at`` arrives as a raw epoch-millis
    integer despite being documented ``type: string`` -- handle both rather than trust the spec
    for every field."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    return datetime.fromisoformat(value)


def parse_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def order_from_json(item: dict) -> PlacedOrder:
    """A TastyTrade Order JSON object -> ``PlacedOrder``. Shared by the REST client (order-history,
    where every field below is always present) and ``TastyTradeMessageSource`` (account-streamer
    Order notifications, which per TastyTrade's own docs use "the same json object representations
    as elsewhere in the API" but in practice omit some fields, e.g. ``received-at`` -- hence the
    fallback to "now" rather than a hard KeyError for that one field)."""
    legs = [
        PlacedLeg(
            instrument_type=leg["instrument-type"],
            symbol=leg["symbol"],
            action=leg["action"],
            quantity=parse_decimal(leg.get("quantity")) or Decimal("0"),
            remaining_quantity=parse_decimal(leg.get("remaining-quantity")) or Decimal("0"),
            fills=[
                PlacedFill(
                    fill_id=fill["fill-id"],
                    quantity=parse_decimal(fill.get("quantity")) or Decimal("0"),
                    fill_price=parse_decimal(fill.get("fill-price")) or Decimal("0"),
                    filled_at=parse_datetime(fill["filled-at"]),
                    destination_venue=fill.get("destination-venue"),
                    ext_exec_id=fill.get("ext-exec-id"),
                    ext_group_fill_id=fill.get("ext-group-fill-id"),
                )
                for fill in leg.get("fills", [])
            ],
        )
        for leg in item.get("legs", [])
    ]
    return PlacedOrder(
        id=str(item["id"]),
        account_number=str(item["account-number"]),
        received_at=parse_datetime(item.get("received-at")) or datetime.now(UTC),
        legs=legs,
        underlying_symbol=item.get("underlying-symbol"),
        underlying_instrument_type=item.get("underlying-instrument-type"),
        order_type=item.get("order-type"),
        time_in_force=item.get("time-in-force"),
        gtc_date=item.get("gtc-date"),
        price=parse_decimal(item.get("price")),
        stop_trigger=parse_decimal(item.get("stop-trigger")),
        price_effect=item.get("price-effect"),
        status=item.get("status"),
        reject_reason=item.get("reject-reason"),
        complex_order_id=item.get("complex-order-id"),
        complex_order_tag=item.get("complex-order-tag"),
        updated_at=parse_datetime(item.get("updated-at")),
        terminal_at=parse_datetime(item.get("terminal-at")),
    )


def transaction_from_json(item: dict) -> BrokerTransaction:
    order_id = item.get("order-id")
    return BrokerTransaction(
        id=str(item["id"]),
        account_number=str(item["account-number"]),
        order_id=(str(order_id) if order_id is not None else None),
        underlying_symbol=item.get("underlying-symbol"),
        symbol=item.get("symbol"),
        instrument_type=item.get("instrument-type"),
        transaction_type=item.get("transaction-type"),
        transaction_sub_type=item.get("transaction-sub-type"),
        action=item.get("action"),
        quantity=parse_decimal(item.get("quantity")),
        price=parse_decimal(item.get("price")),
        value=parse_decimal(item.get("value")),
        value_effect=item.get("value-effect"),
        net_value=parse_decimal(item.get("net-value")),
        net_value_effect=item.get("net-value-effect"),
        commission=parse_decimal(item.get("commission")),
        clearing_fees=parse_decimal(item.get("clearing-fees")),
        regulatory_fees=parse_decimal(item.get("regulatory-fees")),
        proprietary_index_option_fees=parse_decimal(item.get("proprietary-index-option-fees")),
        is_estimated_fee=item.get("is-estimated-fee"),
        description=item.get("description"),
        executed_at=parse_datetime(item.get("executed-at")),
        transaction_date=parse_date(item.get("transaction-date")),
    )


def position_from_json(item: dict) -> BrokerPosition:
    multiplier = item.get("multiplier")
    return BrokerPosition(
        account_number=str(item["account-number"]),
        symbol=item["symbol"],
        quantity=parse_decimal(item.get("quantity")) or Decimal("0"),
        quantity_direction=item.get("quantity-direction", ""),
        underlying_symbol=item.get("underlying-symbol"),
        instrument_type=item.get("instrument-type"),
        average_open_price=parse_decimal(item.get("average-open-price")),
        mark_price=parse_decimal(item.get("mark-price")),
        close_price=parse_decimal(item.get("close-price")),
        realized_day_gain=parse_decimal(item.get("realized-day-gain")),
        realized_day_gain_effect=item.get("realized-day-gain-effect"),
        # confirmed against the real API: CurrentPosition.multiplier can arrive as a decimal-looking
        # string ("50.0", not "50") -- int() directly on that raises ValueError, so go through
        # float() first.
        multiplier=int(float(multiplier)) if multiplier is not None else 1,
        expires_at=parse_datetime(item.get("expires-at")),
    )


def balance_from_json(item: dict) -> BalanceMessage:
    """A TastyTrade AccountBalance JSON object -> ``BalanceMessage``. Shared by the REST client
    (``get_balances``) and ``TastyTradeMessageSource`` (account-streamer AccountBalance
    notifications — same object shape). Field names verified against the balances-and-positions
    OpenAPI spec; only the fields the ledger persists are parsed, the full object rides in
    ``raw``."""
    return BalanceMessage(
        account_number=str(item.get("account-number", "")),
        raw=item,
        net_liquidating_value=parse_decimal(item.get("net-liquidating-value")),
        cash_balance=parse_decimal(item.get("cash-balance")),
        equity_buying_power=parse_decimal(item.get("equity-buying-power")),
        derivative_buying_power=parse_decimal(item.get("derivative-buying-power")),
        maintenance_requirement=parse_decimal(item.get("maintenance-requirement")),
        pending_cash=parse_decimal(item.get("pending-cash")),
        day_trading_buying_power=parse_decimal(item.get("day-trading-buying-power")),
        captured_at=parse_datetime(item.get("updated-at")),
    )
