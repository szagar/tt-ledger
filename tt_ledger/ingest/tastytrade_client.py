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

from .broker import BrokerPosition, BrokerTransaction, PlacedFill, PlacedLeg, PlacedOrder

if TYPE_CHECKING:
    import httpx

    from ..identity.accounts import LoginConfig

PRODUCTION_URL = "https://api.tastyworks.com"
SANDBOX_URL = "https://api.cert.tastyworks.com"  # aka "cert" -- use this for initial testing

_DEFAULT_PER_PAGE = 250
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

    # --- auth ------------------------------------------------------------------------------

    async def _ensure_token(self) -> str:
        now = datetime.now(UTC)
        if self._access_token is None or self._token_expires_at is None or now >= self._token_expires_at:
            await self._authenticate()
        return self._access_token

    async def _authenticate(self) -> None:
        resp = await self._http.post(
            "/oauth/token",
            data={
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

    async def _get_all_pages(self, path: str, params: dict) -> list[dict]:
        items: list[dict] = []
        page_offset = 0
        while True:
            body = await self._get(path, {**params, "page-offset": page_offset, "per-page": _DEFAULT_PER_PAGE})
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
        )
        return [_to_placed_order(item) for item in items]

    async def get_transaction_history(self, account_number: str, start: date, end: date) -> list[BrokerTransaction]:
        items = await self._get_all_pages(
            f"/accounts/{account_number}/transactions",
            {"start-date": start.isoformat(), "end-date": end.isoformat()},
        )
        return [_to_broker_transaction(item) for item in items]

    async def get_positions(self, account_number: str) -> list[BrokerPosition]:
        body = await self._get(f"/accounts/{account_number}/positions", {})
        items = body.get("data", {}).get("items", [])
        return [_to_broker_position(item) for item in items]


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


def _dec(value) -> Decimal | None:  # noqa: ANN001
    return None if value is None else Decimal(str(value))


def _dt(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


def _dt_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _to_placed_order(item: dict) -> PlacedOrder:
    legs = [
        PlacedLeg(
            instrument_type=leg["instrument-type"],
            symbol=leg["symbol"],
            action=leg["action"],
            quantity=_dec(leg.get("quantity")) or Decimal("0"),
            remaining_quantity=_dec(leg.get("remaining-quantity")) or Decimal("0"),
            fills=[
                PlacedFill(
                    fill_id=fill["fill-id"],
                    quantity=_dec(fill.get("quantity")) or Decimal("0"),
                    fill_price=_dec(fill.get("fill-price")) or Decimal("0"),
                    filled_at=_dt(fill["filled-at"]),
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
        received_at=_dt(item["received-at"]),
        legs=legs,
        underlying_symbol=item.get("underlying-symbol"),
        underlying_instrument_type=item.get("underlying-instrument-type"),
        order_type=item.get("order-type"),
        time_in_force=item.get("time-in-force"),
        gtc_date=item.get("gtc-date"),
        price=_dec(item.get("price")),
        stop_trigger=_dec(item.get("stop-trigger")),
        price_effect=item.get("price-effect"),
        status=item.get("status"),
        reject_reason=item.get("reject-reason"),
        complex_order_id=item.get("complex-order-id"),
        complex_order_tag=item.get("complex-order-tag"),
        updated_at=_dt(item.get("updated-at")),
        terminal_at=_dt(item.get("terminal-at")),
    )


def _to_broker_transaction(item: dict) -> BrokerTransaction:
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
        quantity=_dec(item.get("quantity")),
        price=_dec(item.get("price")),
        value=_dec(item.get("value")),
        value_effect=item.get("value-effect"),
        net_value=_dec(item.get("net-value")),
        net_value_effect=item.get("net-value-effect"),
        commission=_dec(item.get("commission")),
        clearing_fees=_dec(item.get("clearing-fees")),
        regulatory_fees=_dec(item.get("regulatory-fees")),
        proprietary_index_option_fees=_dec(item.get("proprietary-index-option-fees")),
        is_estimated_fee=item.get("is-estimated-fee"),
        description=item.get("description"),
        executed_at=_dt(item.get("executed-at")),
        transaction_date=_dt_date(item.get("transaction-date")),
    )


def _to_broker_position(item: dict) -> BrokerPosition:
    multiplier = item.get("multiplier")
    return BrokerPosition(
        account_number=str(item["account-number"]),
        symbol=item["symbol"],
        quantity=_dec(item.get("quantity")) or Decimal("0"),
        quantity_direction=item.get("quantity-direction", ""),
        underlying_symbol=item.get("underlying-symbol"),
        instrument_type=item.get("instrument-type"),
        average_open_price=_dec(item.get("average-open-price")),
        mark_price=_dec(item.get("mark-price")),
        close_price=_dec(item.get("close-price")),
        realized_day_gain=_dec(item.get("realized-day-gain")),
        realized_day_gain_effect=item.get("realized-day-gain-effect"),
        multiplier=int(multiplier) if multiplier is not None else 1,
        expires_at=_dt(item.get("expires-at")),
    )
