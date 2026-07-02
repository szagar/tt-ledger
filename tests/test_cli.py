"""``tt-ledger`` CLI (docs/api.md -> CLI), exercised via typer's CliRunner.

Test bodies are plain sync functions (not ``async def``) -- CLI commands manage their own
event loop via ``asyncio.run`` internally, mirroring how a real terminal invocation works, and
mixing that with pytest-asyncio's own loop would just create nested-loop conflicts for no benefit.
Data is seeded by calling the SDK directly (also via a sync ``asyncio.run`` wrapper) against the
same SQLite file the CLI subprocess-equivalent will then open.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, UTC
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tt_ledger.cli import build_app
from tt_ledger.identity import AccountMapper, PassthroughResolver
from tt_ledger.ingest.mock_broker import MockTastyTradeClient
from tt_ledger.sdk import LedgerClient
from tt_ledger.store.sql import SqlLedgerStore

runner = CliRunner()

_ACCOUNTS_TOML = """
[trader1]
default = "ACCT0001"
client_id = "x"
client_secret = "x"
refresh_token = "x"

  [trader1.accounts]
  ACCT0001 = { nickname = "main" }
"""


@pytest.fixture
def accounts_path(tmp_path: Path) -> Path:
    p = tmp_path / "accounts.toml"
    p.write_text(_ACCOUNTS_TOML)
    return p


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / 'ledger.db'}"

    async def _create_tables() -> None:
        store = SqlLedgerStore(url)
        await store.create_all()
        await store.dispose()

    asyncio.run(_create_tables())
    return url


def _seed(db_url: str, accounts_path: Path) -> None:
    """Pull-only (no reconcile) -- leaves real work for the CLI's own ``reconcile`` command,
    unlike ``LedgerClient.sync()`` which bundles both."""

    async def _run() -> None:
        from tt_ledger.ingest.pull import sync_orders, sync_transactions

        store = SqlLedgerStore(db_url)
        accounts = AccountMapper.from_toml(accounts_path)
        resolver = PassthroughResolver()
        broker = MockTastyTradeClient()
        broker.fill(
            account_number="ACCT0001", order_id="O-1", symbol="AAPL", instrument_type="Equity",
            action="Buy to Open", quantity=Decimal("10"), fill_price=Decimal("150"),
            filled_at=datetime(2026, 1, 5, tzinfo=UTC), status="Filled",
        )
        await sync_orders(store, "main", client=broker, accounts=accounts, resolver=resolver)
        await sync_transactions(store, "main", client=broker, accounts=accounts, resolver=resolver)
        await store.dispose()

    asyncio.run(_run())


@pytest.fixture
def seeded(db_url: str, accounts_path: Path) -> tuple[str, Path]:
    _seed(db_url, accounts_path)
    return db_url, accounts_path


def _invoke(db_url: str, accounts_path: Path, *args: str):
    app = build_app()
    return runner.invoke(app, ["--url", db_url, "--accounts", str(accounts_path), *args])


def test_help():
    result = runner.invoke(build_app(), ["--help"])
    assert result.exit_code == 0
    assert "sync" in result.output
    assert "listen" in result.output
    assert "trades" in result.output
    assert "reconcile" in result.output


def test_trades_list_empty(db_url, accounts_path):
    result = _invoke(db_url, accounts_path, "trades", "list")
    assert result.exit_code == 0
    assert "No trades matched" in result.output


def test_sync_unknown_account_errors_clearly(db_url, accounts_path):
    """``sync`` needs a real broker client wired from accounts.toml's [login] credentials
    (docs/identity.md) -- an account nickname no [login] section owns can't build one."""
    result = _invoke(db_url, accounts_path, "sync", "--account", "does-not-exist")
    assert result.exit_code == 1
    assert "no login section" in result.output


def test_sync_missing_accounts_toml_errors_clearly(db_url, tmp_path):
    result = _invoke(db_url, tmp_path / "does-not-exist.toml", "sync", "--account", "main")
    assert result.exit_code == 1
    assert "Error" in result.output


def test_error_messages_render_literal_brackets_safely(db_url, tmp_path):
    """Regression test: error text (broker/API error bodies, or -- as exercised here -- a file
    path) can contain literal '[...]'. Without escaping, Rich interprets that as markup and
    silently swallows it (this masked a real TastyTrade error code while debugging a live sync)."""
    weird_path = tmp_path / "[weird].toml"
    result = _invoke(db_url, weird_path, "sync", "--account", "main")
    assert result.exit_code == 1
    assert "[weird].toml" in result.output


def test_listen_unknown_account_errors_clearly(db_url, accounts_path):
    """``listen`` reuses the same broker-wiring as ``sync`` -- same login-resolution error."""
    result = _invoke(db_url, accounts_path, "listen", "--account", "does-not-exist")
    assert result.exit_code == 1
    assert "no login section" in result.output


def test_reconcile_groups_seeded_activity(seeded):
    db_url, accounts_path = seeded
    result = _invoke(db_url, accounts_path, "reconcile")
    assert result.exit_code == 0
    assert "trade_groups: 1" in result.output

    result2 = _invoke(db_url, accounts_path, "reconcile")
    assert "trade_groups: 0" in result2.output  # idempotent


def test_trades_list_shows_the_seeded_trade(seeded):
    db_url, accounts_path = seeded
    _invoke(db_url, accounts_path, "reconcile")

    result = _invoke(db_url, accounts_path, "trades", "list", "--account", "main")
    assert result.exit_code == 0
    assert "single" in result.output


def test_trades_list_needs_review_filter(seeded):
    db_url, accounts_path = seeded
    _invoke(db_url, accounts_path, "reconcile")

    result = _invoke(db_url, accounts_path, "trades", "list", "--needs-review")
    assert result.exit_code == 0
    assert "No trades matched" not in result.output


def _group_id(db_url: str, accounts_path: Path) -> str:
    async def _get() -> str:
        store = SqlLedgerStore(db_url)
        accounts = AccountMapper.from_toml(accounts_path)
        client = LedgerClient(store, accounts=accounts, resolver=PassthroughResolver())
        trades = await client.trades(account="main")
        await client.close()
        return trades[0].group_id

    return asyncio.run(_get())


def test_trades_show(seeded):
    db_url, accounts_path = seeded
    _invoke(db_url, accounts_path, "reconcile")
    group_id = _group_id(db_url, accounts_path)

    result = _invoke(db_url, accounts_path, "trades", "show", group_id)
    assert result.exit_code == 0
    assert group_id in result.output
    assert "O-1" in result.output  # the order shows up in the detail view


def test_trades_show_unknown_group(seeded):
    db_url, accounts_path = seeded
    result = _invoke(db_url, accounts_path, "trades", "show", "does-not-exist")
    assert result.exit_code == 1
    assert "not found" in result.output


def test_trades_remap(seeded):
    db_url, accounts_path = seeded
    _invoke(db_url, accounts_path, "reconcile")
    group_id = _group_id(db_url, accounts_path)

    result = _invoke(
        db_url, accounts_path, "trades", "remap", group_id,
        "--strategy", "42", "--bot", "my-bot", "--reviewed-by", "alice",
    )
    assert result.exit_code == 0
    assert "confirmed" in result.output.lower()


def test_trades_dismiss(seeded):
    db_url, accounts_path = seeded
    _invoke(db_url, accounts_path, "reconcile")
    group_id = _group_id(db_url, accounts_path)

    result = _invoke(db_url, accounts_path, "trades", "dismiss", group_id, "--reviewed-by", "alice")
    assert result.exit_code == 0
    assert "ignored" in result.output.lower()


def test_trades_regroup_requires_to_or_new(seeded):
    db_url, accounts_path = seeded
    _invoke(db_url, accounts_path, "reconcile")
    group_id = _group_id(db_url, accounts_path)

    result = _invoke(db_url, accounts_path, "trades", "regroup", group_id, "--move", "1")
    assert result.exit_code == 1
    assert "--to" in result.output or "--new" in result.output


def test_positions_empty(seeded):
    db_url, accounts_path = seeded
    result = _invoke(db_url, accounts_path, "positions", "--account", "main")
    assert result.exit_code == 0
    assert "No positions matched" in result.output


def test_rebuild_positions_then_list(seeded):
    db_url, accounts_path = seeded
    result = _invoke(db_url, accounts_path, "rebuild-positions", "--account", "main")
    assert result.exit_code == 0
    assert "positions rebuilt: 1" in result.output

    result = _invoke(db_url, accounts_path, "positions", "--account", "main")
    assert result.exit_code == 0
    assert "AAPL" in result.output


def test_closed_positions_empty(seeded):
    db_url, accounts_path = seeded
    _invoke(db_url, accounts_path, "rebuild-positions", "--account", "main")

    result = _invoke(db_url, accounts_path, "closed-positions", "--account", "main")
    assert result.exit_code == 0
    assert "No closed positions matched" in result.output


def test_trades_regroup_rejects_both_to_and_new(seeded):
    db_url, accounts_path = seeded
    _invoke(db_url, accounts_path, "reconcile")
    group_id = _group_id(db_url, accounts_path)

    result = _invoke(
        db_url, accounts_path, "trades", "regroup", group_id,
        "--move", "1", "--to", "some-group", "--new",
    )
    assert result.exit_code == 1
    assert "not both" in result.output
