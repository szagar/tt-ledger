"""``tt-ledger`` CLI (docs/api.md → CLI). Requires the ``[cli]`` extra.

``typer``/``rich`` are imported lazily inside ``build_app`` so ``import tt_ledger`` works
without them. Commands: sync, listen, trades list/show/remap/regroup/dismiss, reconcile,
positions, closed-positions, rebuild-positions — all thin wrappers over ``LedgerClient``,
matching the API layer's own "thin wrapper" role.

Two deviations from the docs' illustrative examples, both forced by the standalone schema:
  * ``trades remap --strategy`` takes an **int** (``strategy_id``, a soft ref — no strategy-name
    table exists in this standalone package), not the symbolic name (``spx_ic``) docs show.
  * every remap-family command needs ``--reviewed-by`` (defaults to the OS user) since
    ``TradeGroupRow.reviewed_by`` has no default in the data model.
"""

import getpass
import os
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .sdk import LedgerClient

_DEFAULT_URL = os.environ.get("TT_LEDGER_DATABASE_URL", "sqlite+aiosqlite:///ledger.db")
_DEFAULT_ACCOUNTS = os.environ.get("TT_LEDGER_ACCOUNTS", "config/accounts.toml")


def build_app():
    """Construct the typer app. ``typer``/``rich`` imported lazily."""
    try:
        import typer
        from rich.console import Console
        from rich.table import Table
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("The CLI needs the [cli] extra: pip install tt-ledger[cli]") from exc

    import asyncio

    from .identity import AccountMapper, LoginConfig
    from .ingest.tastytrade_client import PRODUCTION_URL, SANDBOX_URL, TastyTradeClient
    from .sdk import LedgerClient

    # a fixed width (rather than auto-detected) keeps tables from wrapping IDs mid-string when
    # output isn't a real TTY (piped, redirected, or under a test runner) -- narrow-terminal auto
    # -detection defaults to 80 columns, which splits a 36-char group_id across lines.
    console = Console(width=200)

    def _open_client(ctx: "typer.Context") -> "LedgerClient":
        accounts = AccountMapper.from_toml(ctx.obj["accounts_path"])
        return LedgerClient.open(ctx.obj["url"], accounts=accounts)

    def _open_client_with_broker(ctx: "typer.Context", account: str):
        """Like ``_open_client``, but also wires a real ``TastyTradeClient`` -- needed by ``sync``
        and ``listen``, the only commands that talk to the broker directly. ``TastyTradeClient``
        needs the ``[tastytrade]`` extra; a paper account's ``env`` routes it to the sandbox/cert
        API (the same environment TastyTrade itself uses for paper trading), not a separate
        global setting. Returns ``(LedgerClient, TastyTradeClient)`` -- callers that also need the
        raw broker object (``listen``, to build a ``TastyTradeMessageSource``) don't have to reach
        into ``LedgerClient``'s private state for it."""
        accounts_path = ctx.obj["accounts_path"]
        accounts = AccountMapper.from_toml(accounts_path)
        login = accounts.login_for(account)
        if login is None:
            raise RuntimeError(f"no login section in {accounts_path} owns account {account!r}")
        login_config = LoginConfig.from_toml(login, accounts_path)
        base_url = SANDBOX_URL if accounts.env_for(account) == "paper" else PRODUCTION_URL
        broker = TastyTradeClient.from_login_config(login_config, base_url=base_url)
        client = LedgerClient.open(ctx.obj["url"], accounts=accounts, client=broker)
        return client, broker

    def _open_client_for_sync(ctx: "typer.Context", account: str) -> "LedgerClient":
        client, _broker = _open_client_with_broker(ctx, account)
        return client

    def _run(coro):
        try:
            return asyncio.run(coro)
        except (FileNotFoundError, ValueError, RuntimeError, KeyError) as exc:
            console.print(f"[red]Error:[/red] {exc}")
            raise typer.Exit(code=1) from None
        except KeyboardInterrupt:
            console.print("\n[yellow]Stopped.[/yellow]")
            raise typer.Exit(code=0) from None

    def _money(value) -> str:  # noqa: ANN001
        return "-" if value is None else str(value)

    def _parse_since(value: str | None) -> date | None:
        # typer (as of this version) doesn't support datetime.date as a param type directly --
        # take a plain ISO string and parse it here instead.
        return date.fromisoformat(value) if value else None

    def _print_sync_result(result) -> None:  # noqa: ANN001
        table = Table(title="Sync result")
        table.add_column("Orders", justify="right")
        table.add_column("Transactions", justify="right")
        table.add_column("Positions", justify="right")
        table.add_column("Trade groups", justify="right")
        table.add_row(str(result.orders), str(result.transactions), str(result.positions), str(result.trade_groups))
        console.print(table)
        if result.errors:
            console.print("[red]Errors:[/red]")
            for err in result.errors:
                console.print(f"  - {err}")

    def _print_trades_table(trades) -> None:  # noqa: ANN001
        table = Table(title="Trades")
        for col in ("Group", "Account", "Origin", "Review", "Status", "Strategy", "Underlying", "Legs", "Premium"):
            table.add_column(
                col, justify="right" if col in ("Legs", "Premium") else "left", no_wrap=(col == "Group"),
            )
        for t in trades:
            table.add_row(
                t.group_id, t.account, str(t.origin), str(t.review_status), t.status,
                t.strategy_type or "-", t.underlying or "-", str(t.leg_count), _money(t.total_premium),
            )
        console.print(table)
        if not trades:
            console.print("[dim]No trades matched.[/dim]")

    def _print_orders_table(orders) -> None:  # noqa: ANN001
        table = Table(title="Orders")
        for col in ("tt_order_id", "Account", "Origin", "Status", "Underlying", "Security", "Price"):
            table.add_column(col, no_wrap=(col == "tt_order_id"))
        for o in orders:
            table.add_row(
                o.tt_order_id or "-", o.account, str(o.origin), o.oms_status or "-",
                o.underlying or "-", o.security_id or "-", _money(o.price),
            )
        console.print(table)
        if not orders:
            console.print("[dim]No orders matched.[/dim]")

    def _print_activity_table(activity) -> None:  # noqa: ANN001
        table = Table(title="Account activity")
        for col in ("tt_transaction_id", "Type", "Security", "Qty", "Net value", "Origin", "Review"):
            table.add_column(col, no_wrap=(col == "tt_transaction_id"))
        for a in activity:
            table.add_row(
                a.tt_transaction_id, a.transaction_type or "-", a.security_id or "-",
                _money(a.quantity), _money(a.net_value),
                str(a.origin) if a.origin else "-", str(a.review_status) if a.review_status else "-",
            )
        console.print(table)
        if not activity:
            console.print("[dim]No activity matched.[/dim]")

    def _print_positions_table(positions) -> None:  # noqa: ANN001
        table = Table(title="Positions")
        for col in ("Security", "Qty", "Direction", "Avg open", "Mark", "Unrealized P&L"):
            table.add_column(col, justify="right" if col not in ("Security", "Direction") else "left")
        for p in positions:
            table.add_row(
                p.security_id, _money(p.quantity), p.quantity_direction,
                _money(p.average_open_price), _money(p.mark_price), _money(p.unrealized_pnl),
            )
        console.print(table)
        if not positions:
            console.print("[dim]No positions matched.[/dim]")

    def _print_closed_positions_table(closed) -> None:  # noqa: ANN001
        table = Table(title="Closed positions")
        for col in ("Security", "Qty", "Direction", "Avg open", "Avg close", "Realized P&L", "Closed at", "Held (days)"):
            table.add_column(col, justify="right" if col not in ("Security", "Direction", "Closed at") else "left")
        for c in closed:
            table.add_row(
                c.security_id, _money(c.quantity), c.quantity_direction, _money(c.average_open_price),
                _money(c.average_close_price), _money(c.realized_pnl),
                c.closed_at.isoformat() if c.closed_at else "-",
                str(c.holding_period_days) if c.holding_period_days is not None else "-",
            )
        console.print(table)
        if not closed:
            console.print("[dim]No closed positions matched.[/dim]")

    app = typer.Typer(name="tt-ledger", help="Broker order/trade/transaction ledger.", no_args_is_help=True)
    trades_app = typer.Typer(help="Inspect, reconcile, and remap trades.")
    app.add_typer(trades_app, name="trades")

    @app.callback()
    def main_callback(
        ctx: typer.Context,
        url: str = typer.Option(_DEFAULT_URL, "--url", help="Store connection URL (env TT_LEDGER_DATABASE_URL)."),
        accounts_path: str = typer.Option(_DEFAULT_ACCOUNTS, "--accounts", help="Path to accounts.toml (env TT_LEDGER_ACCOUNTS)."),
    ) -> None:
        ctx.obj = {"url": url, "accounts_path": accounts_path}

    @app.command()
    def sync(
        ctx: typer.Context,
        account: str = typer.Option(..., "--account", help="Account nickname."),
        since: str | None = typer.Option(None, "--since", help="ISO date, e.g. 2026-01-01."),
    ) -> None:
        """Pull (orders + transactions + positions) then reconcile."""

        async def _do():
            client = _open_client_for_sync(ctx, account)
            try:
                result = await client.sync(account, since=_parse_since(since))
                _print_sync_result(result)
            finally:
                await client.close()

        _run(_do())

    @app.command()
    def listen(
        ctx: typer.Context,
        account: str = typer.Option(..., "--account", help="Account nickname to stream."),
    ) -> None:
        """Run the real account-streamer live (orders/positions/balances) until interrupted."""
        from .ingest.tastytrade_stream import TastyTradeMessageSource

        async def _do():
            client, broker = _open_client_with_broker(ctx, account)
            try:
                accounts = AccountMapper.from_toml(ctx.obj["accounts_path"])
                source = TastyTradeMessageSource.from_client(broker, accounts.to_account_number(account))
                consumer = client.stream_consumer(source)
                console.print(f"Listening for {account!r} -- Ctrl+C to stop.")
                await consumer.run()
            finally:
                await client.close()

        _run(_do())

    @app.command()
    def reconcile(
        ctx: typer.Context,
        account: str | None = typer.Option(None, "--account", help="Omit to reconcile every account with activity."),
        since: str | None = typer.Option(None, "--since", help="ISO date, e.g. 2026-01-01."),
        dry_run: bool = typer.Option(False, "--dry-run", help="Preview without persisting new trade_groups."),
    ) -> None:
        """Re-group already-synced activity into trade_groups (without a broker pull)."""

        async def _do():
            client = _open_client(ctx)
            try:
                result = await client.reconcile(account, since=_parse_since(since), dry_run=dry_run)
                console.print(f"trade_groups: {result.trade_groups}")
                if result.errors:
                    console.print("[red]Errors:[/red]")
                    for err in result.errors:
                        console.print(f"  - {err}")
            finally:
                await client.close()

        _run(_do())

    @app.command()
    def positions(
        ctx: typer.Context,
        account: str = typer.Option(..., "--account", help="Account nickname."),
        show_all: bool = typer.Option(False, "--all", help="Include flat (quantity 0) rows for securities once held."),
    ) -> None:
        """List positions (open only, unless --all)."""

        async def _do():
            client = _open_client(ctx)
            try:
                _print_positions_table(await client.positions(account, open_only=not show_all))
            finally:
                await client.close()

        _run(_do())

    @app.command("closed-positions")
    def closed_positions(
        ctx: typer.Context,
        account: str = typer.Option(..., "--account", help="Account nickname."),
        security_id: str | None = typer.Option(None, "--security-id"),
    ) -> None:
        """List completed open->close position lifecycles."""

        async def _do():
            client = _open_client(ctx)
            try:
                _print_closed_positions_table(await client.closed_positions(account, security_id))
            finally:
                await client.close()

        _run(_do())

    @app.command("rebuild-positions")
    def rebuild_positions(
        ctx: typer.Context,
        account: str | None = typer.Option(None, "--account", help="Omit to rebuild every account with activity."),
    ) -> None:
        """Rebuild positions/closed-positions from transaction history (docs/ingestion.md → Replay)."""

        async def _do():
            client = _open_client(ctx)
            try:
                result = await client.rebuild_positions(account)
                console.print(f"positions rebuilt: {result.positions}")
                if result.errors:
                    console.print("[red]Errors:[/red]")
                    for err in result.errors:
                        console.print(f"  - {err}")
            finally:
                await client.close()

        _run(_do())

    @trades_app.command("list")
    def trades_list(
        ctx: typer.Context,
        needs_review: bool = typer.Option(False, "--needs-review"),
        origin: str | None = typer.Option(None, "--origin"),
        account: str | None = typer.Option(None, "--account"),
        underlying: str | None = typer.Option(None, "--underlying"),
    ) -> None:
        """List trades."""

        async def _do():
            client = _open_client(ctx)
            try:
                filters = {}
                if origin is not None:
                    filters["origin"] = origin
                if account is not None:
                    filters["account"] = account
                if underlying is not None:
                    filters["underlying"] = underlying
                if needs_review:
                    filters["review_status"] = "needs_review"
                _print_trades_table(await client.trades(**filters))
            finally:
                await client.close()

        _run(_do())

    @trades_app.command("show")
    def trades_show(ctx: typer.Context, group_id: str) -> None:
        """Show a trade's detail: the trade plus its orders and transactions."""

        async def _do():
            client = _open_client(ctx)
            try:
                trade = await client.trade(group_id)
                if trade is None:
                    console.print(f"[red]Error:[/red] trade group {group_id!r} not found")
                    raise typer.Exit(code=1)
                _print_trades_table([trade])
                orders, transactions = await client.trade_detail(group_id)
                _print_orders_table(orders)
                _print_activity_table(transactions)
            finally:
                await client.close()

        _run(_do())

    @trades_app.command("remap")
    def trades_remap(
        ctx: typer.Context,
        group_id: str,
        strategy: int | None = typer.Option(None, "--strategy", help="strategy_id (int) -- a soft ref, no name lookup in this standalone package."),
        bot: str | None = typer.Option(None, "--bot"),
        signal: str | None = typer.Option(None, "--signal"),
        strategy_type: str | None = typer.Option(None, "--strategy-type"),
        reviewed_by: str = typer.Option(getpass.getuser(), "--reviewed-by"),
    ) -> None:
        """Set attribution on a trade; flips it to manually_attributed + CONFIRMED."""

        async def _do():
            client = _open_client(ctx)
            try:
                trade = await client.remap_trade(
                    group_id, strategy=strategy, bot=bot, signal=signal,
                    strategy_type=strategy_type, reviewed_by=reviewed_by,
                )
                _print_trades_table([trade])
            finally:
                await client.close()

        _run(_do())

    @trades_app.command("regroup")
    def trades_regroup(
        ctx: typer.Context,
        group_id: str,  # noqa: ARG001 - contextual (URL/CLI nesting); regroup itself operates on --move's txn ids
        move: list[int] = typer.Option(..., "--move", help="Transaction id(s) to move (repeat the flag for multiple)."),
        to: str | None = typer.Option(None, "--to", help="Target group id. Omit and pass --new to split into a new group."),
        new: bool = typer.Option(False, "--new"),
        reviewed_by: str = typer.Option(getpass.getuser(), "--reviewed-by"),
    ) -> None:
        """Move transactions to a different (or brand-new) trade_group; recomputes both."""
        if to and new:
            console.print("[red]Error:[/red] specify either --to or --new, not both")
            raise typer.Exit(code=1)
        if not to and not new:
            console.print("[red]Error:[/red] specify --to <group_id> or --new")
            raise typer.Exit(code=1)

        async def _do():
            client = _open_client(ctx)
            try:
                _print_trades_table(await client.regroup(move, target=to, reviewed_by=reviewed_by))
            finally:
                await client.close()

        _run(_do())

    @trades_app.command("dismiss")
    def trades_dismiss(
        ctx: typer.Context,
        group_id: str,
        reviewed_by: str = typer.Option(getpass.getuser(), "--reviewed-by"),
    ) -> None:
        """review_status=IGNORED (transfers / non-trades)."""

        async def _do():
            client = _open_client(ctx)
            try:
                _print_trades_table([await client.dismiss_trade(group_id, reviewed_by=reviewed_by)])
            finally:
                await client.close()

        _run(_do())

    return app


def main() -> None:
    """Console-script entry point (pyproject [project.scripts] tt-ledger)."""
    build_app()()


if __name__ == "__main__":
    main()
