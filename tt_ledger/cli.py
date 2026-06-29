"""``tt-ledger`` CLI (docs/api.md → CLI). Requires the ``[cli]`` extra.

``typer`` is imported lazily inside ``main`` so ``import tt_ledger`` works without it.
Commands (to implement): sync, trades list/show/remap/regroup/dismiss, reconcile.
"""

from __future__ import annotations


def build_app():
    """Construct the typer app. ``typer`` imported lazily."""
    try:
        import typer
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("The CLI needs the [cli] extra: pip install tt-ledger[cli]") from exc

    app = typer.Typer(name="tt-ledger", help="Broker order/trade/transaction ledger.", no_args_is_help=True)
    trades = typer.Typer(help="Inspect, reconcile, and remap trades.")
    app.add_typer(trades, name="trades")

    # TODO: implement commands:
    #   tt-ledger sync --account --since
    #   tt-ledger trades list [--needs-review] [--origin]
    #   tt-ledger trades show <group_id>
    #   tt-ledger trades remap <group_id> --strategy …
    #   tt-ledger trades regroup <group_id> --move <txn_ids> [--to | --new]
    #   tt-ledger trades dismiss <group_id>
    #   tt-ledger reconcile [--account] [--since]
    return app


def main() -> None:
    """Console-script entry point (pyproject [project.scripts] tt-ledger)."""
    build_app()()


if __name__ == "__main__":
    main()
