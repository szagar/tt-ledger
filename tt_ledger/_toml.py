"""Tiny shared TOML-load shim: stdlib ``tomllib`` (3.11+), ``tomli`` fallback below it."""

from __future__ import annotations

import os

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover (python < 3.11)
    import tomli as tomllib


def load(path: str | os.PathLike[str]) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)
