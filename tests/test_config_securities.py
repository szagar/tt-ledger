"""``load_securities_toml`` (docs/identity.md -> Rule 2)."""

from __future__ import annotations

from pathlib import Path

from tt_ledger.config import SecurityRules, load_securities_toml

_EXAMPLE = Path(__file__).parent.parent / "config" / "securities.toml.example"


def test_load_securities_toml_parses_the_example_config():
    rules = load_securities_toml(_EXAMPLE)

    assert isinstance(rules, SecurityRules)
    assert rules.index_option_roots == {"SPX": ["SPX", "SPXW"], "NDX": ["NDX", "NDXP"]}
    assert rules.futures == {"ES": {"exchange": "CME", "multiplier": 50}}
    assert rules.universes == {}


def test_load_securities_toml_missing_sections_default_empty(tmp_path):
    p = tmp_path / "securities.toml"
    p.write_text("[futures]\nES = { exchange = \"CME\", multiplier = 50 }\n")

    rules = load_securities_toml(p)
    assert rules.index_option_roots == {}
    assert rules.futures == {"ES": {"exchange": "CME", "multiplier": 50}}
    assert rules.universes == {}
