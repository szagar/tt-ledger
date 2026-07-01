"""``AccountMapper`` / ``LoginConfig`` TOML loaders (docs/identity.md -> Rule 1)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tt_ledger.identity import AccountMapper
from tt_ledger.identity.accounts import LoginConfig

_EXAMPLE = Path(__file__).parent.parent / "config" / "accounts.toml.example"

_TWO_LOGINS = """
[trader1]
default = "ACCT0001"
client_id = "id1"
client_secret = "secret1"
refresh_token = "token1"

  [trader1.accounts]
  ACCT0001 = { nickname = "main" }
  PAPER001 = { nickname = "main_paper", env = "paper" }

[trader2]
default = "ACCT0099"
client_id = "id2"
client_secret = "secret2"
refresh_token = "token2"

  [trader2.accounts]
  ACCT0099 = { nickname = "side" }
"""


def _write(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "accounts.toml"
    p.write_text(content)
    return p


def test_from_toml_loads_the_example_config():
    mapper = AccountMapper.from_toml(_EXAMPLE)

    assert mapper.to_account_number("main") == "ACCT0001"
    assert mapper.to_nickname("ACCT0002") == "ira"
    assert mapper.env_for("main_paper") == "paper"
    assert mapper.env_for("main") == "live"
    assert sorted(mapper.list_nicknames(env="live")) == ["ira", "main"]
    assert mapper.list_nicknames(env="paper") == ["main_paper"]
    assert mapper.default_nickname == "main"


def test_from_toml_rejects_paper_nickname_without_paper(tmp_path):
    bad = _write(
        tmp_path,
        """
        [trader1]
        default = "ACCT0001"
        client_id = "x"
        client_secret = "x"
        refresh_token = "x"

          [trader1.accounts]
          PAPER001 = { nickname = "sandbox", env = "paper" }
        """,
    )
    with pytest.raises(ValueError, match="paper"):
        AccountMapper.from_toml(bad)


def test_from_toml_merges_multiple_logins_into_one_nickname_space(tmp_path):
    cfg = _write(tmp_path, _TWO_LOGINS)
    mapper = AccountMapper.from_toml(cfg)

    assert mapper.to_account_number("main") == "ACCT0001"
    assert mapper.to_account_number("side") == "ACCT0099"
    assert sorted(mapper.list_nicknames()) == ["main", "main_paper", "side"]
    # ambiguous across two logins -> the first login's default wins
    assert mapper.default_nickname == "main"


def test_from_toml_scoped_to_one_login(tmp_path):
    cfg = _write(tmp_path, _TWO_LOGINS)
    mapper = AccountMapper.from_toml(cfg, login="trader2")

    assert mapper.list_nicknames() == ["side"]
    assert mapper.default_nickname == "side"
    with pytest.raises(KeyError):
        mapper.to_account_number("main")


def test_from_toml_unknown_login_raises(tmp_path):
    cfg = _write(tmp_path, _TWO_LOGINS)
    with pytest.raises(KeyError):
        AccountMapper.from_toml(cfg, login="does-not-exist")


def test_from_toml_rejects_duplicate_nickname_across_logins(tmp_path):
    cfg = _write(
        tmp_path,
        """
        [trader1]
        default = "ACCT0001"
        client_id = "x"
        client_secret = "x"
        refresh_token = "x"

          [trader1.accounts]
          ACCT0001 = { nickname = "main" }

        [trader2]
        default = "ACCT0002"
        client_id = "x"
        client_secret = "x"
        refresh_token = "x"

          [trader2.accounts]
          ACCT0002 = { nickname = "main" }
        """,
    )
    with pytest.raises(ValueError, match="duplicate nickname"):
        AccountMapper.from_toml(cfg)


def test_login_config_from_toml_scopes_credentials_and_accounts(tmp_path):
    cfg = _write(tmp_path, _TWO_LOGINS)
    login_cfg = LoginConfig.from_toml("trader1", cfg)

    assert login_cfg.login == "trader1"
    assert login_cfg.client_id == "id1"
    assert login_cfg.client_secret == "secret1"
    assert login_cfg.refresh_token == "token1"
    assert login_cfg.default_account == "ACCT0001"
    assert login_cfg.account_mapper.list_nicknames() == ["main", "main_paper"]


def test_login_config_from_toml_unknown_login_raises(tmp_path):
    cfg = _write(tmp_path, _TWO_LOGINS)
    with pytest.raises(KeyError):
        LoginConfig.from_toml("does-not-exist", cfg)
