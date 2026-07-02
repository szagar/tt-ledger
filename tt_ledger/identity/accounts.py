"""AccountMapper — nickname↔account-number (docs/identity.md → Rule 1).

Internal code uses the nickname only; the raw account_number appears solely at broker calls and
audit columns. A paper account's nickname MUST contain "paper".
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from .._toml import load as _load_toml


def _parse_login_section(login: str, section: dict) -> tuple[dict[str, str], dict[str, str], str | None]:
    """One ``[login]`` table -> (nickname->account_number, nickname->env, default account_number)."""
    n2num: dict[str, str] = {}
    n2env: dict[str, str] = {}
    for account_number, meta in section.get("accounts", {}).items():
        nickname = meta["nickname"]
        env = meta.get("env", "live")
        if env == "paper" and "paper" not in nickname.lower():
            raise ValueError(
                f"{login!r}.accounts[{account_number!r}]: paper account nickname {nickname!r} "
                "must contain 'paper'"
            )
        n2num[nickname] = account_number
        n2env[nickname] = env
    return n2num, n2env, section.get("default")


class AccountMapper:
    """Bidirectional nickname↔account-number map + per-account env."""

    def __init__(
        self,
        nickname_to_number: dict[str, str],
        *,
        default_account: str | None = None,
        login: str | None = None,
        nickname_to_env: dict[str, str] | None = None,
        nickname_to_login: dict[str, str] | None = None,
    ) -> None:
        self._n2num = dict(nickname_to_number)
        self._num2n = {v: k for k, v in nickname_to_number.items()}
        self._default = default_account
        self._login = login
        self._n2env = nickname_to_env or {n: "live" for n in nickname_to_number}
        # falls back to the single `login` ctor arg when unset -- callers that build an
        # AccountMapper directly (not via from_toml) typically pass one login's accounts only.
        self._n2login = nickname_to_login or {n: login for n in nickname_to_number if login is not None}

    # translation
    def to_nickname(self, account_number: str) -> str:
        return self._num2n[account_number]

    def to_account_number(self, nickname: str) -> str:
        return self._n2num[nickname]

    def env_for(self, nickname: str) -> str:
        return self._n2env.get(nickname, "live")

    def login_for(self, nickname: str) -> str | None:
        """Which ``accounts.toml`` ``[login]`` section owns ``nickname`` -- the credential set
        needed to build a real broker client for it (``LoginConfig.from_toml(login, path)``).
        Unambiguous by construction: ``from_toml`` rejects a nickname reused across logins."""
        return self._n2login.get(nickname)

    # listing
    def list_nicknames(self, env: str | None = None) -> list[str]:
        return [n for n in self._n2num if env is None or self._n2env.get(n) == env]

    @property
    def default_nickname(self) -> str | None:
        return self._num2n.get(self._default) if self._default else None

    # loaders
    @classmethod
    def from_toml(cls, config_path: str | os.PathLike[str], login: str | None = None) -> "AccountMapper":
        """Load config/accounts.toml.

        ``login`` scopes the mapper to one ``[login]`` table's ``.accounts``; omitted (the common
        case), every login table in the file is merged into one nickname space — internal code
        never needs to know which broker login an account came from.
        """
        data = _load_toml(config_path)
        if login is not None and login not in data:
            raise KeyError(f"login {login!r} not found in {config_path}")

        sections = {login: data[login]} if login is not None else {
            name: section for name, section in data.items() if isinstance(section, dict) and "accounts" in section
        }

        n2num: dict[str, str] = {}
        n2env: dict[str, str] = {}
        n2login: dict[str, str] = {}
        default_account: str | None = None
        for name, section in sections.items():
            s_n2num, s_n2env, s_default = _parse_login_section(name, section)
            overlap = n2num.keys() & s_n2num.keys()
            if overlap:
                raise ValueError(f"duplicate nickname(s) across logins in {config_path}: {sorted(overlap)}")
            n2num.update(s_n2num)
            n2env.update(s_n2env)
            n2login.update({nickname: name for nickname in s_n2num})
            if default_account is None:
                default_account = s_default

        resolved_login = login if login is not None else (next(iter(sections)) if len(sections) == 1 else None)
        return cls(
            n2num, default_account=default_account, login=resolved_login,
            nickname_to_env=n2env, nickname_to_login=n2login,
        )


@dataclass
class LoginConfig:
    """One login section of accounts.toml (OAuth creds + its account mapper)."""

    login: str
    client_id: str
    client_secret: str
    refresh_token: str
    default_account: str | None
    account_mapper: AccountMapper

    @classmethod
    def from_toml(cls, login: str, config_path: str | os.PathLike[str]) -> "LoginConfig":
        data = _load_toml(config_path)
        if login not in data:
            raise KeyError(f"login {login!r} not found in {config_path}")
        section = data[login]
        return cls(
            login=login,
            client_id=section["client_id"],
            client_secret=section["client_secret"],
            refresh_token=section["refresh_token"],
            default_account=section.get("default"),
            account_mapper=AccountMapper.from_toml(config_path, login=login),
        )
