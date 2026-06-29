"""AccountMapper — nickname↔account-number (docs/identity.md → Rule 1).

PORT from the host platform's ``shared/accounts/`` (mapper.py + config.py) or reimplement
(~200 lines). Internal code uses the nickname only; the raw account_number appears solely at
broker calls and audit columns. A paper account's nickname MUST contain "paper".
"""

from __future__ import annotations

from dataclasses import dataclass


class AccountMapper:
    """Bidirectional nickname↔account-number map + per-account env."""

    def __init__(
        self,
        nickname_to_number: dict[str, str],
        *,
        default_account: str | None = None,
        login: str | None = None,
        nickname_to_env: dict[str, str] | None = None,
    ) -> None:
        self._n2num = dict(nickname_to_number)
        self._num2n = {v: k for k, v in nickname_to_number.items()}
        self._default = default_account
        self._login = login
        self._n2env = nickname_to_env or {n: "live" for n in nickname_to_number}

    # translation
    def to_nickname(self, account_number: str) -> str:
        return self._num2n[account_number]

    def to_account_number(self, nickname: str) -> str:
        return self._n2num[nickname]

    def env_for(self, nickname: str) -> str:
        return self._n2env.get(nickname, "live")

    # listing
    def list_nicknames(self, env: str | None = None) -> list[str]:
        return [n for n in self._n2num if env is None or self._n2env.get(n) == env]

    @property
    def default_nickname(self) -> str | None:
        return self._num2n.get(self._default) if self._default else None

    # loaders
    @classmethod
    def from_toml(cls, config_path: str, login: str | None = None) -> "AccountMapper":
        """Load config/accounts.toml. Enforce: paper nicknames contain 'paper'. TODO: implement."""
        raise NotImplementedError("AccountMapper.from_toml — see docs/identity.md")


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
    def from_toml(cls, login: str, config_path: str) -> "LoginConfig":
        raise NotImplementedError("LoginConfig.from_toml — see docs/identity.md")
