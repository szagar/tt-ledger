"""Shared test fixtures.

Per docs/implementation-notes.md, repository/ingest tests must run against BOTH backends.
``store_url`` parametrizes over SQLite (always) and Postgres (when TT_LEDGER_TEST_PG is set).
"""

from __future__ import annotations

import os

import pytest

_URLS = ["sqlite+aiosqlite:///:memory:"]
if os.getenv("TT_LEDGER_TEST_PG"):
    _URLS.append(os.environ["TT_LEDGER_TEST_PG"])  # e.g. postgresql+asyncpg://user@localhost/ledger_test


@pytest.fixture(params=_URLS)
def store_url(request) -> str:  # noqa: ANN001
    return request.param
