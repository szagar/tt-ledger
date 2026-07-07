"""``RedisStreamMessageSource`` — the durable ``zts:events`` stream variant.

The transport is faked with a minimal in-memory consumer-group double (no [redis] extra
needed), mirroring ``test_ingest_redis_source.py``. What's pinned here beyond mapping:
the ACK protocol (ack only after the caller processed the previous message), PEL replay
after a consumer crash, and the ack-and-skip filtering rules.
"""

from __future__ import annotations

import json

import pytest

from tt_ledger.enums import Ingest, Origin
from tt_ledger.identity import AccountMapper
from tt_ledger.ingest.push import MessageSource, StreamConsumer
from tt_ledger.ingest.redis_stream_source import RedisStreamMessageSource
from tt_ledger.rows import OrderRow
from tt_ledger.store.memory import InMemoryStore


@pytest.fixture
def accounts() -> AccountMapper:
    return AccountMapper({"main": "ACCT1", "roth": "ACCT2"})


def _envelope(
    msg_type: str,
    nickname: str,
    data: dict,
    *,
    timestamp: str = "2026-01-05T15:30:00+00:00",
) -> dict:
    return {
        "type": msg_type,
        "account_number": nickname,
        "source": "streamer",
        "timestamp": timestamp,
        "data": data,
    }


def _entry(msg: dict, *, event_type: str | None = None) -> dict:
    """The host's zts:events entry fields (payload identical to the pub/sub message)."""
    return {
        "event_type": event_type
        if event_type is not None
        else f"account.{msg['type']}",
        "category": "lifecycle",
        "source": "ass.testlogin",
        "account": msg.get("account_number", ""),
        "ts": msg.get("timestamp", ""),
        "payload": json.dumps(msg),
    }


class _Exhausted(Exception):
    """Raised by the fake when its scripted entries are consumed (ends messages())."""


class _FakeStreamRedis:
    """The slice of redis.asyncio.Redis the stream source touches.

    One stream, real consumer-group semantics for what the tests need:
    ``>`` delivery moves the group pointer and populates the PEL; XACK removes;
    XAUTOCLAIM hands the PEL over when ``min_idle_time`` is 0.
    """

    def __init__(self, entries: list[tuple[str, dict]]) -> None:
        self.entries = entries
        self.pointer: int | None = None  # set by xgroup_create
        self.pel: dict[str, dict] = {}  # id -> fields
        self.acked: list[str] = []
        self.group_creates = 0
        self.closed = False

    async def xgroup_create(
        self, name: str, groupname: str, id: str, mkstream: bool = False
    ) -> None:
        from redis.exceptions import ResponseError

        self.group_creates += 1
        if self.pointer is not None:
            raise ResponseError("BUSYGROUP Consumer Group name already exists")
        self.pointer = len(self.entries) if id == "$" else 0

    async def xreadgroup(
        self, groupname: str, consumername: str, streams: dict, count: int, block: int
    ):
        assert self.pointer is not None, "xreadgroup before xgroup_create"
        if self.pointer >= len(self.entries):
            raise _Exhausted()
        batch = self.entries[self.pointer : self.pointer + count]
        self.pointer += len(batch)
        for entry_id, fields in batch:
            self.pel[entry_id] = fields
        return [("zts:events", list(batch))]

    async def xack(self, stream: str, group: str, entry_id: str) -> int:
        self.acked.append(entry_id)
        return 1 if self.pel.pop(entry_id, None) is not None else 0

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str,
        count: int,
    ):
        if min_idle_time > 0:
            return ("0-0", [], [])  # nothing "old enough" in these tests
        claimed = sorted(self.pel.items())[:count]
        return ("0-0", claimed, [])

    async def aclose(self) -> None:
        self.closed = True


def _source(
    client: _FakeStreamRedis, accounts: AccountMapper, **kw
) -> RedisStreamMessageSource:
    kw.setdefault(
        "min_idle_reclaim_ms", 60_000
    )  # reclaim disabled unless a test opts in
    kw.setdefault("start_id", "0")  # deliver the scripted backlog ("$" would skip it)
    return RedisStreamMessageSource(
        "redis://unused:6379",
        accounts=accounts,
        group="ledger:testlogin",
        consumer="c1",
        client=client,
        max_connect_attempts=1,  # the fake's exhaustion ends messages()
        **kw,
    )


async def _drain(source: RedisStreamMessageSource) -> list:
    out = []
    with pytest.raises(_Exhausted):
        async for m in source.messages():
            out.append(m)
    return out


def _ids(n: int) -> list[str]:
    return [f"{i + 1}-0" for i in range(n)]


def test_conforms_to_protocol(accounts):
    assert isinstance(_source(_FakeStreamRedis([]), accounts), MessageSource)


async def test_delivers_typed_messages_and_acks(accounts):
    entries = [
        ("1-0", _entry(_envelope("balance", "main", {"net_liquidating_value": "1"}))),
        (
            "2-0",
            _entry(
                _envelope(
                    "position",
                    "roth",
                    {
                        "symbol": "AAPL",
                        "quantity": "100",
                        "quantity_direction": "Long",
                    },
                )
            ),
        ),
    ]
    fake = _FakeStreamRedis(entries)
    out = await _drain(_source(fake, accounts))

    assert [m.account_number for m in out] == ["ACCT1", "ACCT2"]
    # Both entries acked (each after the caller consumed it) — PEL empty.
    assert fake.acked == ["1-0", "2-0"]
    assert fake.pel == {}


async def test_foreign_and_malformed_entries_are_acked_and_skipped(accounts):
    entries = [
        (
            "1-0",
            _entry(_envelope("balance", "main", {}), event_type="trade_group.closed"),
        ),  # other producer
        (
            "2-0",
            _entry(_envelope("balance", "someone_elses_account", {})),
        ),  # unmappable nickname
        (
            "3-0",
            {"event_type": "account.balance", "payload": "not json"},
        ),  # malformed → ERROR + ack
        ("4-0", _entry(_envelope("balance", "main", {}))),
    ]
    fake = _FakeStreamRedis(entries)
    out = await _drain(_source(fake, accounts))

    assert [m.account_number for m in out] == ["ACCT1"]
    # Every entry acked — skips must never clog the PEL.
    assert fake.acked == ["1-0", "2-0", "3-0", "4-0"]


async def test_nickname_filter_scopes_to_one_login(accounts):
    entries = [
        ("1-0", _entry(_envelope("balance", "main", {}))),
        ("2-0", _entry(_envelope("balance", "roth", {}))),
    ]
    out = await _drain(_source(_FakeStreamRedis(entries), accounts, nicknames={"roth"}))
    assert [m.account_number for m in out] == ["ACCT2"]


async def test_crash_before_processing_leaves_entry_pending(accounts):
    """The at-least-once core: an entry the consumer never finished is NOT acked."""
    entries = [
        ("1-0", _entry(_envelope("balance", "main", {"net_liquidating_value": "5"})))
    ]
    fake = _FakeStreamRedis(entries)
    source = _source(fake, accounts)

    gen = source.messages()
    first = await gen.__anext__()  # delivered, "processing" starts
    assert first.account_number == "ACCT1"
    await gen.aclose()  # crash before asking for the next message

    assert fake.acked == []  # never acked...
    assert "1-0" in fake.pel  # ...still pending for reclaim


async def test_reclaim_replays_pending_entries(accounts):
    """A restarted daemon XAUTOCLAIMs the crashed consumer's PEL and re-applies."""
    entries = [
        ("1-0", _entry(_envelope("balance", "main", {"net_liquidating_value": "5"})))
    ]
    fake = _FakeStreamRedis(entries)

    # First consumer crashes mid-processing (as above).
    gen = _source(fake, accounts).messages()
    await gen.__anext__()
    await gen.aclose()
    assert "1-0" in fake.pel

    # Restarted consumer: nothing new to read, but the reclaim pass replays.
    fake.pointer = len(fake.entries)  # group pointer is past the entry
    out = await _drain(_source(fake, accounts, min_idle_reclaim_ms=0))

    assert [m.account_number for m in out] == ["ACCT1"]
    assert fake.acked == ["1-0"]
    assert fake.pel == {}


class _TimeoutScriptRedis(_FakeStreamRedis):
    """xreadgroup consults a script first: "T" raises a read timeout (the
    socket_timeout-races-block_ms production pattern), "E" delivers the next
    scripted entry. Script exhausted → _Exhausted ends messages()."""

    def __init__(self, entries: list[tuple[str, dict]], script: str) -> None:
        super().__init__(entries)
        self.script = list(script)
        self.timeouts_raised = 0

    async def xreadgroup(
        self, groupname: str, consumername: str, streams: dict, count: int, block: int
    ):
        if not self.script:
            raise _Exhausted()
        step = self.script.pop(0)
        if step == "T":
            self.timeouts_raised += 1
            raise TimeoutError("Timeout reading from localhost:6379")
        return await super().xreadgroup(groupname, consumername, streams, count, block)


async def test_read_timeouts_are_empty_polls_not_reconnects(accounts):
    """Regression (2026-07-06): an idle stream + socket_timeout <= block_ms made
    every full-length block "connection lost" — ~1.4k spurious WARNINGs/day/login.
    Interspersed timeouts must deliver everything without touching the reconnect
    path, and a successful read must reset the consecutive-timeout counter."""
    entries = [
        ("1-0", _entry(_envelope("balance", "main", {"net_liquidating_value": "1"}))),
        ("2-0", _entry(_envelope("balance", "roth", {"net_liquidating_value": "2"}))),
    ]
    # 2 timeouts, a read, 2 more timeouts, a read — never 3 in a row.
    fake = _TimeoutScriptRedis(entries, script="TTETTE")
    out = await _drain(_source(fake, accounts))

    assert [m.account_number for m in out] == ["ACCT1", "ACCT2"]
    assert fake.timeouts_raised == 4
    # No reconnect: the group was created exactly once (a reconnect re-enters
    # _ensure_group and would bump this).
    assert fake.group_creates == 1


async def test_persistent_read_timeouts_escalate_to_reconnect(accounts):
    """A genuinely hung server times out consecutively — the third in a row
    escalates to the reconnect path (here surfaced by max_connect_attempts=1)."""
    fake = _TimeoutScriptRedis([], script="TTTTTT")

    with pytest.raises(TimeoutError):
        async for _ in _source(fake, accounts).messages():
            pass  # pragma: no cover - no messages expected

    assert fake.timeouts_raised == 3  # escalated on the 3rd, not silently forever
    assert fake.group_creates == 1


async def test_end_to_end_fill_through_stream_consumer(accounts):
    store = InMemoryStore()
    await store.upsert_orders(
        [
            OrderRow(
                tt_order_id="O-7",
                account="main",
                origin=Origin.ZTS,
                ingest=Ingest.OMS_SUBMIT,
                oms_status="submitted",
            )
        ]
    )
    entries = [
        (
            "1-0",
            _entry(
                _envelope(
                    "order",
                    "main",
                    {
                        "order_id": "O-7",
                        "status": "Filled",
                        "filled_quantity": "1",
                        "remaining_quantity": "0",
                        "fill_price": "2.50",
                    },
                )
            ),
        ),
    ]
    fake = _FakeStreamRedis(entries)
    consumer = StreamConsumer(store, _source(fake, accounts), accounts=accounts)
    with pytest.raises(_Exhausted):
        await consumer.run()

    order = await store.get_order("O-7")
    assert order.oms_status == "filled"
    # Acked only after StreamConsumer applied the fill (ack-after-processing).
    assert fake.acked == ["1-0"]
