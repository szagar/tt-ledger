"""``RedisStreamMessageSource`` — durable host-platform ``MessageSource`` over ``zts:events``.

The pub/sub transport (``redis_source.RedisMessageSource``) is fire-and-forget: a message
published while this daemon is down/reconnecting is gone, and for PAPER accounts there is no
broker REST sync to recover it. This source consumes the host's durable ``zts:events`` Redis
STREAM instead, via a per-daemon consumer group — a restarted daemon **replays** what it
missed, and a crashed daemon's in-flight entries are reclaimed from the PEL.

Wire format (host envelope spec: the host repo's ``docs/plans/zts-events-stream.md``): each
stream entry carries flat string fields ``event_type`` (``account.order`` /
``account.position`` / ``account.balance``), ``account`` (nickname), ``ts``, and ``payload`` —
where ``payload`` is byte-identical to the pub/sub channel message, so this module reuses
``redis_source``'s ``*_from_pubsub`` mappers unchanged.

Delivery contract (at-least-once):

* Entries are XACK'd **after** the consumer has processed them. ``messages()`` is an async
  generator: it acks entry N when the caller resumes it for entry N+1 — and
  ``StreamConsumer.run()`` applies each message before advancing the iterator, so the ack
  always follows the DB write. A crash mid-apply leaves the entry pending.
* On startup and every ``reclaim_interval_seconds``, stale pending entries (any consumer,
  idle ≥ ``min_idle_reclaim_ms``) are XAUTOCLAIM'd and re-yielded.
* Entries that are not for this daemon (other logins' accounts, non-``account.*`` event
  types) are acked immediately — every group receives every entry; filtering is per-consumer.
* A malformed ``account.*`` entry is an UNPLANNED path: logged at ERROR (loudly — this is the
  canonical store's feed) and acked so it never poison-loops the group.

Reconnects: retries forever with capped exponential backoff (a daemon transport), resetting
after any successful read. ``max_connect_attempts`` bounds it for tests.

Read timeouts are NOT connection loss: when the caller's client carries a ``socket_timeout``
at or below ``block_ms``, every idle full-length server block races the client's own socket
timeout and loses (observed in production 2026-07-06: four daemons logging "connection lost
... reconnecting" in the same microsecond, ~1.4k WARNINGs/day/login, on a perfectly healthy
stream). redis-py disconnects the socket before raising, so the pooled client is safe to
reuse — a blocking-read timeout is treated as an empty poll and retried quietly, escalating
to the reconnect path only after ``_MAX_CONSECUTIVE_READ_TIMEOUTS`` in a row (a genuinely
hung server keeps timing out; an idle-but-healthy stream intersperses successful reads).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, AsyncIterator

from .redis_source import (
    balance_from_pubsub,
    fill_event_from_pubsub,
    position_from_pubsub,
)

if TYPE_CHECKING:
    from ..identity import AccountMapper
    from ..rows import FillEvent
    from .broker import BalanceMessage, BrokerPosition

logger = logging.getLogger(__name__)

_DEFAULT_STREAM_KEY = "zts:events"
_RECONNECT_INITIAL_SECONDS = 1.0
_RECONNECT_MAX_SECONDS = 30.0
_MAX_CONSECUTIVE_READ_TIMEOUTS = 3


def _read_timeout_errors() -> tuple[type[BaseException], ...]:
    """Exception types that mean "the blocking read timed out", not "the connection died".
    Builtin ``TimeoutError`` covers ``asyncio.TimeoutError`` (an alias since 3.11); redis-py's
    ``TimeoutError`` subclasses ``RedisError`` only, so it needs its own entry."""
    try:
        from redis.exceptions import TimeoutError as _RedisTimeoutError
    except ModuleNotFoundError:  # test-seam clients don't require the [redis] extra
        return (TimeoutError,)
    return (TimeoutError, _RedisTimeoutError)


class RedisStreamMessageSource:
    """Implements the ``MessageSource`` Protocol (``ingest/push.py``) over the host platform's
    ``zts:events`` Redis Stream with a consumer group. ``nicknames`` (optional) filters to one
    login's accounts — a multi-login host publishes every account on the same stream."""

    def __init__(
        self,
        url: str,
        *,
        accounts: "AccountMapper",
        group: str,
        consumer: str,
        nicknames: set[str] | None = None,
        stream_key: str = _DEFAULT_STREAM_KEY,
        start_id: str = "$",
        block_ms: int = 5000,
        batch_count: int = 100,
        reclaim_interval_seconds: float = 60.0,
        min_idle_reclaim_ms: int = 60_000,
        max_connect_attempts: int | None = None,
        client=None,  # noqa: ANN001 -- test seam: a pre-built redis.asyncio.Redis
    ) -> None:
        if client is None:
            try:
                import redis  # noqa: F401 -- import only to fail fast & clearly if missing
            except ModuleNotFoundError as exc:  # pragma: no cover
                raise RuntimeError(
                    "RedisStreamMessageSource needs the [redis] extra: pip install tt-ledger[redis]"
                ) from exc
        self._url = url
        self._accounts = accounts
        self._nicknames = nicknames
        self._stream = stream_key
        self._group = group
        self._consumer = consumer
        self._start_id = start_id
        self._block_ms = block_ms
        self._batch_count = batch_count
        self._reclaim_interval = reclaim_interval_seconds
        self._min_idle_reclaim_ms = min_idle_reclaim_ms
        self._max_connect_attempts = max_connect_attempts
        self._client = client

    def _make_client(self):  # noqa: ANN202
        if self._client is not None:
            return self._client
        import redis.asyncio as aioredis

        return aioredis.from_url(self._url, decode_responses=True)

    async def _ensure_group(self, client) -> None:  # noqa: ANN001
        """Idempotent XGROUP CREATE (MKSTREAM); swallows BUSYGROUP."""
        from redis.exceptions import ResponseError

        try:
            await client.xgroup_create(
                name=self._stream,
                groupname=self._group,
                id=self._start_id,
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def messages(
        self,
    ) -> "AsyncIterator[FillEvent | BrokerPosition | BalanceMessage]":
        attempts = 0
        backoff = _RECONNECT_INITIAL_SECONDS
        timeout_errors = _read_timeout_errors()
        while True:
            client = self._make_client()
            try:
                await self._ensure_group(client)
                next_reclaim = 0.0
                idle_timeouts = 0
                while True:
                    if time.monotonic() >= next_reclaim:
                        async for item in self._drain_reclaimed(client):
                            yield item
                        next_reclaim = time.monotonic() + self._reclaim_interval

                    try:
                        resp = await client.xreadgroup(
                            groupname=self._group,
                            consumername=self._consumer,
                            streams={self._stream: ">"},
                            count=self._batch_count,
                            block=self._block_ms,
                        )
                    except timeout_errors:
                        # Idle stream racing the client's socket_timeout — an
                        # empty poll, not connection loss (redis-py already
                        # recycled the socket; the pool reconnects on the next
                        # command). Escalate only when timeouts are persistent.
                        idle_timeouts += 1
                        if idle_timeouts >= _MAX_CONSECUTIVE_READ_TIMEOUTS:
                            raise
                        logger.debug(
                            "blocking read timed out on idle stream; polling again (%d/%d)",
                            idle_timeouts,
                            _MAX_CONSECUTIVE_READ_TIMEOUTS,
                        )
                        continue
                    idle_timeouts = 0
                    attempts, backoff = 0, _RECONNECT_INITIAL_SECONDS  # healthy
                    if not resp:
                        # Real redis blocked server-side for block_ms; the
                        # explicit yield keeps this loop cooperative on
                        # clients whose xreadgroup returns immediately
                        # (fakeredis) so it can never starve the event loop.
                        await asyncio.sleep(0)
                        continue
                    for _stream_key, entries in resp or []:
                        for message_id, fields in entries:
                            parsed = self._parse_entry(message_id, fields)
                            if parsed is None:
                                await client.xack(self._stream, self._group, message_id)
                                continue
                            yield parsed
                            # The caller has processed the message (generator
                            # resumed) -- safe to ack. A crash before this
                            # point leaves the entry pending for reclaim.
                            await client.xack(self._stream, self._group, message_id)
            except Exception as exc:
                attempts += 1
                if (
                    self._max_connect_attempts is not None
                    and attempts >= self._max_connect_attempts
                ):
                    raise
                logger.warning(
                    "redis stream connection lost (%s); reconnecting in %.1fs (attempt %d)",
                    exc,
                    backoff,
                    attempts,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_MAX_SECONDS)
            finally:
                # a client handed in via the test seam is owned by the caller
                if self._client is None:
                    try:
                        await client.aclose()
                    except Exception:
                        pass

    async def _drain_reclaimed(
        self,
        client,  # noqa: ANN001
    ) -> "AsyncIterator[FillEvent | BrokerPosition | BalanceMessage]":
        """XAUTOCLAIM stale pending entries (a dead consumer's PEL) and re-yield them."""
        cursor = "0-0"
        while True:
            resp = await client.xautoclaim(
                name=self._stream,
                groupname=self._group,
                consumername=self._consumer,
                min_idle_time=self._min_idle_reclaim_ms,
                start_id=cursor,
                count=self._batch_count,
            )
            next_cursor, entries = resp[0], resp[1]
            for message_id, fields in entries:
                if not fields:
                    # Trimmed away underneath the PEL -- unrecoverable; the
                    # REST sync / nightly reconcile are the only backstops.
                    logger.error(
                        "pending zts:events entry %s was trimmed before replay "
                        "(group=%s) -- possible data loss for paper accounts",
                        message_id,
                        self._group,
                    )
                    await client.xack(self._stream, self._group, message_id)
                    continue
                parsed = self._parse_entry(message_id, fields)
                if parsed is None:
                    await client.xack(self._stream, self._group, message_id)
                    continue
                yield parsed
                await client.xack(self._stream, self._group, message_id)
            next_str = (
                next_cursor if isinstance(next_cursor, str) else next_cursor.decode()
            )
            if next_str == "0-0" or next_str == cursor or not entries:
                return
            cursor = next_str

    def _parse_entry(
        self,
        message_id,
        fields,  # noqa: ANN001
    ) -> "FillEvent | BrokerPosition | BalanceMessage | None":
        """One stream entry -> a typed message, or None (ack-and-skip).

        None covers three cases: not an ``account.*`` event (other producers
        share the stream by design), not one of this daemon's accounts, or a
        malformed entry (unplanned -- logged at ERROR, never poison-loops).
        """

        def _s(value) -> str:  # noqa: ANN001 -- tolerate bytes clients
            return value if isinstance(value, str) else value.decode()

        try:
            decoded = {_s(k): _s(v) for k, v in fields.items()}
            event_type = decoded.get("event_type", "")
            if not event_type.startswith("account."):
                return None
            msg = json.loads(decoded["payload"])
            if not isinstance(msg, dict):
                raise ValueError("payload is not an object")
        except Exception:
            logger.exception(
                "malformed zts:events entry %s (group=%s) -- acked and skipped",
                message_id,
                self._group,
            )
            return None

        nickname = msg.get("account_number")
        if not nickname or (
            self._nicknames is not None and nickname not in self._nicknames
        ):
            return None
        try:
            account_number = self._accounts.to_account_number(nickname)
        except KeyError:
            return None  # not one of ours (unknown to this mapper) -- skip, don't fail the stream

        msg_type = msg.get("type")
        if msg_type == "order":
            return fill_event_from_pubsub(msg)
        if msg_type == "position":
            return position_from_pubsub(msg, account_number)
        if msg_type == "balance":
            return balance_from_pubsub(msg, account_number)
        logger.error(
            "zts:events entry %s has account.* event_type but unknown payload type %r -- acked and skipped",
            message_id,
            msg_type,
        )
        return None
