"""$PORT health endpoint (LAB-738 AC-0).

Render's free tier hosts web services only, and a free web service must
answer HTTP on $PORT or the deploy's port scan fails. This module is the
ingester's whole HTTP surface: ``GET /health``, liveness only — no aggregate
data, no key material.

Hand-rolled on ``asyncio.start_server`` so the listener shares the ingest
event loop without blocking it, and without adding a web framework for one
route. ``/health`` returns 503 while Jetstream is disconnected so a dead
consumer inside a live process is visible from outside (same property
``jetstream.ingest_raw`` guards for silent drops).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from contextlib import suppress

logger = logging.getLogger(__name__)

_REASONS = {200: "OK", 404: "Not Found", 405: "Method Not Allowed", 503: "Service Unavailable"}
# One deadline for the whole exchange (read + respond). Per-line timeouts
# alone let a client drip one header every few seconds and hold the
# connection — and its task on the shared ingest loop — open forever
# (expert-panel finding, CWE-400).
_EXCHANGE_DEADLINE_SECONDS = 10.0
# Line length is enforced by the StreamReader limit= (readline raises
# ValueError past it); this also bounds per-connection buffer memory.
_MAX_LINE_BYTES = 8192
_MAX_HEADER_LINES = 100


class HealthState:
    """Liveness counters shared by the consumer, the publish loop and the listener.

    Single-threaded by design: every writer runs on the ingest event loop, so
    plain attributes need no locking.
    """

    def __init__(self, now_fn: Callable[[], float] = time.time) -> None:
        self._now = now_fn
        self.started_at = now_fn()
        self.jetstream_connected = False
        self.events_seen = 0
        self.events_missing_source = 0
        self.last_event_at: float | None = None
        self.last_publish_at: float | None = None

    def event(self) -> None:
        self.events_seen += 1
        self.last_event_at = self._now()

    def published(self) -> None:
        self.last_publish_at = self._now()

    def missing_source(self) -> int:
        self.events_missing_source += 1
        return self.events_missing_source

    def snapshot(self) -> tuple[int, dict]:
        """(HTTP status, body) for /health — 503 whenever Jetstream is down."""
        now = self._now()

        def age(t: float | None) -> float | None:
            return None if t is None else round(now - t, 1)

        status = 200 if self.jetstream_connected else 503
        return status, {
            "status": "ok" if status == 200 else "degraded",
            "jetstream_connected": self.jetstream_connected,
            "events_seen": self.events_seen,
            "events_missing_source": self.events_missing_source,
            "last_event_age_seconds": age(self.last_event_at),
            "last_publish_age_seconds": age(self.last_publish_at),
            "uptime_seconds": round(now - self.started_at, 1),
        }


async def _exchange(state: HealthState, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    request_line = await reader.readline()
    method, path, *_ = (*request_line.decode("latin-1").split(), "", "")
    # Drain headers so well-behaved clients aren't reset mid-send; the count
    # cap plus the caller's overall deadline bound hostile ones.
    for _ in range(_MAX_HEADER_LINES):
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
    extra_headers = ""
    if method not in ("GET", "HEAD"):
        status, body = 405, {"error": "method_not_allowed"}
        extra_headers = "allow: GET, HEAD\r\n"
    elif path.partition("?")[0] != "/health":
        status, body = 404, {"error": "not_found"}
    else:
        status, body = state.snapshot()
    payload = json.dumps(body).encode()
    writer.write(
        (
            f"HTTP/1.1 {status} {_REASONS[status]}\r\n"
            f"content-type: application/json\r\n"
            f"content-length: {len(payload)}\r\n"
            f"{extra_headers}"
            f"connection: close\r\n\r\n"
        ).encode()
        + (b"" if method == "HEAD" else payload)
    )
    await writer.drain()


async def _respond(state: HealthState, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        await asyncio.wait_for(_exchange(state, reader, writer), _EXCHANGE_DEADLINE_SECONDS)
    except (TimeoutError, ConnectionError, OSError, ValueError) as exc:
        # Port scanners, half-open probes, oversized lines (ValueError from
        # the reader limit): expected on a public listener, so close without
        # escalating — but leave a debug trace so a systematic failure (e.g.
        # every probe timing out) is diagnosable (Kody review, PR #9).
        logger.debug("health connection aborted: %r", exc)
    finally:
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()


async def start_health_server(state: HealthState, port: int, host: str = "0.0.0.0") -> asyncio.Server:
    server = await asyncio.start_server(lambda r, w: _respond(state, r, w), host=host, port=port, limit=_MAX_LINE_BYTES)
    logger.info("health endpoint listening on %s:%d", host, server.sockets[0].getsockname()[1])
    return server


async def serve_health(state: HealthState, port: int) -> None:
    """Run the listener forever on the shared event loop (one gather() leg)."""
    server = await start_health_server(state, port)
    async with server:
        await server.serve_forever()
