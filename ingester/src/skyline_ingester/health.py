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
_LINE_TIMEOUT_SECONDS = 5.0
# A request line / header line longer than this is not a health probe.
_MAX_LINE_BYTES = 8192


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
        self.last_event_at: float | None = None
        self.last_publish_at: float | None = None

    def event(self) -> None:
        self.events_seen += 1
        self.last_event_at = self._now()

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
            "last_event_age_seconds": age(self.last_event_at),
            "last_publish_age_seconds": age(self.last_publish_at),
            "uptime_seconds": round(now - self.started_at, 1),
        }


async def _respond(state: HealthState, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        request_line = await asyncio.wait_for(reader.readline(), _LINE_TIMEOUT_SECONDS)
        if len(request_line) > _MAX_LINE_BYTES:
            return
        method, path, *_ = (*request_line.decode("latin-1").split(), "", "")
        # Drain headers so well-behaved clients aren't reset mid-send.
        while True:
            line = await asyncio.wait_for(reader.readline(), _LINE_TIMEOUT_SECONDS)
            if line in (b"\r\n", b"\n", b"") or len(line) > _MAX_LINE_BYTES:
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
    except (TimeoutError, ConnectionError, OSError):
        pass  # port scanners and half-open probes; nothing to answer
    finally:
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()


async def start_health_server(state: HealthState, port: int, host: str = "0.0.0.0") -> asyncio.Server:
    server = await asyncio.start_server(lambda r, w: _respond(state, r, w), host=host, port=port)
    bound = server.sockets[0].getsockname()[1] if server.sockets else port
    logger.info("health endpoint listening on %s:%d", host, bound)
    return server


async def serve_health(state: HealthState, port: int) -> None:
    """Run the listener forever on the shared event loop (one gather() leg)."""
    server = await start_health_server(state, port)
    async with server:
        await server.serve_forever()
