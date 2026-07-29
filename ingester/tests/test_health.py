"""AC-0 (LAB-738): the /health listener — status codes, liveness body, routing."""

from __future__ import annotations

import asyncio
import json

from skyline_ingester.health import HealthState, start_health_server


def make_state(now: float = 1000.0) -> tuple[HealthState, list[float]]:
    clock = [now]
    state = HealthState(now_fn=lambda: clock[0])
    return state, clock


def test_snapshot_disconnected_is_503() -> None:
    state, _ = make_state()
    status, body = state.snapshot()
    assert status == 503
    assert body["status"] == "degraded"
    assert body["jetstream_connected"] is False


def test_snapshot_connected_reports_ages() -> None:
    state, clock = make_state(now=1000.0)
    state.jetstream_connected = True
    state.event()
    state.last_publish_at = 1000.0
    clock[0] = 1012.5
    status, body = state.snapshot()
    assert status == 200
    assert body["status"] == "ok"
    assert body["events_seen"] == 1
    assert body["last_event_age_seconds"] == 12.5
    assert body["last_publish_age_seconds"] == 12.5
    assert body["uptime_seconds"] == 12.5


def test_snapshot_never_leaks_payload_or_keys() -> None:
    # AC-0: liveness only — no aggregate data, no key material. Pin the exact
    # key set so a future field addition is a conscious decision.
    state, _ = make_state()
    _, body = state.snapshot()
    assert set(body) == {
        "status",
        "jetstream_connected",
        "events_seen",
        "last_event_age_seconds",
        "last_publish_age_seconds",
        "uptime_seconds",
    }


async def _request(port: int, raw: bytes) -> tuple[int, dict[str, str], bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    response = await reader.read()
    writer.close()
    await writer.wait_closed()
    head, _, payload = response.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {k.lower(): v for k, v in (line.split(": ", 1) for line in lines[1:])}
    return status, headers, payload


def test_server_end_to_end() -> None:
    # No pytest-asyncio in the dev group; one asyncio.run keeps it that way.
    asyncio.run(_server_end_to_end())


async def _server_end_to_end() -> None:
    state, _ = make_state()
    server = await start_health_server(state, port=0, host="127.0.0.1")
    port = server.sockets[0].getsockname()[1]
    try:
        status, headers, payload = await _request(port, b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
        assert status == 503
        assert headers["content-type"] == "application/json"
        assert json.loads(payload)["jetstream_connected"] is False

        state.jetstream_connected = True
        status, _, payload = await _request(port, b"GET /health?probe=1 HTTP/1.1\r\nhost: x\r\n\r\n")
        assert status == 200
        assert json.loads(payload)["status"] == "ok"

        status, headers, payload = await _request(port, b"HEAD /health HTTP/1.1\r\nhost: x\r\n\r\n")
        assert status == 200
        assert payload == b""
        assert int(headers["content-length"]) > 0

        status, _, payload = await _request(port, b"GET /api/stats HTTP/1.1\r\nhost: x\r\n\r\n")
        assert status == 404

        status, headers, _ = await _request(port, b"POST /health HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\n\r\n")
        assert status == 405
        assert headers["allow"] == "GET, HEAD"

        # Oversized request line: the reader limit turns it into a ValueError
        # inside the handler, which must close the connection quietly instead
        # of escaping as an unretrieved task exception (expert-panel finding).
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /" + b"a" * 16384 + b" HTTP/1.1\r\n\r\n")
        await writer.drain()
        assert await reader.read() == b""
        writer.close()
        await writer.wait_closed()

        # And the listener still answers afterwards.
        status, _, _ = await _request(port, b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
        assert status == 200
    finally:
        server.close()
        await server.wait_closed()
