"""Shared fixtures. Everything runs with no network and no CACHEKIT_API_KEY.

Fixture-stream anchor: NOW is an exact minute boundary; the recorded events sit
at NOW-4..0 min (5m window), NOW-30 min (1h), NOW-600 min (24h), NOW-1500 min
(outside every window), plus non-post noise the filter must drop.
"""

from pathlib import Path

import pytest

from skyline_ingester.backends import MemoryBytesBackend
from skyline_ingester.jetstream import ingest_raw
from skyline_ingester.publisher import Publisher
from skyline_ingester.windows import WindowStore

NOW_MIN = 29_233_338
NOW = NOW_MIN * 60.0

# Post totals baked into tests/fixtures/jetstream_events.jsonl
FIXTURE_TOTALS = {"5m": 12, "1h": 18, "24h": 26}

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "jetstream_events.jsonl"

MASTER_KEY = "ab" * 32  # test-only key for the secure cache


@pytest.fixture
def fixture_lines() -> list[str]:
    return FIXTURE_PATH.read_text().splitlines()


@pytest.fixture
def store(fixture_lines) -> WindowStore:
    s = WindowStore()
    for line in fixture_lines:
        ingest_raw(line, s)
    return s


@pytest.fixture
def backend() -> MemoryBytesBackend:
    return MemoryBytesBackend()


@pytest.fixture
def publisher(store, backend) -> Publisher:
    return Publisher(store, backend, master_key=MASTER_KEY, now_fn=lambda: NOW)
