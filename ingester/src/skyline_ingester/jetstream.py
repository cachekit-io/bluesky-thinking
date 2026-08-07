"""Bluesky Jetstream consumer: filtered WebSocket -> WindowStore."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from urllib.parse import urlencode

import websockets

from skyline_ingester.extract import POST_COLLECTION, extract_post
from skyline_ingester.health import HealthState
from skyline_ingester.windows import WindowStore

logger = logging.getLogger(__name__)

MAX_BACKOFF = 60.0
# Payload timestamps are untrusted. One far-future time_us would (a) set a
# retention floor in WindowStore._prune that instantly evicts every real bucket
# — wiping the restart-critical 24h window and poisoning the next checkpoint —
# and (b) as a resume cursor, skip every real event on the next reconnect.
# Anything beyond this skew over wall-clock is dropped whole.
MAX_FUTURE_SKEW_SECONDS = 300.0
MISSING_SOURCE_LOG_INTERVAL = 1_000


def ingest_raw(
    raw: str | bytes,
    store: WindowStore,
    *,
    now_fn: Callable[[], float] = time.time,
    health: HealthState | None = None,
) -> int | None:
    """Parse one Jetstream frame into the store; returns the event's time_us cursor.

    Future-dated events (beyond MAX_FUTURE_SKEW_SECONDS of wall-clock) are dropped
    entirely — neither aggregated nor used to advance the cursor.
    """
    try:
        event = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        logger.warning("unparseable Jetstream frame (%d bytes)", len(raw))
        return None
    time_us = event.get("time_us") if isinstance(event, dict) else None
    if not isinstance(time_us, int):
        # Same visibility as the sibling drops: a Jetstream schema change here
        # would otherwise be 100% silent data loss under a healthy-looking loop.
        logger.warning("dropping Jetstream frame without int time_us")
        return None
    if time_us / 1_000_000 > now_fn() + MAX_FUTURE_SKEW_SECONDS:
        logger.warning("dropping future-dated Jetstream event (time_us=%d)", time_us)
        return None
    feats = extract_post(event)
    if feats is not None:
        source_id = event.get("did")
        if (not isinstance(source_id, str) or not source_id) and health is not None:
            missing_count = health.missing_source()
            if missing_count == 1 or missing_count % MISSING_SOURCE_LOG_INTERVAL == 0:
                logger.warning("Jetstream posts missing source DID; trend signals excluded (count=%d)", missing_count)
        # The raw DID crosses only this call boundary. WindowStore immediately
        # folds it into a process-keyed contribution digest and never stores or
        # logs the identifier itself.
        store.add(feats, source_id=source_id)
    return time_us


def subscribe_url(base: str, cursor: int | None = None) -> str:
    params = [("wantedCollections", POST_COLLECTION)]
    if cursor is not None:
        params.append(("cursor", str(cursor)))
    return base + ("&" if "?" in base else "?") + urlencode(params)


async def consume(base_url: str, store: WindowStore, health: HealthState) -> None:
    """Consume forever, reconnecting with backoff and resuming from the last cursor.

    ponytail: resumes at the last seen time_us with no rewind — a reconnect can
    drop the in-flight events. Rewind the cursor a few seconds (and dedupe) if
    exact counts ever matter more than simplicity.
    """
    cursor: int | None = None
    backoff = 1.0
    while True:
        url = subscribe_url(base_url, cursor)
        try:
            async with websockets.connect(url) as ws:
                logger.info("connected to Jetstream: %s", url)
                health.jetstream_connected = True
                async for raw in ws:
                    time_us = ingest_raw(raw, store, health=health)
                    if time_us is not None:
                        # Reset on real events, not on handshake: a server that
                        # accepts-then-closes must not defeat the backoff.
                        backoff = 1.0
                        cursor = time_us
                        health.event()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Jetstream connection lost (%s: %s); reconnecting in %.0fs", type(exc).__name__, exc, backoff)
        # Both exits land here — error AND clean close (the async-for ends
        # without raising). Backing off both plugs the zero-delay reconnect
        # spin a drain/policy close would otherwise cause (expert-panel
        # finding), and the flag drops BEFORE the sleep so /health goes 503
        # the moment the socket dies, not after the backoff.
        health.jetstream_connected = False
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
