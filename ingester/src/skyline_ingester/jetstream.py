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
from skyline_ingester.windows import WindowStore

logger = logging.getLogger(__name__)

MAX_BACKOFF = 60.0
# Payload timestamps are untrusted. One far-future time_us would (a) set a
# retention floor in WindowStore._prune that instantly evicts every real bucket
# — wiping the restart-critical 24h window and poisoning the next checkpoint —
# and (b) as a resume cursor, skip every real event on the next reconnect.
# Anything beyond this skew over wall-clock is dropped whole.
MAX_FUTURE_SKEW_SECONDS = 300.0


def ingest_raw(raw: str | bytes, store: WindowStore, *, now_fn: Callable[[], float] = time.time) -> int | None:
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
        return None
    if time_us / 1_000_000 > now_fn() + MAX_FUTURE_SKEW_SECONDS:
        logger.warning("dropping future-dated Jetstream event (time_us=%d)", time_us)
        return None
    feats = extract_post(event)
    if feats is not None:
        store.add(feats)
    return time_us


def subscribe_url(base: str, cursor: int | None = None) -> str:
    params = [("wantedCollections", POST_COLLECTION)]
    if cursor is not None:
        params.append(("cursor", str(cursor)))
    return base + ("&" if "?" in base else "?") + urlencode(params)


async def consume(base_url: str, store: WindowStore) -> None:
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
                backoff = 1.0
                async for raw in ws:
                    time_us = ingest_raw(raw, store)
                    if time_us is not None:
                        cursor = time_us
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Jetstream connection lost (%s: %s); reconnecting in %.0fs", type(exc).__name__, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
