"""Service entrypoint.

With CACHEKIT_API_KEY set: ingests live Jetstream and writes real CachekitIO
entries. Without it: dry-run mode — same pipeline, in-process backend, every
write logged. Each window republishes at TTL/2 so readers never see a miss.
"""

from __future__ import annotations

import asyncio
import logging
import time

from skyline_ingester.backends import MemoryBytesBackend
from skyline_ingester.config import Settings
from skyline_ingester.jetstream import consume
from skyline_ingester.publisher import Publisher
from skyline_ingester.windows import WINDOW_TTLS, WindowStore

logger = logging.getLogger("skyline_ingester")


def build_publisher(settings: Settings, store: WindowStore) -> Publisher:
    if settings.cachekit_api_key is not None:
        # Fail closed (epic decision, ray 2026-07-24): the AC-6 secure cache is part
        # of the demo, so a live deploy without its master key must not come up.
        if settings.cachekit_master_key is None:
            raise RuntimeError("CACHEKIT_MASTER_KEY is required in live mode: the secure sentiment cache must fail closed")
        from cachekit.backends.cachekitio import CachekitIOBackend

        backend = CachekitIOBackend(api_key=settings.cachekit_api_key.get_secret_value())
        logger.info("live mode: publishing to CachekitIO")
    else:
        backend = MemoryBytesBackend(log_writes=True)
        logger.warning("CACHEKIT_API_KEY not set — dry-run mode, writes are logged only")
    master_key = settings.cachekit_master_key.get_secret_value() if settings.cachekit_master_key else None
    publisher = Publisher(store, backend, master_key=master_key, top_n=settings.top_n)
    if not publisher.secure_enabled:
        logger.warning("CACHEKIT_MASTER_KEY not set — secure sentiment cache disabled (dry-run only)")
    return publisher


async def publish_loop(publisher: Publisher, tick_seconds: float) -> None:
    next_due = dict.fromkeys(WINDOW_TTLS, 0.0)
    while True:
        now = time.time()
        for window, ttl in WINDOW_TTLS.items():
            if now >= next_due[window]:
                published = await asyncio.to_thread(publisher.publish_window, window)
                logger.info("published %d aggregates for window %s", published, window)
                next_due[window] = now + ttl / 2
        await asyncio.sleep(tick_seconds)


async def checkpoint_loop(publisher: Publisher, interval_seconds: float) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await asyncio.to_thread(publisher.checkpoint)
            logger.debug("checkpoint written")
        except Exception:
            logger.exception("checkpoint failed")


async def run(settings: Settings) -> None:
    store = WindowStore()
    publisher = build_publisher(settings, store)
    await asyncio.to_thread(publisher.restore_checkpoint)
    await asyncio.gather(
        consume(settings.jetstream_url, store),
        publish_loop(publisher, settings.publish_tick_seconds),
        checkpoint_loop(publisher, settings.checkpoint_interval_seconds),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(run(Settings()))
    except KeyboardInterrupt:
        logger.info("shutting down")


if __name__ == "__main__":
    main()
