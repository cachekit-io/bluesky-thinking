"""Publish window aggregates to CacheKit under the locked interop/v1 contract.

cachekit is decorator-only (no get/set), so publishing is: invalidate the
key, then call the wrapper — the miss recomputes from the WindowStore and
writes fresh bytes. TTL is fixed per decorator, so each (operation, window)
pair gets its own wrapper pinned to the locked TTL (15 in total).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from cachekit import cache
from cachekit.config import EncryptionConfig, L1CacheConfig

from skyline_ingester import NAMESPACE
from skyline_ingester.windows import OPERATIONS, WINDOW_TTLS, WindowStore

logger = logging.getLogger(__name__)

CHECKPOINT_TTL = 26 * 3600  # outlives the 24h window plus restart slack
SECURE_WINDOW = "1h"  # the sensitive derived cache publishes one window
SECURE_TTL = WINDOW_TTLS[SECURE_WINDOW]


def _no_l1() -> L1CacheConfig:
    # This service only writes; cachekit's L1 is process-global per key, so
    # fronting the writes would duplicate every published value in RAM (and
    # leak state across Publisher instances in tests) for zero read benefit.
    return L1CacheConfig(enabled=False)


def _no_encryption() -> EncryptionConfig:
    # cachekit auto-enables encryption when CACHEKIT_MASTER_KEY is in the env
    # (which it is whenever the secure cache is on). The interop aggregates are
    # contract-locked to PLAIN MessagePack — encrypted bytes would be unreadable
    # by the TS/Rust readers — and the checkpoint must survive a restart on a
    # different host (Render), which a machine-local encryption UUID would break.
    return EncryptionConfig(enabled=False)


class Publisher:
    """Owns the decorated cache wrappers and the publish/checkpoint actions."""

    def __init__(
        self,
        store: WindowStore,
        backend,
        *,
        master_key: str | None = None,
        top_n: int = 50,
        now_fn: Callable[[], float] = time.time,
    ):
        self._store = store
        self._top_n = top_n
        self._now = now_fn

        self._publish_fns: dict[tuple[str, str], Callable] = {}
        for operation in OPERATIONS:
            for window, ttl in WINDOW_TTLS.items():
                self._publish_fns[(operation, window)] = self._make_publish_fn(operation, ttl, backend)

        @cache(namespace=NAMESPACE, ttl=CHECKPOINT_TTL, backend=backend, l1=_no_l1(), encryption=_no_encryption())
        def skyline_window_checkpoint() -> dict:
            return self._store.snapshot(self._now())

        self._checkpoint_fn = skyline_window_checkpoint

        self._secure_fn = None
        if master_key is not None:
            # No l1 override here: the secure intent preset pins its own L1
            # config (encrypted bytes only), and a second l1 kwarg collides.

            @cache.secure(master_key=master_key, namespace=NAMESPACE, ttl=SECURE_TTL, backend=backend)
            def language_sentiment(window: str) -> dict:
                return self._store.sentiment_value(window, self._now())

            self._secure_fn = language_sentiment

    def _make_publish_fn(self, operation: str, ttl: int, backend) -> Callable:
        @cache(interop=operation, namespace=NAMESPACE, ttl=ttl, backend=backend, l1=_no_l1(), encryption=_no_encryption())
        def publish(window: str) -> dict:
            return self._store.build_value(operation, window, self._now(), self._top_n)

        return publish

    @property
    def secure_enabled(self) -> bool:
        return self._secure_fn is not None

    def _refresh(self, fn: Callable, window: str, recompute: Callable[[], object], label: str) -> int:
        """Recompute the value, then invalidate + republish one wrapper.

        cachekit is decorator-only, so a fresh write is invalidate-then-call — and
        if the recompute would raise we must find out BEFORE invalidating, or a
        failed recompute leaves the key deleted (a cache miss on the metered-miss
        path) until the next tick.

        Honest ceiling: the probe only covers RECOMPUTE failure. The wrapper call
        after invalidate is itself recompute-then-backend-WRITE, and a write
        failure at that point still leaves the key deleted until the next tick —
        cachekit has no atomic set/replace (confirmed against 0.15.0), so this is
        as close as the decorator API allows.
        """
        try:
            recompute()
            fn.invalidate_cache(window)
            fn(window)
            return 1
        except Exception:
            logger.exception("publish failed: %s", label)
            return 0

    def publish_window(self, window: str) -> int:
        """Refresh every operation for one window; returns how many published."""
        published = 0
        for operation in OPERATIONS:
            fn = self._publish_fns[(operation, window)]
            published += self._refresh(
                fn,
                window,
                lambda operation=operation: self._store.build_value(operation, window, self._now(), self._top_n),
                f"{operation}/{window}",
            )
        if window == SECURE_WINDOW and self._secure_fn is not None:
            published += self._refresh(
                self._secure_fn,
                window,
                lambda: self._store.sentiment_value(window, self._now()),
                f"language_sentiment/{window}",
            )
        return published

    def checkpoint(self) -> None:
        """Force-write the current window state (restart insurance).

        No recompute probe here (unlike _refresh): snapshot() is a pure in-memory
        walk of our own state — the only realistic failure after invalidate is the
        backend WRITE, which no probe can cover (see _refresh's ceiling note), so a
        probe would just double the snapshot cost for nothing.
        """
        self._checkpoint_fn.invalidate_cache()
        self._checkpoint_fn()

    def restore_checkpoint(self) -> int:
        """Load the last checkpoint into the store; returns buckets restored.

        On a cold cache the call is a miss, which harmlessly writes a snapshot
        of the (empty) store and restores 0 buckets. A read failure (backend error
        or a malformed checkpoint) degrades to 0 rather than propagating — a bad
        checkpoint must never crash startup into a permanent boot loop.
        """
        try:
            snap = self._checkpoint_fn()
        except Exception:
            logger.exception("checkpoint read failed at startup; continuing with a cold window")
            return 0
        restored = self._store.restore(snap, self._now())
        if restored:
            logger.info("restored %d window buckets from checkpoint (saved_at=%s)", restored, snap.get("saved_at"))
        return restored
