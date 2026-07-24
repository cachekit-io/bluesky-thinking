"""In-process bytes backend: the unit-test double and the dry-run stand-in.

Implements cachekit's structural BaseBackend protocol (bytes in/out), so the
interop value contract is fully enforced even without a real backend —
interop mode rejects backend=None (L1-only) by design.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


class MemoryBytesBackend:
    def __init__(self, log_writes: bool = False):
        self._store: dict[str, tuple[bytes, float | None]] = {}
        self.log_writes = log_writes
        self.ttls: dict[str, int | None] = {}  # last ttl per key, for tests/inspection

    def get(self, key: str) -> bytes | None:
        item = self._store.get(key)
        if item is None:
            return None
        value, expires = item
        if expires is not None and time.monotonic() > expires:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: bytes, ttl: int | None = None) -> None:
        if self.log_writes:
            logger.info("dry-run write: %s (%d bytes, ttl=%ss)", key, len(value), ttl)
        self._store[key] = (bytes(value), time.monotonic() + ttl if ttl else None)
        self.ttls[key] = ttl

    def delete(self, key: str) -> bool:
        self.ttls.pop(key, None)
        return self._store.pop(key, None) is not None

    def exists(self, key: str) -> bool:
        return self.get(key) is not None

    def health_check(self) -> tuple[bool, dict[str, Any]]:
        return True, {"latency_ms": 0.0, "backend_type": "memory-bytes"}

    def stored_keys(self) -> list[str]:
        return [k for k in list(self._store) if self.get(k) is not None]
