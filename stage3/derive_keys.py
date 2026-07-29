"""Print every cache key the ingester writes (LAB-737 evidence tooling).

Drives the real Publisher against the in-process bytes backend, so the keys
come from the same decorator machinery the live service uses — the 15
interop/v1 aggregate keys, the auto-mode checkpoint key, and the auto-mode
@cache.secure sentiment key. No network, no credentials.

    cd ingester && uv run python ../stage3/derive_keys.py
"""

from __future__ import annotations

import time

from skyline_ingester.backends import MemoryBytesBackend
from skyline_ingester.publisher import Publisher
from skyline_ingester.windows import WINDOW_TTLS, WindowStore

# Obviously-fake placeholder: only unlocks the Publisher's secure-cache code
# path so its key NAME gets derived. Cache-key derivation never mixes the
# master key in, so any value yields the same keys; nothing here is secret.
PLACEHOLDER_MASTER_KEY = "0" * 64


def main() -> None:
    backend = MemoryBytesBackend()
    publisher = Publisher(WindowStore(), backend, master_key=PLACEHOLDER_MASTER_KEY, now_fn=time.time)
    for window in WINDOW_TTLS:
        publisher.publish_window(window)
    publisher.checkpoint()
    for key in sorted(backend.stored_keys()):
        print(key)


if __name__ == "__main__":
    main()
