"""Print every cache key the ingester writes (LAB-737 evidence tooling).

Drives the real Publisher against the in-process bytes backend, so the keys
come from the same decorator machinery the live service uses — the 15
interop/v1 aggregate keys, the auto-mode checkpoint key, and the auto-mode
@cache.secure sentiment key. No network, no credentials.

    cd ingester && uv run python ../stage3/derive_keys.py
"""

from __future__ import annotations

import hashlib
import time

from skyline_ingester.backends import MemoryBytesBackend
from skyline_ingester.publisher import Publisher
from skyline_ingester.windows import WINDOW_TTLS, WindowStore


def _placeholder_master_key() -> str:
    """A syntactically valid 64-hex value that unlocks the Publisher's
    secure-cache code path so its key NAME gets derived. Cache-key
    derivation never mixes the master key in, so any value yields the same
    keys. Derived at runtime from a public string precisely so no literal
    in this source is — or could be swapped for — real key material.
    """
    return hashlib.sha256(b"skyline-derive-keys-placeholder-not-a-secret").hexdigest()


def main() -> None:
    backend = MemoryBytesBackend()
    publisher = Publisher(WindowStore(), backend, master_key=_placeholder_master_key(), now_fn=time.time)
    for window in WINDOW_TTLS:
        publisher.publish_window(window)
    publisher.checkpoint()
    for key in sorted(backend.stored_keys()):
        print(key)


if __name__ == "__main__":
    main()
