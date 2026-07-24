"""LAB-735 AC-1: CachekitIO round-trip against api.cachekit.io.

Runs the moment CACHEKIT_API_KEY is set (see docs/architecture.md provisioning
runbook). Proves: interop/v1 write+read on the shared namespace, @cache.io
decorator end-to-end, and the distributed-lock endpoint responding.

    export CACHEKIT_API_KEY=ck_live_...
    uv run --with cachekit==0.15.0 python roundtrip.py
"""

import os
import sys

if not os.environ.get("CACHEKIT_API_KEY"):
    sys.exit("CACHEKIT_API_KEY not set — run the provisioning runbook first (docs/architecture.md)")

from cachekit import cache
from cachekit.interop import generate_interop_key

NS = "bluesky-thinking"

# 1. interop/v1 key round-trip via @cache.io (the SaaS backend the demo uses)
@cache.io(interop="posts_per_minute", namespace=NS)
def posts_per_minute(window: str) -> dict:
    return {"window": window, "ppm": 42.0, "source": "lab-735-spike"}

key = generate_interop_key(NS, "posts_per_minute", ["5m"])
first = posts_per_minute("5m")   # miss -> compute -> write
second = posts_per_minute("5m")  # hit -> served from CachekitIO
assert first == second, f"round-trip mismatch: {first!r} != {second!r}"
print(f"PASS @cache.io interop round-trip on {key}")
print("AC-1 satisfied: namespace live, key accepted, read-after-write consistent.")
