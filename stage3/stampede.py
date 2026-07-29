"""AC-5 (epic AC-5): the CachekitIO SaaS distributed lock prevents a stampede.

N concurrent invocations hit one cold key; the SaaS lock
(`POST /v1/cache/{key}/lock`) must serialize the recompute so exactly one
executes and the rest are served the winner's value.

The cached function is **async**, and that is load-bearing: cachekit-py's
sync wrapper does not do distributed locking at all ("Sync wrappers don't
support distributed locking (backend protocol is async-only)" —
decorators/wrapper.py). Only the async wrapper takes the
`hasattr(backend, "acquire_lock")` path this AC exercises. Empirically
confirmed on 0.15.0: the sync variant of this probe recorded 12/12
recomputes and zero lock traffic.

Instrumentation, per the AC:
- a recompute counter inside the cached function (must read exactly 1);
- httpx request logging (INFO) — the actual `POST …/lock` / `DELETE …/lock`
  SaaS traffic, straight from the SDK's HTTP client, no mocking.

Design notes (LAB-737 "two design gotchas"):
- the wrapper's lock waiters block up to 5 s, and the lock self-expires at
  30 s — so the recompute sleeps well under 5 s. A slower recompute would
  make a *correct* system report > 1 execution.
- L1 is left enabled deliberately: it cannot mask a stampede on a cold key
  (nothing is in L1 before the first recompute), and the wrapper's
  double-checked locking is the property under test.

Env: CACHEKIT_API_KEY (required), CACHEKIT_API_URL / CACHEKIT_ALLOW_CUSTOM_HOST
for the dev instance.

    op run --env-file=../.op.apikey.env -- \
        uv run python ../stage3/stampede.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time

from cachekit import cache
from cachekit.backends.cachekitio import CachekitIOBackend
from cachekit.interop import generate_interop_key

NAMESPACE = "bluesky-thinking"
OPERATION = "stampede_probe"  # dedicated key: never collides with the five locked aggregates
N = 12
RECOMPUTE_SECONDS = 2.0  # < 5 s blocking_timeout, < 30 s lock_timeout

recomputes = 0


async def main() -> int:
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.INFO)  # the SaaS lock traffic evidence

    backend = CachekitIOBackend()  # env config, explicit instance

    @cache(interop=OPERATION, namespace=NAMESPACE, ttl=60, backend=backend)
    async def stampede_probe(window: str) -> dict:
        global recomputes
        recomputes += 1
        generation = recomputes
        await asyncio.sleep(RECOMPUTE_SECONDS)
        return {"window": window, "generation": generation}

    key = generate_interop_key(NAMESPACE, OPERATION, ["5m"])
    backend.delete(key)  # cold start — the whole point
    print(f"cold key: {key}")

    started = time.perf_counter()
    try:
        results = await asyncio.gather(*(stampede_probe("5m") for _ in range(N)))
    finally:
        backend.delete(key)  # leave the namespace as we found it, even on failure
    elapsed = time.perf_counter() - started

    distinct = {tuple(sorted(r.items())) for r in results}
    print(f"\n{N} concurrent invocations finished in {elapsed:.2f}s")
    print(f"recomputes: {recomputes}")
    print(f"distinct results: {len(distinct)} -> {results[0]}")
    if recomputes != 1:
        print("FAIL: expected exactly one recompute")
        return 1
    if len(distinct) != 1:
        print("FAIL: callers saw different values")
        return 1
    print("PASS: exactly one recompute; all callers served the winner's value")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
