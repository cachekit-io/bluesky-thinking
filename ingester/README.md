# Skyline ingester (Stage 2, Python — LAB-744)

Consumes the [Bluesky Jetstream](https://github.com/bluesky-social/jetstream), maintains 5m/1h/24h
sliding windows in minute buckets, and publishes the five locked analytics aggregates to CacheKit
under the **interop/v1** contract in [`../docs/architecture.md`](../docs/architecture.md) — keys are
byte-identical to what the TS edge API and Rust-WASM hot path read.

## Run

```bash
cd ingester
uv sync

# Live: writes real CachekitIO entries. Creds per docs/architecture.md#credentials:
CACHEKIT_API_URL=https://api.dev.cachekit.io CACHEKIT_ALLOW_CUSTOM_HOST=true \
    op run --env-file=../.op.env -- uv run skyline-ingester

# Dry-run: no key -> same pipeline, in-process backend, every write logged
uv run skyline-ingester
```

Configuration (env or `.env`, via pydantic-settings; secrets are `SecretStr`).
In live mode the backend itself is built by the SDK's env-config path, so the
`CACHEKIT_*` backend variables must be real process env vars (`op run`
provides that) — the SDK does not read this service's `.env` file:

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `CACHEKIT_API_KEY` | unset | CachekitIO key. Unset → dry-run mode. |
| `CACHEKIT_MASTER_KEY` | unset | 64-hex master key for the `@cache.secure` sentiment cache. **Required in live mode** (fail closed — a live deploy without it refuses to start); unset in dry-run → secure cache disabled with a warning. |
| `CACHEKIT_API_URL` | `https://api.cachekit.io` | Backend endpoint (the demo uses the dev instance, `https://api.dev.cachekit.io`). |
| `CACHEKIT_ALLOW_CUSTOM_HOST` | unset | Required `true` for the dev instance — its hostname is outside the SDK's SSRF allowlist. |
| `JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream endpoint. |
| `PORT` | `8080` | `/health` listener port (Render injects this on deploy). |
| `PUBLISH_TICK_SECONDS` | `15` | Publish-loop poll interval. |
| `CHECKPOINT_INTERVAL_SECONDS` | `120` | Window-state checkpoint cadence. |
| `TOP_N` | `50` | Entries kept in trending lists. |

## Health endpoint (Stage 4, LAB-738)

The ingester's whole HTTP surface is `GET /health` on `$PORT` — it exists because Render's free
tier only hosts *web services*, which must answer HTTP, and because the keep-alive cron
(`edge/wrangler.toml [triggers]`) needs something to ping. Liveness only, no aggregate data, no
key material:

```json
{"status": "ok", "jetstream_connected": true, "events_seen": 12345,
 "events_missing_source": 0,
 "last_event_age_seconds": 0.4, "last_publish_age_seconds": 7.1, "uptime_seconds": 900.0}
```

Returns **503** whenever the Jetstream socket is down, so a dead consumer inside a live process is
visible from outside — Render's health check then restarts the service, and the CacheKit
checkpoint makes that restart safe. Deployment blueprint: [`../render.yaml`](../render.yaml).

## What it publishes

Each window republishes at TTL/2 (locked TTLs: 5m→60 s, 1h→300 s, 24h→900 s), so readers always
hit. cachekit is decorator-only, so a publish is `invalidate_cache(window)` + call — the miss
recomputes from the in-memory windows and writes fresh bytes. The recompute is probed *before*
invalidating so a compute failure never deletes a live key; a backend **write** failure after the
invalidate can still leave the key briefly deleted until the next tick — cachekit has no atomic
set/replace, so that gap is inherent to the decorator API.

Values are interop/v1 plain MessagePack, top-level maps with string keys. All carry
`window` (str), `generated_at` (unix seconds, int), `total_posts` (int),
`total_events_considered` (int), `total_signal_candidates` (int),
`excluded_count_by_reason` (map), and `normalization_version` (str), plus:

| Operation | Payload field |
| :--- | :--- |
| `trending_hashtags` | `hashtags`: `[{tag, display, count}]`, top 50; `tag` remains canonical and `display` preserves the most frequent spelling |
| `trending_links` | `links`: `[{uri, count}]` and `domains`: `[{domain, count}]`, top 50 |
| `lang_mix` | `langs`: `{lang: share}` (floats summing to ~1; top 25 + `other`) |
| `posts_per_minute` | `ppm`: float |
| `top_emoji` | `emoji`: `[{emoji, count}]`, top 25 (ZWJ sequences count once) |

### Secure cache (AC-6 groundwork)

`language_sentiment(window="1h")` — per-language lexicon sentiment `{lang: {avg, n}}` — is written
via `@cache.secure(master_key=…)` auto mode, `namespace="bluesky-thinking"`. Its key is the
Python-only 7-segment auto key (`ns:bluesky-thinking:func:…`), and the backend stores ciphertext
only (asserted in tests). Zero-knowledge holds end-to-end: the sentiment value is encrypted here and
its plaintext source is never written to any other key (the checkpoint omits it — see below), so the
backend never sees it in the clear. Its secure value contains only the window, generation time,
normalization version, and live per-language sentiment; public transparency counters derived from
the operator-writable checkpoint are deliberately excluded. Ciphertext-only verification against
the live SaaS is Stage 3.

### Checkpointing

Window state is checkpointed into CacheKit (auto-mode key, TTL 26 h) every
`CHECKPOINT_INTERVAL_SECONDS` and restored on startup, so a process restart doesn't zero the 24h
window (the spec's Render-restart mitigation). Per-minute counters are truncated to their top-K
entries in the snapshot — long-tail trending, language, and emoji counts are approximate after a
restore; `posts_per_minute` and `total_signal_candidates` stay exact.

The checkpoint is stored **unencrypted**, so it deliberately omits the per-language sentiment
totals: those are the cleartext source of the `@cache.secure` value, and persisting them in the
plaintext checkpoint would let the backend reconstruct it (`avg = sum / count`), breaking the
zero-knowledge property. Sentiment is not restart-critical — the secure 1h window repopulates within
an hour of a restart; the aggregate counts above are unaffected.

The checkpoint is equally **untrusted on read-back** (a backend operator can poison it): `restore()`
validates every entry, dropping unsafe counter keys and values individually instead of erasing the
rest of their minute or crashing startup,
and ignores any legacy `sent` field entirely — restoring it would let a poisoned checkpoint choose
the plaintext that the next secure publish encrypts. Restore keeps the same per-counter top-K
accepted entries written by `snapshot()`, examines at most 512 additional rejected raw entries
per map, charges any unexamined remainder, and considers at most one 24-hour window of minute
buckets. An oversized operator-poisoned map therefore cannot turn startup into a memory or CPU
boot loop while a bounded invalid prefix still cannot displace valid history.

Checkpoint schema v2 is tied to `skyline-normalization-v1`. A checkpoint from
an older normalization version is rejected instead of mixing incompatible
ranking keys under a new version label. Canonical domains, display-label counts,
and aggregate exclusion counts are restart-safe; the transient source ledger is
not.

## Privacy

Aggregate-only: the extractor reduces each post to normalized counter inputs
(tags, links/domains, primary language, emoji, a lexicon sentiment score) and
aggregate exclusion reasons. Post text and record keys are never stored.

For public tags, URLs, and domains, one source contributes a given canonical
value at most once per rolling five minutes. The raw DID crosses one local call
boundary, is immediately folded into a process-keyed tuple digest, and is never
stored or logged. The random key and opaque five-minute ledger are excluded from
buckets, checkpoints, cache values, and history, and rotate on restart. The
ledger holds at most 100,000 tuples; a fast replay that fills it evicts the
oldest-expiring tuple, weakening the bound temporarily instead of risking OOM.
Full canonicalization, safety, filter-list, tracking-parameter, and transparency
semantics: [public signal policy](../docs/signal-policy.md).

After a reconnect, a backlog delivered faster than real time shares the current
process-time source bound and can under-count trend signals; volume,
language, and emoji aggregates remain exact. Event timestamps never expire the
privacy ledger because they are untrusted.

## Tests

```bash
uv run pytest -q        # no network, no CACHEKIT_API_KEY needed
uv run ruff check src tests && uv run ruff format --check src tests
```

The suite drives the real SDK against an in-process bytes backend (interop mode enforces the
cross-SDK value contract, so `backend=None`/L1-only is rejected by cachekit itself) and asserts the
byte-locked key vectors from the architecture spec, aggregate correctness from a recorded fixture
stream, window expiry, checkpoint restore, ciphertext-only secure storage, and
the recorded signal-quality before/after evaluation.
