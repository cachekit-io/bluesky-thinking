# Skyline ingester (Stage 2, Python — LAB-744)

Consumes the [Bluesky Jetstream](https://github.com/bluesky-social/jetstream), maintains 5m/1h/24h
sliding windows in minute buckets, and publishes the five locked analytics aggregates to CacheKit
under the **interop/v1** contract in [`../docs/architecture.md`](../docs/architecture.md) — keys are
byte-identical to what the TS edge API and Rust-WASM hot path read.

## Run

```bash
cd ingester
uv sync

# Live: writes real CachekitIO entries (key from the provisioning runbook)
CACHEKIT_API_KEY=ck_live_... uv run skyline-ingester

# Dry-run: no key -> same pipeline, in-process backend, every write logged
uv run skyline-ingester
```

Configuration (env or `.env`, via pydantic-settings; secrets are `SecretStr`):

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `CACHEKIT_API_KEY` | unset | CachekitIO key. Unset → dry-run mode. |
| `CACHEKIT_MASTER_KEY` | unset | 64-hex master key for the `@cache.secure` sentiment cache. **Required in live mode** (fail closed — a live deploy without it refuses to start); unset in dry-run → secure cache disabled with a warning. |
| `JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream endpoint. |
| `PUBLISH_TICK_SECONDS` | `15` | Publish-loop poll interval. |
| `CHECKPOINT_INTERVAL_SECONDS` | `120` | Window-state checkpoint cadence. |
| `TOP_N` | `50` | Entries kept in trending lists. |

## What it publishes

Each window republishes at TTL/2 (locked TTLs: 5m→60 s, 1h→300 s, 24h→900 s), so readers always
hit. cachekit is decorator-only, so a publish is `invalidate_cache(window)` + call — the miss
recomputes from the in-memory windows and writes fresh bytes. The recompute is probed *before*
invalidating so a compute failure never deletes a live key; a backend **write** failure after the
invalidate can still leave the key briefly deleted until the next tick — cachekit has no atomic
set/replace, so that gap is inherent to the decorator API.

Values are interop/v1 plain MessagePack, top-level maps with string keys. All carry
`window` (str), `generated_at` (unix seconds, int), `total_posts` (int), plus:

| Operation | Payload field |
| :--- | :--- |
| `trending_hashtags` | `hashtags`: `[{tag, count}]`, top 50 |
| `trending_links` | `links`: `[{uri, count}]`, top 50 (link facets + external embeds) |
| `lang_mix` | `langs`: `{lang: share}` (floats summing to ~1; top 25 + `other`) |
| `posts_per_minute` | `ppm`: float |
| `top_emoji` | `emoji`: `[{emoji, count}]`, top 25 (ZWJ sequences count once) |

### Secure cache (AC-6 groundwork)

`language_sentiment(window="1h")` — per-language lexicon sentiment `{lang: {avg, n}}` — is written
via `@cache.secure(master_key=…)` auto mode, `namespace="bluesky-thinking"`. Its key is the
Python-only 7-segment auto key (`ns:bluesky-thinking:func:…`), and the backend stores ciphertext
only (asserted in tests). Zero-knowledge holds end-to-end: the sentiment value is encrypted here and
its plaintext source is never written to any other key (the checkpoint omits it — see below), so the
backend never sees it in the clear. Ciphertext-only verification against the live SaaS is Stage 3.

### Checkpointing

Window state is checkpointed into CacheKit (auto-mode key, TTL 26 h) every
`CHECKPOINT_INTERVAL_SECONDS` and restored on startup, so a process restart doesn't zero the 24h
window (the spec's Render-restart mitigation). Per-minute counters are truncated to their top-K
entries in the snapshot — long-tail trending counts are approximate after a restore;
`posts_per_minute` and `lang_mix` stay exact.

The checkpoint is stored **unencrypted**, so it deliberately omits the per-language sentiment
totals: those are the cleartext source of the `@cache.secure` value, and persisting them in the
plaintext checkpoint would let the backend reconstruct it (`avg = sum / count`), breaking the
zero-knowledge property. Sentiment is not restart-critical — the secure 1h window repopulates within
an hour of a restart; the aggregate counts above are unaffected.

## Privacy

Aggregate-only: the extractor reduces each post to counter inputs (tags, links, primary language,
emoji, a lexicon sentiment score). Post text, author DIDs, and rkeys are never stored — not in the
windows, not in the checkpoint, not in any cache value.

## Tests

```bash
uv run pytest -q        # no network, no CACHEKIT_API_KEY needed
uv run ruff check src tests && uv run ruff format --check src tests
```

The suite drives the real SDK against an in-process bytes backend (interop mode enforces the
cross-SDK value contract, so `backend=None`/L1-only is rejected by cachekit itself) and asserts the
byte-locked key vectors from the architecture spec, aggregate correctness from a recorded fixture
stream, window expiry, checkpoint restore, and ciphertext-only secure storage.
