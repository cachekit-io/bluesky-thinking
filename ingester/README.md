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
| `PORT` | `8080` | `/health` listener port (the k3s liveness probe targets it). |
| `PUBLISH_TICK_SECONDS` | `15` | Publish-loop poll interval. |
| `CHECKPOINT_INTERVAL_SECONDS` | `300` | Window-state checkpoint cadence — also the restart staleness bound. 300 s was originally sized to fit Render's 5 GB/month egress allowance (see *Checkpointing*); the lab cluster has no egress cap, but the cadence stays — nothing needs it tighter. |
| `TOP_N` | `50` | Entries kept in trending lists. |

## Health endpoint (Stage 4, LAB-738)

The ingester's whole HTTP surface is `GET /health` on `$PORT` (it originated as a Render
free-tier web-service requirement; today it is the k3s liveness probe's target). Liveness only,
no aggregate data, no key material:

```json
{"status": "ok", "jetstream_connected": true, "events_seen": 12345,
 "events_missing_source": 0,
 "last_event_age_seconds": 0.4, "last_publish_age_seconds": 7.1, "uptime_seconds": 900.0,
 "rss_mib": 61.4, "rss_peak_mib": 74.2,
 "buckets": 1440, "counter_keys": 217565, "ledger_entries": 11976}
```

Returns **503** whenever the Jetstream socket is down, so a dead consumer inside a live process is
visible from outside — the k3s `livenessProbe` turns a sustained 503 (~3 min) into a container
restart, and the CacheKit checkpoint makes that restart safe. Deployment manifests and runbook:
[`../deploy/k3s/`](../deploy/k3s/).

The last five fields are memory diagnostics (LAB-1775). They are **sizes, never contents** — a
count of live counter keys, not the keys — so the endpoint stays liveness-only. `rss_mib` is the
current resident set (`/proc/self/statm`, `null` off Linux) and `rss_peak_mib` the high-water mark
(`resource.getrusage`); both are stdlib, no new dependency. The two come from different kernel
accounting paths and `ru_maxrss` updates lazily, so `rss_mib` can read a little *above*
`rss_peak_mib` — that is expected, not a bug. They exist because the host's memory graph
(Render's dashboard then, the lab cluster now) sits behind access no agent has, so an OOM
recurrence has to be diagnosable from the endpoint alone: `counter_keys` climbing without bound
is the signature of the LAB-1775 regression returning.

## Window retention and memory

The store keeps one counter bucket per minute for 24 h. Resident cost is therefore
*(retained minutes) × (distinct keys per minute)*, and at observed firehose rates a minute carries
~2,470 distinct keys — so 1,440 full-fidelity minutes projected to **~1,050 MiB**, against the
512 MiB of the Render free plan. That is the OOM restart LAB-1775 chased down.

A bucket keeps every distinct key while it is inside the **full-fidelity horizon** — the 5 m
window plus 5 minutes of slack, 10 minutes in total. Once it ages past that it is truncated in
place to its top-K entries (20 tags / 20 links / 20 domains / 10 emoji / 32 languages, one display
spelling per surviving tag) and is re-truncated if it ever regrows.

The slack is load-bearing, not padding. `_prune` anchors the compaction floor on the minute being
*added* (deliberately — one far-future timestamp must never become a permanent retention anchor),
and `jetstream.ingest_raw` accepts events up to `MAX_FUTURE_SKEW_SECONDS` (300 s) ahead. Without
slack, a single accepted future-dated post dragged the floor into the live window and truncated
it — measured 1,000 → 100 distinct tags in the 5 m window from one `+300 s` frame, repeatable
every minute. `test_windows.py` pins slack ≥ the accepted skew.

Consequences worth knowing:

- **The 5 m window is bit-exact.** Only the 1 h and 24 h *long tails* are approximate — the same
  approximation the CacheKit checkpoint has always shipped for restore.
- **Retained counts are exact.** Truncation drops keys; it never rewrites a count.
- **`posts_per_minute` and the exclusion denominators are exact in every window.** Compaction
  never touches `n`, `signal_candidates` or `excluded`.
- Truncation is **frequency-ordered**, not arrival-ordered, so it keeps what was actually
  trending in that minute. Measured on a Zipf-distributed hour against an uncompacted control:
  top-25 *membership* is unchanged and top-10 *ordering* is preserved. Counts are exact for the
  heaviest tags and degrade gradually down the ranking — ranks 1–6 exact, rank 10 at 97.5 %,
  median 92 % across the top 25, worst 55 %. 47 of the top 50 survive; a tag averaging under
  about one occurrence per minute never makes a minute's top-K and can drop out entirely.
  Trend *ranking* is what this preserves; per-key totals in the 1 h and 24 h windows are a
  lower bound, not a census.
- **`lang_mix` shares are renormalized over the languages a bucket retains.** Below 32 distinct
  languages per minute — every minute at observed rates, which carry 23–27 — that is a no-op;
  above it, 1 h and 24 h shares describe the retained set, not all posts. `total_posts` stays
  exact regardless.

Measured with [`tools/soak_memory.py`](tools/soak_memory.py) against the live public Jetstream —
no credentials needed, it drives `extract` → `WindowStore` directly:

```bash
uv run python tools/soak_memory.py live --seconds 540      # RSS + cardinality vs the real firehose
uv run python tools/soak_memory.py saturate --minutes 1440 # a full 24h window, synthetically filled
```

At a full 1,440-bucket window and observed live cardinality that is **41.6 MiB steady / 48.0 MiB
peak** including the `merged()` transient, versus **438.5 MiB / 634.2 MiB** with compaction
disabled (`--no-compaction`) — the latter over the 512 MiB limit on retained structure alone.

> **On-cluster reality check (LAB-2586).** The soak figures above measure retained structure plus
> one merge transient, single-threaded, over minutes. On the lab k3s deployment the container
> working set is larger and grows for the whole first 24 h: live counter keys match the model
> (159k keys ≈ 45 MiB at the measured ~299 B/key, at hour 14.5, via `/health`), but each publish
> tick's full-window copy + merge fold runs in an `asyncio.to_thread` worker, and glibc keeps each
> worker heap at its transient high-water — measured ~2.4× live window state, a ratchet that
> plateaus only once the window stops growing (~260–310 MiB projected at 24 h+, LAB-2586 probes).
> [`deploy/k3s/skyline-ingester.yaml`](../deploy/k3s/skyline-ingester.yaml) sizes for that
> on-cluster reality, not for these single-threaded soak numbers.

Worst case, measured: a quiet tail then a burst into the uncompacted head, which is when the
contribution ledger is empty and a single minute can draw on the whole of it —

```bash
uv run python tools/soak_memory.py saturate --minutes 1440 --per-bucket 200 --head-per-bucket 40000
```

— is **110.8 MiB steady / 140.9 MiB** at the 24 h merge peak, with the ledger refusing 1,000,000
offered contributions. (An earlier pass published 71.8 MiB for this case. That number was low
twice over: it counted one retained key per accepted contribution when a hashtag mints two, `tags`
and `tag_labels`, and it treated the ledger's ~20,000/min *sustained* rate as a per-minute ceiling.)
**While event time advances**, `MAX_SOURCE_LEDGER_ENTRIES` over `SOURCE_DEDUPE_SECONDS` bounds what
the head can accept, so it needs no cap of its own; `test_windows.py` derives both the aged and the
head budget from those constants (driving the head one through `add()`), so raising the ledger cap
fails a test. That precondition is the firehose's own contract and what the measurement above
assumes — it is not enforced.

**One axis is still unbounded.** A feed that *stalls* event time while still delivering volume keeps
one head bucket permanently inside the full-fidelity horizon: ledger entries expire on monotonic
time and free capacity for new keys, while nothing ages the bucket out. It accumulates at roughly
27k keys/min, without bound. `ingest_raw` bounds event time from above but accepts any past
timestamp, so this is reachable from a broken or hostile feed, not from a healthy Jetstream. The
bucket-count cap does not help — it bounds how many buckets exist, not how many keys one head
bucket accumulates. Closing it needs a head admission cap with explicit at-cap semantics.
`counter_keys` on `/health` climbing without bound is its signature.

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
| `lang_mix` | `langs`: `{lang: share}`, top 25 real tokens only; plus `other_share` (float, sibling key, present only if there's a long tail) — floats sum to ~1 |
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

The checkpoint is also the ingester's dominant **egress** path: LAB-1894 measured it at ~97 % of
outbound bandwidth — ~2.3 MB wire × 720 writes/day ≈ 49 GB/month at per-minute snapshots on a
120 s cadence, which is what exhausted Render's 5 GB/month free allowance and suspended the
workspace (2026-08-13). Two changes fit it back inside (LAB-1933): buckets older than the 1 h
window are **hour-coarsened** at snapshot time — each aged hour folds into one unit keyed at the
hour's first minute, so a full 24 h window serializes as ~85 units instead of ~1,445 — and the
default cadence stretched from 120 s to 300 s. Measured with the audit's soak methodology (600 s
live Jetstream through the real pipeline; wire bytes are what `CachekitIOBackend` PUTs, LZ4 ratio
×0.682 measured on real soak data, within 0.6 % of the audit's ×0.686): **~159 KB/write ×
288 writes/day ≈ 46 MB/day ≈ ~1.4 GB/month of checkpoint egress**, down from ~49 GB/month.

Total egress is that plus the aggregate-publish path, which this change does not touch (the
15 s `publish_tick_seconds` tick, ~5,760 uncompressed-msgpack writes/day). That component was
not re-measured here; the audit put the checkpoint at ~97 % of a ~50 GB/month total, which
leaves **~1.2–1.5 GB/month** for everything else. So:

| Egress component | Per month | Source |
| :--- | :--- | :--- |
| Checkpoint (after this change) | ~1.4 GB | measured, this PR |
| Aggregate publish + overhead | ~1.2–1.5 GB | audit residual, unchanged by this PR |
| **Total** | **~2.6–2.9 GB** | **~52–58 % of the 5 GB cap, ~1.7–1.9× headroom** |

At a zero-compression ceiling the checkpoint term becomes ~2.0 GB/month, for ~3.2–3.5 GB/month
total — still inside the cap. Honest delta against the audit's ~2.4 GB/month projection for the
total: firehose volume at measurement time ran ~10 % heavier than the audit's, and the residual
above is a derived range rather than a fresh measurement. Re-measuring the publish path is the
obvious next tightening if the cap ever gets close. The TTL stays 26 h: the checkpoint still
covers a full 24 h window and must outlive it plus restart slack — the cadence change doesn't
alter that.

Recovery semantics after the restructure, stated precisely:

- **Restart staleness** is bounded by the cadence: at most 300 s of window state is lost.
- **5 m and 1 h windows restore minute-exact** — the live hour stays minute-keyed.
- **The 24 h trailing edge expires in hour steps after a restore.** An hour unit is keyed at its
  hour *floor*, so up to 59 min of true tail events can drop early (≤ ~4 % of a 24 h count) — and
  an event older than 24 h is never re-served as in-window. Early, never late.
- **Totals stay exact within surviving units** (aggregates only ever sum buckets); per-key
  fidelity is the same top-K approximation the checkpoint has always shipped, applied per aged
  hour instead of per aged minute. The wire format is unchanged (still schema v2), so a new
  binary restores an old fat checkpoint and vice versa across a deploy.

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
accepted entries written by `snapshot()` and considers at most one 24-hour window of minute
buckets. Each checkpoint map is scanned completely up to 1,024 entries; a larger map rejects its
whole bucket instead of silently restoring a partial counter. An oversized operator-poisoned map
therefore cannot displace valid history behind an invalid prefix or publish a healthy-looking
partial minute.

Checkpoint schema v2 is tied to `skyline-normalization-v1`. A checkpoint from
an older normalization version is rejected instead of mixing incompatible
ranking keys under a new version label. Canonical domains, display-label counts,
and aggregate exclusion counts are restart-safe; the transient source ledger is
not.

## Privacy

Aggregate-only: the extractor reduces each post to normalized counter inputs
(tags, links/domains, primary language, emoji, a lexicon sentiment score) and
aggregate exclusion reasons. Post text and record keys are never stored.

For public tags, URLs, domains, and emoji, one source contributes a given
canonical value at most once per rolling five minutes. The raw DID crosses one
local call boundary, is immediately folded into a process-keyed tuple digest,
and is never stored or logged. The random key and opaque five-minute ledger are
excluded from buckets, checkpoints, cache values, and history, and rotate on
restart. The ledger holds at most 1,024 tuples per source and 100,000 globally.
Both ceilings refuse rather than evict: a source at its own ceiling has further
contributions refused (reported as `rate_limited_source_*`), and global
pressure from many distinct sources refuses new contributions (reported as
`rate_limited_global_*`) instead of evicting the globally oldest tuple — a
live-tuple eviction would re-credit an already-counted signal and refill its
source's budget. Only genuinely expired entries free capacity. Full canonicalization, safety,
filter-list, tracking-parameter, and transparency
semantics: [public signal policy](../docs/signal-policy.md).

After a reconnect, a backlog delivered faster than real time shares the current
process-time source bound and can under-count trend signals; volume aggregates
remain exact, and language aggregates remain exact in the 5 m window and for any
minute that carried at most 32 distinct languages (every minute, at observed
rates — see *Window retention and memory* for the compaction bound). Event
timestamps never expire the privacy ledger because they are untrusted.

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
