# Skyline aggregate-snapshot history (LAB-1616, decided 2026-08-14)

Skyline's live values are rolling 5m/1h/24h windows: once a window moves on, the
prior state is gone, so nobody can tell whether a topic is rising, fading,
recurring, or merely always large. This document is the design decision for the
durable history store, plus the operating contract (privacy, retention,
deletion, cost, restore) the acceptance criteria require.

## The decision

**Store:** Cloudflare D1 (`skyline-history`, bound to `skyline-edge` as
`HISTORY`). **Writer:** the edge Worker's existing cron — never the ingester.
**Cadence:** hourly snapshots of the `1h` window at each top of hour; daily
snapshots of the `24h` window at each UTC midnight.

### Why the edge writes, not the ingester

The obvious design — the Python ingester persists what it publishes — was
rejected on measured grounds, not taste:

- **Egress is the ingester's binding constraint.** LAB-1894 measured the
  checkpoint alone at ~1.63 GB/day against Render's 5 GB/month bandwidth cap;
  the service has already been suspended once for exhausting it. Any
  ingester-side history write makes that worse. Edge-side capture adds **zero**
  Render egress.
- **Failure isolation comes free.** The AC requires that history failure never
  stops current aggregate publishing. With capture on the edge, the ingester
  does not even know history exists — the property holds by construction, and
  the reverse holds too (a dead ingester just leaves gaps).
- **The aggregates are already on the edge.** The Worker reads the same
  interop/v1 entries it has served since Stage 2; capture is five extra GETs
  per hour on an existing, credentialed path.

### Why D1

| Candidate | Verdict |
| :--- | :--- |
| **Cloudflare D1** | **Chosen.** Native Workers binding, SQL range queries, migrations, Time Travel restore, free tier ~1000× our budget (below). |
| Workers KV | No range queries — a 7d chart is 168 point-reads or a hand-rolled index. Wrong shape. |
| R2 objects | Range query = manifest + N GETs, retention = lifecycle rules per tier. More moving parts for the same rows. |
| Render Postgres | Free instances expire after 30 days, and writes from Render add the exact egress LAB-1894 says we cannot afford. |
| CachekitIO as the store | It is a cache: TTL-bounded, key-value, no range scans. Using it as a durable archive misrepresents the product and the epic's privacy posture. It **does** serve as the response cache (below) — the correct role. |

### Cadence and tiering: capture-time, not post-hoc

Each tier snapshots the rolling window whose span equals the tier period, at
the moment the two align: the `1h` window at each top of hour IS that hour; the
`24h` window at UTC midnight IS that day. This sidesteps downsampling
arithmetic entirely — top-N rankings are not additive, so "merge 24 hourly
top-20s into a daily top-20" would be a lie; snapshotting the ingester's own
24h window is exact. Skew between the bucket boundary and the aggregate's
`generated_at` is bounded by the source TTL (300 s for `1h`, 900 s for `24h`)
and enforced by a staleness guard: anything older becomes a **gap**, never a
mislabeled point.

A 5-minute tier was considered and cut: no consumer exists (a 7d chart at 5m
resolution is 2,016 points), the live 5m window already serves "now", and the
tier can be added later without touching stored data. YAGNI.

The capture rides the existing `*/10` keep-alive cron (`captureTick` no-ops
except at minute 0) because Workers Free allows **five cron expressions per
account**, shared across every Worker on it — a new schedule would spend 20 %
of that budget to fire the same code.

## Data model

One row per (operation, tier, bucket) in the `snapshots` table
([`edge/migrations/0001_history_snapshots.sql`](../edge/migrations/0001_history_snapshots.sql)):
`bucket_ts` (UTC epoch seconds, end of bucket), `generated_at`,
`normalization_version` (the [signal-policy](signal-policy.md) semantics
version, stored per row and surfaced per point so comparisons across a policy
change are explicit, never silent), and `payload` — the aggregate JSON rebuilt
through an **allowlist** (window, totals, exclusion counts, ranked lists
trimmed to top-20). Unknown keys cannot reach storage; a payload over 32 KiB
is rejected as a gap. Payload-shape versioning is the migration history
itself — a `schema_version` column ships with the migration that first needs
one, not before.

The staleness guard is **symmetric**: a bucket label tolerates `generated_at`
skew of up to one source TTL on either side. Older means the entry should
already have expired; newer means the cron tick was delivered later than one
TTL past the boundary. Both become gaps — a tighter "newer" bound would turn
routine cron delivery lag into five false gaps on a healthy pipeline. Capture
also runs **concurrently** with the keep-alive ping in the shared scheduled
handler, so a slow Render cold-start (90 s timeout) cannot eat the skew
budget.

**Idempotency is the primary key.** Capture uses `INSERT OR IGNORE` on
(operation, tier, bucket_ts): a re-fired cron, an isolate retry, or any
duplicate publish is a counted no-op and the first write wins. Ingester
restarts are irrelevant to history correctness — the ingester doesn't write.

**Gaps are gaps.** A missed cron fire, an expired aggregate, a stale or
malformed payload each produce an absent row plus a structured
`history_gap` log line. Nothing backfills, nothing zero-fills; range responses
report `expected_points` vs `present_points` and the UI says when history
began and when coverage is incomplete. History is forward-only from first
deploy.

## Serving: `/api/history/*`

- `GET /api/history/{operation}?range=7d|30d` — bounded by construction:
  7d → ≤168 hourly points, 30d → ≤30 daily points, ranked lists at their
  stored top-20 depth. Stable `bucket_ts` ascending order. Coverage block +
  per-point and distinct `normalization_versions`. (A `top` trim parameter
  was considered and cut — no consumer needed it, and every extra dimension
  multiplies response-cache key cardinality.)
- `GET /api/history/status` — capture health derived from the data itself:
  per-tier row counts, newest bucket, and a `stale` flag (newest bucket older
  than two periods). This is deliberately separate from live freshness: the
  live endpoints can be healthy while capture is broken, and vice versa.

**Response caching is CacheKit-backed (dogfood):** a computed range response
is written to CachekitIO keyed on (operation, range, current bucket) — a
closed set of 10 keys per bucket — with TTL = one tier period, so within a
bucket the planet shares one D1 computation; `x-history-source:
cachekit|d1|d1-fallback` says which path served. Two honesty rules apply:

- **Cached bytes are distrusted like any other backend read** (the backend is
  operator-writable): empty, oversized (>4 MiB), or unparseable values are
  treated as a miss, recomputed from D1, and overwritten — never relayed into
  the POP cache.
- **Only a complete series is cached**: if the newest expected bucket hasn't
  landed yet (late tick), the response still serves but is not frozen into
  CachekitIO for a period — recomputation stays bounded by the 15 s POP cache
  until the point exists.

D1 is the source of truth — any CachekitIO failure (or an unset
`CACHEKIT_API_KEY`) falls through to plain D1 serving, the inverse of the
live endpoints where CachekitIO *is* the truth. The existing POP cache (15 s)
fronts everything as before. Live endpoints are untouched: `/api/{operation}`
and `/api/stats` behave byte-for-byte as before.

## Privacy

A history row contains exactly what the public `/api/{operation}` endpoints
already serve — normalized ranked values, totals, exclusion counts — trimmed
to top-20 and filtered through an allowlist. **No post text, no author DID, no
record key, no raw event payload** — those never reach the edge in the first
place (the ingester's aggregates are already DID-free, see signal-policy.md).
The `@cache.secure` sentiment cache is excluded from history entirely: it is
zero-knowledge ciphertext, and persisting any derivative would cross the
boundary LAB-744 established. What history changes is **time**: a trending tag
that was public for an hour is now public for up to the retention horizon.
That is the feature, applied to data already published under the signal
policy's safety filters.

## Retention and deletion

- Hourly tier: **35 days** (7d queries need 7; the margin serves the upcoming
  rising/velocity work, LAB-1619). Daily tier: **400 days** (30d queries plus
  year-over-year headroom).
- Enforced by the daily cron sweep (`DELETE … WHERE bucket_ts < horizon`),
  covered by tests — not by hope.
- Manual deletion (a tag/link that must go earlier than its horizon):
  `npx wrangler d1 execute skyline-history --remote --command "DELETE FROM snapshots WHERE …"`,
  then purge the response-cache layer by waiting out the ≤1h/≤24h CachekitIO
  TTL (or deleting the `bluesky-thinking:history_response:*` keys) and the
  15 s POP TTL. Because rows hold only aggregate rankings, removal targets a
  value inside `payload` (SQLite `json_remove`/`REPLACE`) or the whole bucket
  row — documented judgment call, expected to be rare.

## Cost budget (measured against the free tier, 2026-08-14)

D1 free tier: 5 GB storage (account-wide), 100k rows written/day, 5M rows
read/day. Cloudflare docs, verified 2026-08-14.

| Budget | Skyline's use | Headroom |
| :--- | :--- | ---: |
| Rows written/day | 120 hourly + 5 daily + ≤125 retention deletes ≈ **250** | ~400× |
| Rows read/day | ≤168/query, ≥1h response reuse via CacheKit + POP caching; even 10k uncached queries/day ≈ 1.7M | ~3× worst-case, ~10³× expected |
| Storage | rows ≤6 KiB (top-20 trim + 32 KiB hard cap); 4,200 hourly + 2,000 daily rows ≈ **≤40 MiB** steady state | ~100× within Skyline's share |
| Cron slots (5/account) | **0 new** — rides the existing keep-alive schedule | — |
| CachekitIO ops | 5 GETs/hour capture + ≤10 response-cache entries/bucket | dogfood, our own tenant |

The 7d/30d row math: 7d = 168 hourly rows/operation (840 total), 30d = 30
daily rows/operation (150 total) — both served whole in one bounded query.

## Restore and disaster behavior

- **D1 Time Travel** is always on: point-in-time restore to any minute in the
  last 7 days (free plan) via
  `npx wrangler d1 time-travel restore skyline-history --timestamp=…`.
  Restore overwrites in place and returns an undo bookmark.
- A lost or restored-with-holes database is **not** reconstructed: rolling
  windows cannot be replayed (that would require the raw firehose archive this
  design explicitly refuses to keep). Holes become visible coverage gaps, the
  same honest shape as any other outage.
- The response-cache layer needs no restore handling — entries expire within
  one tier period and rebuild from D1.

## Runbook (one-time provisioning — already done for prod)

```bash
cd edge
npx wrangler d1 create skyline-history          # done 2026-08-14 → id in wrangler.toml
npx wrangler d1 migrations apply skyline-history --remote
npx wrangler deploy                              # picks up the HISTORY binding
```

Migrations must be applied **before** deploying a Worker that expects the new
schema. Capture health after deploy: `GET /api/history/status` (hourly tier
populates at the next top of hour; daily at the next UTC midnight —
forward-only, so the first 7d chart fills over its first week).
