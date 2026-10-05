# Skyline edge API + dashboard

TypeScript Cloudflare Worker serving the five Skyline aggregates from the shared
CachekitIO namespace via [interop/v1](../docs/architecture.md#locked-key-convention)
reads (`@cachekit-io/cachekit` 0.1.5), plus a static dashboard on Workers Assets.
For the live aggregates the edge is **read-only**: they are computed and written
by the Python ingester; a cache miss here is surfaced (404 + `X-Cache: MISS`),
never recomputed or faked. The edge's only writes are its own derived state:
hourly/daily snapshot rows into the `HISTORY` D1 store, CacheKit-cached
history responses (below), and short-lived aggregate responses in the
Cloudflare POP cache (`caches.default`, see API) — never the interop aggregate
keys.

## Dashboard

The dashboard is a live Bluesky network pulse; CacheKit's cross-SDK namespace is
the supporting implementation example, not the signal being measured. It renders
the five documented ingester payloads explicitly, so metadata such as
`generated_at` and `total_posts` is never mistaken for a ranking. Each card shows
its selected 5m / 1h / 24h window, rank and proportional bar where applicable,
and the posts/minute card also shows its sample size. The selection is retained in
the `?window=` URL parameter.

A history panel charts posts-per-minute over 7d/30d from
`/api/history`: one amber bar per present bucket, absent buckets rendered as
holes, with a coverage line stating when history began and how many points the
range actually holds ("missing points are gaps in collection, not zero
activity"), plus a `<details>` data table.

`generated_at` is Unix seconds and is rendered as both localized text and an
accessible ISO timestamp. Staleness follows the ingester publishing cadence: 5m
payloads warn after **1 minute**, 1h after **5 minutes**, and 24h after **15
minutes** (the respective aggregate TTLs). Empty aggregates, cache misses,
backend/decode failures, absent cache proof, and temporarily unavailable hot-path
verification have separate dashboard copy.

## API

| Route                                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                  |
| :------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /api/{operation}?window={window}`       | Cached aggregate. `operation` ∈ `trending_hashtags` · `trending_links` · `lang_mix` · `posts_per_minute` · `top_emoji`; `window` ∈ `5m` · `1h` · `24h` (required — interop binding rules forbid default parameters).                                                                                                                                                                                                         |
| `GET /api/stats`                             | Per-isolate `hits` / `misses` / `errors` / `hit_rate` + a `scope` string restating this: counters reset when Cloudflare recycles the isolate, and `hit_rate` is aggregate-key availability at this isolate — not an SDK L1 rate, and not the end-user rate (POP cache hits are served before the worker runs). Sent with `cache-control: no-store` — live counters are never replayed from a browser or intermediary cache.  |
| `GET /api/history/{operation}?range={range}` | Snapshot history from D1: `range` ∈ `7d` (hourly points) · `30d` (daily points). Bounded, ascending series with a coverage block — absent buckets are gaps, never zeros. `x-history-source: cachekit\|d1\|d1-fallback` names the serving layer (responses are CacheKit-cached per bucket; cached bytes are validated before relay and only complete series are cached). Served from D1 alone if `CACHEKIT_API_KEY` is unset. |
| `GET /api/history/status`                    | Capture health: per-tier row counts, newest bucket, `stale` flag. Separate from live freshness by design.                                                                                                                                                                                                                                                                                                                    |
| `GET /`                                      | Static dashboard (Workers Assets).                                                                                                                                                                                                                                                                                                                                                                                           |

Every aggregate response carries `X-Cache: HIT|MISS`. Status codes: unknown
operation → 404, missing/invalid window → 400 (both before any cache read),
backend failure → 502, undecodable entry → 500, missing `CACHEKIT_API_KEY`
secret → 503. Success body: `{ operation, window, data }` where `data` is the
decoded interop/v1 MessagePack map as written by the ingester.

Aggregate reads (not `/api/stats`) are additionally fronted by the Cloudflare
POP cache — 200s for 15 s, 404s for 10 s (negative caching) — so unauthenticated
public traffic can't mint billable misses against the metered CachekitIO
backend at will. A POP-cached
response replays the stored `X-Cache` header; per-POP scope means at most one
backend read per URL per POP per TTL.

## Develop

```bash
npm install
npm test              # mocked backend — no network, no CACHEKIT_API_KEY
npm run demo          # dashboard assets + real handler on http://localhost:8788; unknown assets return 404
npm run lint && npm run format:check && npm run type-check
```

CI ([`edge-qa`](../.github/workflows/edge-qa.yml)) gates every PR and push touching `edge/`.

`test/keys.test.ts` pins the byte-locked key vectors from
[`docs/architecture.md`](../docs/architecture.md#byte-locked-example-keys-generated-by-shipped-sdks-verified-3-way);
if it fails, key derivation drifted from the cross-SDK contract — fix the drift,
not the vectors.

## Hot-path integration

The Worker holds a **service binding** to the Rust-WASM hot path
(`wrangler.toml [[services]]`, `env.HOTPATH` → `skyline-hotpath`). Every
payload served through `/api/{operation}` is first integrity-checked there
(`POST /v1/verify`: xxHash3-64 + strict interop/v1 decode):

- verified → served with `x-hotpath: verified` + `x-hotpath-xxh3: <16-hex>`
- invalid → **500** `integrity_check_failed`; a corrupt entry is never served
- hot path unreachable → served with `x-hotpath: unavailable` (the aggregate
  is real — it came from the backend — it just goes out unverified and says so)

Misses never call the hot path: 404 + `X-Cache: MISS`, unchanged.

History capture is the hourly cron's only job (the Render keep-alive ping
it used to share the schedule with died with the Render deployment): at each
top of hour the scheduled handler snapshots the five `1h` aggregates into the
`HISTORY` D1 binding (daily tier + retention sweep at UTC midnight). Capture
failures produce gaps plus structured
`history_gap` / `history_capture_failed` logs — never invented points. Full
design + operating contract: [`docs/history.md`](../docs/history.md).

## Deploy

The Worker needs two secrets, both from your secret manager (creds per
[docs/architecture.md#credentials](../docs/architecture.md#credentials)):
`CACHEKIT_API_KEY`, the tenant's API key, and `CACHEKIT_API_URL`, the
CachekitIO endpoint the tenant is provisioned on. `wrangler.toml` lists both
under `[secrets] required`, so `wrangler deploy` fails, naming the missing
secret, until both are set on the Worker. Neither is committed: without the
guard, an unset `CACHEKIT_API_URL` would silently send the demo to the SDK's
default production host.

On a Worker that already exists, set or rotate a secret in place:

```bash
npx wrangler secret put CACHEKIT_API_KEY   # prompts for the value
npx wrangler secret put CACHEKIT_API_URL
```

The first deploy of a new Worker, and the first deploy after upgrading from a
config that set `CACHEKIT_API_URL` as a plain `[vars]` entry, must upload the
secrets with the deploy itself. A new Worker has nowhere to `secret put` to
yet, and on an existing one the plain variable still holds the name until a
deploy without `[vars]` removes it. Write the values to a file outside the
repository, deploy, then delete the file:

```bash
# secrets.env holds CACHEKIT_API_URL=… (and CACHEKIT_API_KEY=… for a new Worker)
npx wrangler deploy --secrets-file /path/outside/the/repo/secrets.env
```

Secrets already on the Worker and not in the file are kept.

Any pending D1 migration must be applied **before** the deploy that expects it:

```bash
npx wrangler d1 migrations apply skyline-history --remote
```

Dev deployment: **https://skyline-edge.raywalker.workers.dev**

`@cachekit-io/cachekit` 0.1.5 ships a WASM core that bundles for Workers as
is: no `nodejs_compat` flag, no build-time alias. Do not target 0.1.4 — it is
abandoned and uninstallable.
