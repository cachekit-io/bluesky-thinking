# Skyline edge API + dashboard

TypeScript Cloudflare Worker serving the five Skyline aggregates from the shared
CachekitIO namespace via [interop/v1](../docs/architecture.md#locked-key-convention)
reads (`@cachekit-io/cachekit` 0.1.3), plus a static dashboard on Workers Assets.
The edge is **read-only**: aggregates are computed and written by the Python
ingester; a cache miss here is surfaced (404 + `X-Cache: MISS`), never recomputed
or faked.

## API

| Route                                  | Description                                                                                                                                                                                                          |
| :------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /api/{operation}?window={window}` | Cached aggregate. `operation` ∈ `trending_hashtags` · `trending_links` · `lang_mix` · `posts_per_minute` · `top_emoji`; `window` ∈ `5m` · `1h` · `24h` (required — interop binding rules forbid default parameters). |
| `GET /api/stats`                       | Per-isolate `hits` / `misses` / `errors` / `hit_rate` (resets when Cloudflare recycles the isolate).                                                                                                                 |
| `GET /`                                | Static dashboard (Workers Assets).                                                                                                                                                                                   |

Every aggregate response carries `X-Cache: HIT|MISS`. Status codes: unknown
operation → 404, missing/invalid window → 400 (both before any cache read),
backend failure → 502, undecodable entry → 500, missing `CACHEKIT_API_KEY`
secret → 503. Success body: `{ operation, window, data }` where `data` is the
decoded interop/v1 MessagePack map as written by the ingester.

## Develop

```bash
npm install
npm test              # mocked backend — no network, no CACHEKIT_API_KEY
npm run demo          # dashboard + real handler on http://localhost:8788, seeded in-memory backend
npm run lint && npm run format:check && npm run type-check
```

`test/keys.test.ts` pins the byte-locked key vectors from
[`docs/architecture.md`](../docs/architecture.md#byte-locked-example-keys-generated-by-shipped-sdks-verified-3-way);
if it fails, key derivation drifted from the cross-SDK contract — fix the drift,
not the vectors.

## Deploy (Stage 4)

`wrangler deploy` after `wrangler secret put CACHEKIT_API_KEY` (key provisioned
per the [runbook](../docs/architecture.md#provisioning-runbook)). Live
integration and production routing are Stage 3/4 concerns — nothing here
requires credentials until then.
