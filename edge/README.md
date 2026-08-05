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

Aggregate reads (not `/api/stats`) are additionally fronted by the Cloudflare
POP cache — 200s for 15 s, 404s for 10 s (negative caching) — so unauthenticated
public traffic can't mint billable misses against the metered CachekitIO
backend at will (Stage-3 panel finding, closed in LAB-738). A POP-cached
response replays the stored `X-Cache` header; per-POP scope means at most one
backend read per URL per POP per TTL.

## Develop

```bash
npm install
npm test              # mocked backend — no network, no CACHEKIT_API_KEY
npm run demo          # dashboard + real handler on http://localhost:8788, seeded in-memory backend
npm run lint && npm run format:check && npm run type-check
```

CI ([`edge-qa`](../.github/workflows/edge-qa.yml)) gates every PR and push touching `edge/`.

`test/keys.test.ts` pins the byte-locked key vectors from
[`docs/architecture.md`](../docs/architecture.md#byte-locked-example-keys-generated-by-shipped-sdks-verified-3-way);
if it fails, key derivation drifted from the cross-SDK contract — fix the drift,
not the vectors.

## Hot-path integration (Stage 3)

The Worker holds a **service binding** to the Rust-WASM hot path
(`wrangler.toml [[services]]`, `env.HOTPATH` → `skyline-hotpath`). Every
payload served through `/api/{operation}` is first integrity-checked there
(`POST /v1/verify`: xxHash3-64 + strict interop/v1 decode):

- verified → served with `x-hotpath: verified` + `x-hotpath-xxh3: <16-hex>`
- invalid → **500** `integrity_check_failed`; a corrupt entry is never served
- hot path unreachable → served with `x-hotpath: unavailable` (the aggregate
  is real — it came from the backend — it just goes out unverified and says so)

Misses never call the hot path: 404 + `X-Cache: MISS`, unchanged.

## Deploy

`wrangler deploy`, then set the secret (creds per
[docs/architecture.md#credentials](../docs/architecture.md#credentials)):

```bash
op read "op://cachekit/ck-dev-bluesky-default/credential" | wrangler secret put CACHEKIT_API_KEY
```

Dev deployment: **https://skyline-edge.raywalker.workers.dev** (the dev
instance URL is a `[vars]` entry, `CACHEKIT_API_URL`). Production routing and
a custom domain are Stage 4.

Two build-time accommodations for `@cachekit-io/cachekit` 0.1.3 (both retire
with the 0.1.4 WASM core, blocked on LAB-780): the `nodejs_compat` flag
(transitive node builtins), and a wrangler `[alias]` stubbing the NAPI-native
`@cachekit-io/cachekit-core-ts` — the edge never runs that path (interop
reads only, no ByteStorage envelope), and the stub throws if that ever stops
being true.
