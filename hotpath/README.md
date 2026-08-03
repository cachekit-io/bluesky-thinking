# Skyline hot-path Worker (`hotpath/`)

The Rust-WASM leg of the Skyline edge (Stage 2, LAB-746): `cachekit-rs` 0.5.0 (crates.io)
compiled to `wasm32-unknown-unknown`, deployed as its own Cloudflare Worker. Stage 3 binds it into the
TS serving path (service binding); until then it runs standalone.

Dev deployment: **https://skyline-hotpath.raywalker.workers.dev**

## What it computes

| Job | Endpoint | Notes |
| :--- | :--- | :--- |
| interop/v1 key derivation | `GET /v1/key/:operation/:window` | The five locked operations × `5m`/`1h`/`24h` (contract: [`docs/architecture.md`](../docs/architecture.md)). Returns the key + locked TTL. Off-contract input → 400. |
| Payload integrity | `POST /v1/verify[?expected=<16-hex>]` | Body = raw cached payload. Returns xxHash3-64 (big-endian hex, the `StorageEnvelope` convention) + strict interop/v1 validity (single MessagePack document, no trailing bytes, CK frames flagged with a diagnostic). |
| Window-slice aggregation | `POST /v1/merge` | JSON `{"slices": ["<base64 msgpack {str:int} doc>", …], "top": 50}` → merged top-N counts (count desc, key asc) + the canonical interop/v1 MessagePack of the result, ready to write back byte-identically. |
| Cache read + verify | `GET /v1/cache/:operation/:window` | Derives the key, fetches the live backend, checksums + strict-decodes the payload. 503 only if the `CACHEKIT_API_KEY` secret is missing (set since Stage 3). The fetch is a direct `worker::Fetch` GET — **LAB-1079 workaround**: `WorkersCachekitIO` (cachekit-rs ≤ 0.8.0) panics on wasm32 (`SystemTime::now()` in its session headers); swap back once the SDK fix ships. Key derivation, interop decode and checksum stay on cachekit-rs / cachekit-core. |
| Service info | `GET /` | Contract summary + endpoint list; doubles as a health check. |

Example — the byte-locked spike vector, derived live on the edge:

```console
$ curl https://skyline-hotpath.raywalker.workers.dev/v1/key/posts_per_minute/5m
{"key":"bluesky-thinking:posts_per_minute:230037def14c9a89b18603f313d982d6a3f7acd4af5147b2f6ae2c257b82ce57", …}
```

## Layout

- `src/compute.rs` — pure hot-path logic, target-independent. All contract behaviour
  (byte-locked key vectors, TTL map, checksum, merge semantics) is natively tested here —
  no network, no credentials, no Workers runtime.
- `src/lib.rs` — the `cfg(target_arch = "wasm32")` HTTP surface (worker `Router`, JSON glue).

## Build, test, deploy

Build-chain pins are locked in [`docs/architecture.md`](../docs/architecture.md#build-chain-pins-from-spike-friction-so-stage-2-doesnt-rediscover-them):
`worker-build@^0.1`, `wasm-bindgen-cli` **0.2.126** on `PATH` (Cargo.toml pins the
`wasm-bindgen` crate to `=0.2.126` and the committed `Cargo.lock` holds the full graph, so
CLI and crate ABI can never drift). `cachekit-rs` comes from **crates.io 0.5.0** — the
spec's git-tag workaround retired when LAB-742's publish landed; 0.5.0 keeps `worker`
pinned at 0.4, so the chain pins are unchanged.

```console
$ cargo test                                     # native: contract + vector tests, no creds
$ cargo clippy --all-targets -- -D warnings      # native lint
$ cargo clippy --target wasm32-unknown-unknown -- -D warnings
$ worker-build --release                         # reproducible wasm32 build (the one-liner)
$ npx wrangler deploy                            # runs worker-build itself, then uploads
```

CI ([`hotpath-qa`](../.github/workflows/hotpath-qa.yml)) runs everything above except the deploy —
native tests + clippy, wasm32 clippy, and the pinned `worker-build --release` — on every PR and
push touching `hotpath/`.

Secrets: `CACHEKIT_API_KEY` via `wrangler secret put CACHEKIT_API_KEY`, from
`op://cachekit/ck-dev-bluesky-default/credential`
([docs/architecture.md#credentials](../docs/architecture.md#credentials)).
Config: `CACHEKIT_API_URL` (`wrangler.toml [vars]`) points at the dev
instance. Nothing else is configurable.

Stage 3 also bound this Worker into the TS edge's serving path: the edge
holds a service binding (`env.HOTPATH`) and `POST /v1/verify`s every payload
it serves — see [`edge/README.md`](../edge/README.md).
