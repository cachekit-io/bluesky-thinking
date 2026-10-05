# Live-integration evidence harness

Repeatable proofs against the live CachekitIO backend. Credentials per
[`docs/architecture.md#credentials`](../docs/architecture.md#credentials):
export them from your own secret manager into the shell that runs the
commands, so nothing secret touches disk or logs. Export only
`CACHEKIT_API_KEY`: with `CACHEKIT_MASTER_KEY` also in the environment,
cachekit >= 0.21.0 refuses any cache that omits `encryption=`. All three scripts run from `ingester/` — `derive_keys.py` and
`stampede.py` reuse its venv; `raw_read.py` is deliberately SDK-free and
brings its own deps via `--with`:

```bash
cd ingester
# Replace <endpoint> with your CachekitIO endpoint URL, and export
# CACHEKIT_API_KEY from your secret manager:
export CACHEKIT_API_URL="<endpoint>" CACHEKIT_ALLOW_CUSTOM_HOST=true
```

| Script | Proof | Run |
| :--- | :--- | :--- |
| `derive_keys.py` | Prints all 17 keys the ingester writes (15 interop/v1 aggregates + auto-mode checkpoint + `@cache.secure` sentiment), from the real decorator machinery. No network. | `uv run python ../stage3/derive_keys.py` |
| `raw_read.py` | SDK-free reader over the raw HTTP API. `--expect absent` = clean-namespace audit; default = byte/checksum evidence (xxHash3-64, strict single-document MessagePack check); `--expect ciphertext --forbid …` = zero-knowledge check; `--delete` = cleanup. | `uv run --with httpx --with xxhash --with msgpack python ../stage3/raw_read.py [flags] KEY…` |
| `stampede.py` | N=12 concurrent async callers on one cold key → exactly one recompute, real `POST/DELETE …/lock` SaaS traffic in the httpx log. The cached function must be async — cachekit-py's sync wrapper does no distributed locking. | `uv run python ../stage3/stampede.py` |

Last recorded run: 2026-07-29.
