# Stage-3 live-integration evidence harness (LAB-737)

Repeatable proofs against the live `dev.cachekit` backend. Credentials per
[`docs/architecture.md#credentials`](../docs/architecture.md#credentials);
every command below runs under `op run` so nothing secret touches disk or
logs. All three scripts run from `ingester/` (they reuse its venv):

```bash
cd ingester
export CACHEKIT_API_URL=https://api.dev.cachekit.io CACHEKIT_ALLOW_CUSTOM_HOST=true
```

| Script | Proof | Run |
| :--- | :--- | :--- |
| `derive_keys.py` | Prints all 17 keys the ingester writes (15 interop/v1 aggregates + auto-mode checkpoint + `@cache.secure` sentiment), from the real decorator machinery. No network. | `uv run python ../stage3/derive_keys.py` |
| `raw_read.py` | SDK-free reader over the raw HTTP API. `--expect absent` = AC-2 clean-namespace audit; default = AC-4 byte/checksum evidence (xxHash3-64, strict single-document MessagePack check); `--expect ciphertext --forbid …` = AC-6 zero-knowledge check; `--delete` = cleanup. | `op run --env-file=../.op.apikey.env -- uv run --with httpx --with xxhash --with msgpack python ../stage3/raw_read.py [flags] KEY…` |
| `stampede.py` | AC-5: N=12 concurrent async callers on one cold key → exactly one recompute, real `POST/DELETE …/lock` SaaS traffic in the httpx log. The cached function must be async — cachekit-py's sync wrapper does no distributed locking. | `op run --env-file=../.op.apikey.env -- uv run python ../stage3/stampede.py` |

Recorded Stage-3 results (2026-07-29) live on LAB-737.
