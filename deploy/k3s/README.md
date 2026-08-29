# Skyline ingester on the lab k3s cluster

The ingester's deployment home (LAB-2383), replacing the Render free-tier
blueprint that used to live at `/render.yaml`. The image is published by
[`ingester-image.yml`](../../.github/workflows/ingester-image.yml) to
`ghcr.io/cachekit-io/skyline-ingester` (`latest` + commit SHA) on every push
to `main` that touches `ingester/**`.

## Runbook

Run on a machine with `kubectl` access to the lab cluster. Verify the context
first — memory/MCP services live on `lab`, never `mem`:

```bash
kubectl config current-context   # must say: lab
```

**1. Create the secret** (one-time; both values come from
`op://cachekit/ck-dev-bluesky-default`). The ingester **fails closed** without
both — live mode refuses to start rather than run with the secure sentiment
cache disabled:

```bash
kubectl create namespace skyline --dry-run=client -o yaml | kubectl apply -f -
kubectl -n skyline create secret generic skyline-ingester \
  --from-literal=CACHEKIT_API_KEY="$(op read 'op://cachekit/ck-dev-bluesky-default/credential')" \
  --from-literal=CACHEKIT_MASTER_KEY="$(op read 'op://cachekit/ck-dev-bluesky-default/encryption_key')"
```

`CACHEKIT_MASTER_KEY` is the 64-hex `encryption_key` field; the ingester
validates the format at startup.

**2. Render the image SHA into the manifest and apply** — the manifest ships
with the `SET-COMMIT-SHA` sentinel instead of a mutable tag, so *every* apply
(first or repeat) deploys an immutable commit-SHA reference and liveness-probe
restarts re-run the same bytes. Applying the file unrendered fails closed
(`ImagePullBackOff` on the sentinel) rather than silently resolving `latest`:

Not every `main` commit has an image — the workflow only runs on pushes
touching `ingester/**` — so take the SHA from the latest successful publish
run, not from `git rev-parse`:

```bash
sha="$(gh run list -R cachekit-io/bluesky-thinking -w ingester-image \
  -b main -e push -s success -L 1 --json headSha -q '.[0].headSha')"
sed "s|skyline-ingester:SET-COMMIT-SHA|skyline-ingester:${sha}|" \
  deploy/k3s/skyline-ingester.yaml | kubectl apply -f -
```

Upgrades and rollbacks are the same lines with a different published SHA —
raise `-L` to list recent candidates. Optional hardening: a SHA *tag* is
still repointable by anyone with `packages:write`; for a fully immutable
reference resolve the tag's digest and render `:${sha}@sha256:<digest>`
instead (digest: the `ingester-image` run summary, or
`gh api /orgs/cachekit-io/packages/container/skyline-ingester/versions`).

**3. Verify** — wait for the rollout, then for the forwarded port, then read
`/health`; it should report `jetstream_connected: true` within ~a minute:

```bash
kubectl -n skyline rollout status deploy/skyline-ingester --timeout=180s
kubectl -n skyline port-forward deploy/skyline-ingester 18080:8080 &
curl -s --retry 20 --retry-connrefused --retry-delay 1 \
  localhost:18080/health | python3 -m json.tool
kill %1   # drop the port-forward — a leaked one blocks the next run of this step
```

A `503` body says *why* it is degraded (`jetstream_connected: false`); no
response at all means the pod isn't up — check
`kubectl -n skyline logs deploy/skyline-ingester`.

If GHCR image pulls fail with `unauthorized`: the
`ghcr.io/cachekit-io/skyline-ingester` package must be public (GHCR packages
default to private on first publish — flip it once in the package settings),
or add an `imagePullSecret` to the Deployment.

## Semantics worth knowing (carried over from the Render deployment)

- **Health = liveness, 503 = dead consumer.** `GET /health`
  (`ingester/src/skyline_ingester/health.py`) returns 503 while Jetstream is
  disconnected. The Deployment's `livenessProbe` turns a *sustained* 503
  (~3 min: 6 failures × 30 s) into a container restart — the same
  dead-consumer-restart semantics Render's health check provided. Transient
  Jetstream reconnects never trip it.
- **Restarts are safe; disk is not the persistence layer.** Window state
  survives restarts via the CacheKit checkpoint
  ([#18](https://github.com/cachekit-io/bluesky-thinking/pull/18)), never via
  the filesystem — the container runs with a read-only root fs to keep that
  honest. Restart staleness is bounded by `CHECKPOINT_INTERVAL_SECONDS`
  (300 s).
- **Egress-only.** Outbound WebSocket to Jetstream + HTTPS to
  `api.dev.cachekit.io`. No Service, no Ingress, nothing dials in. The old
  Cloudflare keep-alive cron existed solely for Render's inbound-idle
  spin-down and is gone; the edge cron that remains (`0 * * * *`) is history
  capture only and never touches the ingester.
- **Memory bounds are measured, not guessed.** Requests/limits encode the
  LAB-1775 / [#17](https://github.com/cachekit-io/bluesky-thinking/pull/17)
  window-compaction findings — see the comment block in
  [`skyline-ingester.yaml`](skyline-ingester.yaml).
