# Skyline ingester on the lab k3s cluster

The ingester's deployment home (LAB-2383), replacing the Render free-tier
blueprint that used to live at `/render.yaml`. The image is published by
[`ingester-image.yml`](../../.github/workflows/ingester-image.yml) to
`ghcr.io/cachekit-io/skyline-ingester`, tagged by commit SHA only, on every
push to `main` that touches `ingester/**`. No `latest` tag is published —
nothing may reach the cluster under a mutable reference.

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

**2. Wire image-pull credentials** (one-time, and it *must* happen before the
first apply). The `cachekit-io` org **disables public packages** (the
visibility dialog greys out *Public* with "Setting is disabled by organization
administrators"), so the image is private and every pull needs GHCR
credentials — without them each apply ends in `ImagePullBackOff:
unauthorized`.

Copy the lab cluster's existing GHCR pull secret (`ghcr-creds` in
`arc-runners`) into `skyline` and attach it to the namespace's **default
ServiceAccount** rather than the Deployment: the ServiceAccount admission
controller merges SA pull secrets into every pod at creation, so a later
`kubectl apply` of this manifest can't strip them:

```bash
kubectl -n arc-runners get secret ghcr-creds -o json \
  | jq '.metadata = {name: "ghcr-creds", namespace: "skyline"}' \
  | kubectl apply -f -
kubectl -n skyline patch serviceaccount default \
  -p '{"imagePullSecrets":[{"name":"ghcr-creds"}]}'
```

No `ghcr-creds` to copy? Mint a **fine-grained PAT carrying only
`read:packages`** on `cachekit-io`, feed it to `kubectl create secret
docker-registry ghcr-creds --docker-server=ghcr.io ...`, then run the same
`patch` line above. Never park a broad-scope session token (e.g.
`gh auth token`) in a cluster secret: it outlives the shell, sits
unencrypted in the k3s datastore, and its blast radius is the GitHub org —
not this cluster.

**3. Render the image SHA into the manifest and apply** — the manifest ships
with the `SET-COMMIT-SHA` sentinel instead of a mutable tag, so *every* apply
(first or repeat) deploys an immutable commit-SHA reference and liveness-probe
restarts re-run the same bytes. Applying the file *unrendered* does not fail
gracefully — under `Recreate` the old pod is terminated first, so the sentinel
gives you `ImagePullBackOff` with nothing running. Always render via the guard
below, never `kubectl apply -f deploy/k3s/` directly:

Not every `main` commit has an image — the workflow only runs on pushes
touching `ingester/**` — so take the SHA from the latest successful publish
run, not from `git rev-parse`:

A missing run makes `sha` empty, which would render `skyline-ingester:` — an
invalid reference that `Recreate` would apply *after* terminating the running
pod, so validate before applying, never after:

```bash
sha="$(gh run list -R cachekit-io/bluesky-thinking -w ingester-image \
  -b main -e push -s success -L 1 --json headSha -q '.[0].headSha')"
if [[ "$sha" =~ ^[0-9a-f]{40}$ ]]; then
  sed "s|skyline-ingester:SET-COMMIT-SHA|skyline-ingester:${sha}|" \
    deploy/k3s/skyline-ingester.yaml | kubectl apply -f -
else
  echo "refusing to apply: no published image SHA (got '${sha}')" >&2
  false   # nonzero status so a chained `&& kubectl rollout status ...` won't proceed
fi
```

That `false` matters: without a nonzero status a failed render is merely
advisory, you walk on to the verify step, and it passes green against the
*old* pod that is still running — a deploy that deployed nothing, reported as
success. `false` rather than `exit 1` for the same reason this block avoids
`set -e`: an interactive paste must not kill the operator's shell.

Upgrades and rollbacks are the same lines with a different published SHA —
raise `-L` to list recent candidates.

**4. Verify** — wait for the rollout, then for the forwarded port, then read
`/health`; it should report `jetstream_connected: true` within ~a minute:

```bash
kubectl -n skyline rollout status deploy/skyline-ingester --timeout=180s
kubectl -n skyline port-forward deploy/skyline-ingester 18080:8080 & pf=$!
curl -sS --fail-with-body --retry 20 --retry-connrefused --retry-delay 1 \
  -o /tmp/skyline-health.json localhost:18080/health
health=$?
kill "$pf"   # drop the port-forward — a leaked one blocks the next run of this step
python3 -m json.tool /tmp/skyline-health.json
[[ $health -eq 0 ]] || echo "DEGRADED: /health never returned 200 — body above" >&2
```

Three details in that block are load-bearing, so don't simplify them away:

- `--fail-with-body`, not plain `--fail`: a 503 must be a *failure* (otherwise
  the degraded body pretty-prints and the step looks green on a dead
  consumer), but the body is also the diagnosis — it says *why*
  (`jetstream_connected: false`). `--fail` alone would throw it away.
- `-o` to a file rather than piping straight into `json.tool`: `curl` writes
  the body on *every* failed retry, so a piped version feeds `json.tool` one
  concatenated document per attempt and it dies with `Extra data` instead of
  showing the diagnosis. `-o` truncates per attempt, leaving exactly the last
  body. It also lets `$?` read `curl` directly instead of the pipe's tail.
- `$pf` rather than `%1`: job control is interactive-shell only, so `kill %1`
  fails with "no such job" the moment this block is pasted into a script.

`--retry` treats 503 as transient and `--retry-connrefused` covers the
port-forward race, so the retry budget absorbs both the startup window where
Jetstream hasn't connected yet and a forwarder that isn't listening yet. A 503
that survives it is a real dead consumer. No response at all means the pod
isn't up — check `kubectl -n skyline logs deploy/skyline-ingester`.

If GHCR image pulls fail with `unauthorized`, step 2 did not take. Three
causes, in order of likelihood: the secret is missing from the `skyline`
namespace; it is not attached to the default ServiceAccount
(`kubectl -n skyline get sa default -o jsonpath='{.imagePullSecrets}'` must
name `ghcr-creds`); or the token inside a *copied* secret lacks
`read:packages` on `cachekit-io` (expired or rotated at the source). After
fixing any of them, `kubectl -n skyline rollout restart
deploy/skyline-ingester` — the admission controller injects SA pull secrets
only at pod *creation*, so the already-stuck pod never picks up the fix.

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
