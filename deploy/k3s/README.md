# Skyline ingester on Kubernetes

The ingester runs as a single self-hosted, egress-only container. These
manifests target any conformant Kubernetes cluster (developed against k3s); the
directory name is historical. The image is published by
[`ingester-image.yml`](../../.github/workflows/ingester-image.yml) to
`ghcr.io/cachekit-io/skyline-ingester`, tagged by commit SHA only, on every
push to `main` that touches `ingester/**`. No `latest` tag is published —
nothing may reach the cluster under a mutable reference.

## Runbook

Run on a machine with `kubectl` access to the target cluster. Confirm the
current context names your target cluster first, so nothing lands on the
wrong one:

```bash
kubectl config current-context
```

**1. Create the secret** (first deploy, and again whenever a value rotates —
the command is idempotent). It carries the CachekitIO endpoint and both
credentials, supplied at deploy time: never committed, never echoed. The
ingester **fails closed** without both credentials; live mode refuses to start
rather than run with the secure sentiment cache disabled.

Export all three variables in the same shell first, every time — the command
rewrites the whole Secret, so it needs every value, not just the one that
changed:

- `CACHEKIT_API_URL` — the CachekitIO API endpoint your tenant is provisioned
  on (the SDK default is `https://api.cachekit.io`).
- `CACHEKIT_API_KEY` — the tenant's API key, from your secret manager.
- `CACHEKIT_MASTER_KEY` — its 64-hex master key, from your secret manager; the
  ingester validates the format at startup.

The first line refuses to go on if any of the three is unset or empty, so a
partial export can never blank a working value in the live Secret. It must
stop the whole pipeline, not just its left side: a failed `create` still feeds
`kubectl apply` an empty stream:

```bash
: "${CACHEKIT_API_URL:?}" "${CACHEKIT_API_KEY:?}" "${CACHEKIT_MASTER_KEY:?}" &&
kubectl create namespace skyline --dry-run=client -o yaml | kubectl apply -f - &&
kubectl -n skyline create secret generic skyline-ingester \
  --from-literal=CACHEKIT_API_URL="$CACHEKIT_API_URL" \
  --from-literal=CACHEKIT_API_KEY="$CACHEKIT_API_KEY" \
  --from-literal=CACHEKIT_MASTER_KEY="$CACHEKIT_MASTER_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Rotation only (skip on first deploy): environment variables from a Secret are
fixed at container start, and re-applying an unchanged image SHA leaves the pod as it is, so a rotation
takes effect only after a restart. `/health` checks only Jetstream: a revoked
key fails every cache write while health stays 200. After rotating, restart:

```bash
kubectl -n skyline rollout restart deploy/skyline-ingester
```

**2. Wire image-pull credentials** (one-time, and it *must* happen before the
first apply). The `cachekit-io` org **disables public packages** (the
visibility dialog greys out *Public* with "Setting is disabled by organization
administrators"), so the image is private and every pull needs GHCR
credentials — without them each apply ends in `ImagePullBackOff:
unauthorized`.

Mint a **classic PAT carrying only `read:packages`** with access to
`cachekit-io` packages (GHCR does not accept fine-grained PATs), store it as a
`ghcr-creds` pull secret, and attach it to the namespace's **default
ServiceAccount** rather than the Deployment: the ServiceAccount admission
controller merges SA pull secrets into every pod at creation, so a later
`kubectl apply` of this manifest can't strip them. Like step 1, this is
idempotent, so re-running it with a fresh PAT replaces an expired one:

```bash
: "${GHCR_USER:?}" "${GHCR_READ_PACKAGES_PAT:?}" &&
kubectl -n skyline create secret docker-registry ghcr-creds \
  --docker-server=ghcr.io --docker-username="$GHCR_USER" \
  --docker-password="$GHCR_READ_PACKAGES_PAT" \
  --dry-run=client -o yaml | kubectl apply -f - &&
kubectl -n skyline patch serviceaccount default \
  -p '{"imagePullSecrets":[{"name":"ghcr-creds"}]}'
```

Never park a broad-scope session token (e.g. `gh auth token`) in a cluster
secret: it outlives the shell, sits in the cluster datastore (unencrypted
unless the cluster encrypts Secrets at rest), and its blast radius is the
GitHub org — not this cluster.

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
pod, so validate before applying, never after. A Secret missing one of the
three keys fails the same way (`CreateContainerConfigError` after the old pod
is gone), and an empty one crash-loops the new pod, so the guard also checks
that each key is present and non-empty, without printing any value:

```bash
sha="$(gh run list -R cachekit-io/bluesky-thinking -w ingester-image \
  -b main -e push -s success -L 1 --json headSha -q '.[0].headSha')"
keys=" $(kubectl -n skyline get secret skyline-ingester \
  -o go-template='{{range $k, $v := .data}}{{if $v}}{{$k}} {{end}}{{end}}') "
missing=""
for k in CACHEKIT_API_URL CACHEKIT_API_KEY CACHEKIT_MASTER_KEY; do
  [[ "$keys" == *" $k "* ]] || missing+=" $k"
done
if [[ -n "$missing" ]]; then
  echo "refusing to apply: Secret skyline-ingester lacks or has empty:${missing} (step 1)" >&2
  false
elif [[ "$sha" =~ ^[0-9a-f]{40}$ ]]; then
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

**Then check the endpoint.** `/health` checks only Jetstream, and the SDK
logs backend errors without raising, so a wrong `CACHEKIT_API_URL` passes
everything above while every cache write fails. This line confirms the pod
picked up the Secret and shows the endpoint it publishes to:

```bash
kubectl -n skyline logs deploy/skyline-ingester | grep -F 'live mode: publishing to CachekitIO at'
```

The gate for a wrong endpoint is [`stage4/verify.sh`](../../stage4/verify.sh)
after 75 s (the 60 s TTL plus the edge's 15 s POP cache): its freshness check
fails unless the ingester's
aggregates reach the backend the edge reads.

If GHCR image pulls fail with `unauthorized`, step 2 did not take. Three
causes, in order of likelihood: the secret is missing from the `skyline`
namespace; it is not attached to the default ServiceAccount
(`kubectl -n skyline get sa default -o jsonpath='{.imagePullSecrets}'` must
name `ghcr-creds`); or the PAT in `ghcr-creds` has expired or lacks
`read:packages` on `cachekit-io`. In the PAT case, re-run step 2 with a fresh
PAT first — restarting without refreshing it just hands the new pod the same
dead credentials. After fixing any of them, `kubectl -n skyline
rollout restart deploy/skyline-ingester` — the admission controller injects
SA pull secrets only at pod *creation*, so the already-stuck pod never picks
up the fix.

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
- **Egress-only.** Outbound WebSocket to Jetstream + HTTPS to the
  CachekitIO endpoint. No Service, no Ingress, nothing dials in. The old
  Cloudflare keep-alive cron existed solely for Render's inbound-idle
  spin-down and is gone; the edge cron that remains (`0 * * * *`) is history
  capture only and never touches the ingester.
- **Memory bounds are measured, not guessed.** Requests/limits encode
  measurements taken in production against live Jetstream (full-24h-window
  working set plus the glibc transient high-water the publish ticks ratchet
  up, which the original
  [#17](https://github.com/cachekit-io/bluesky-thinking/pull/17)
  minutes-scale measurement could not see) — see the comment block in
  [`skyline-ingester.yaml`](skyline-ingester.yaml).
