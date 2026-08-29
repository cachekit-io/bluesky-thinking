# Skyline ingester container (LAB-2383): the k3s replacement for the Render
# blueprint that used to live in /render.yaml. Build context is ingester/.
#
# Multi-stage per the uv docs' recommended pattern: the builder image carries
# uv, the runtime image carries only the synced virtualenv — no uv binary, no
# build cache, no source checkout in the final layer.
#
# Python is 3.12 to match ingester/.python-version — keep the two in sync when
# bumping either. Dependencies are exactly uv.lock (--locked fails the build on
# any drift, same contract as the old Render `uv sync --locked` build command).
#
# Both base images are pinned tag@digest (multi-arch manifest-list digests):
# the tag is documentation, the digest is what builds — reproducible and
# immune to tag repointing. Renovate (renovate.json) keeps the digests current.

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58 AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependency layer first so code-only changes don't re-resolve the lock.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

# hatchling needs README.md (pyproject `readme`); --no-editable installs the
# project into the venv so the runtime stage needs no /app/src.
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.12-slim-bookworm@sha256:0f5b26b9518d002b6173fd61daad821fa340635ebfec5bba471013f9ca114579

# Non-root with a FIXED numeric UID: k8s `runAsNonRoot: true` rejects images
# whose USER is a name (kubelet can't prove a name isn't root), so the UID
# must be numeric and stable. The process needs no writable filesystem:
# window state checkpoints into CacheKit, never onto disk.
RUN groupadd --gid 10001 skyline \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin skyline

COPY --from=builder --chown=10001:10001 /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

USER 10001:10001

# /health listener (skyline_ingester/health.py); PORT env overrides, default 8080.
EXPOSE 8080

# Equivalent of the old `uv run skyline-ingester` start command: the same
# console script, straight from the synced venv.
ENTRYPOINT ["skyline-ingester"]
