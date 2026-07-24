"""Skyline Stage-2 Python ingester (LAB-744).

Consumes the Bluesky Jetstream, maintains 5m/1h/24h sliding windows, and
publishes the five locked analytics aggregates to CacheKit under the
interop/v1 contract in docs/architecture.md.
"""

NAMESPACE = "bluesky-thinking"
