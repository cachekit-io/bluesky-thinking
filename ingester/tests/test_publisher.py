"""Publishing through the shipped SDK onto an in-process bytes backend."""

from cachekit import decode_interop_value, generate_interop_key

from skyline_ingester import NAMESPACE
from skyline_ingester.extract import PostFeatures
from skyline_ingester.windows import OPERATIONS, WINDOW_TTLS

from .conftest import FIXTURE_TOTALS, NOW


def test_publish_writes_all_operations_under_locked_keys(publisher, backend):
    assert publisher.publish_window("5m") == 5
    for operation in OPERATIONS:
        key = generate_interop_key(NAMESPACE, operation, ["5m"])
        raw = backend.get(key)
        assert raw is not None, f"missing {key}"
        value = decode_interop_value(raw)
        assert value["window"] == "5m"
        assert value["total_posts"] == FIXTURE_TOTALS["5m"]
    # the byte-locked vector key specifically
    assert backend.get("bluesky-thinking:trending_hashtags:230037def14c9a89b18603f313d982d6a3f7acd4af5147b2f6ae2c257b82ce57")


def test_locked_ttls_reach_the_backend(publisher, backend):
    for window, ttl in WINDOW_TTLS.items():
        publisher.publish_window(window)
        key = generate_interop_key(NAMESPACE, "posts_per_minute", [window])
        assert backend.ttls[key] == ttl


def test_values_are_plain_msgpack_maps(publisher, backend):
    publisher.publish_window("5m")
    raw = backend.get(generate_interop_key(NAMESPACE, "lang_mix", ["5m"]))
    assert raw[:2] != b"CK"  # no ByteStorage frame — interop/v1 plain msgpack
    value = decode_interop_value(raw)
    assert isinstance(value, dict) and all(isinstance(k, str) for k in value)


def test_interop_values_stay_plaintext_with_master_key_in_env(monkeypatch, store, backend):
    """CACHEKIT_MASTER_KEY auto-enables cachekit encryption from the env; the
    interop aggregates are contract-locked to plain msgpack (TS/Rust read them
    without keys) and must pin encryption off regardless."""
    from skyline_ingester.publisher import Publisher

    from .conftest import MASTER_KEY

    monkeypatch.setenv("CACHEKIT_MASTER_KEY", MASTER_KEY)
    publisher = Publisher(store, backend, master_key=MASTER_KEY, now_fn=lambda: NOW)
    publisher.publish_window("5m")
    raw = backend.get(generate_interop_key(NAMESPACE, "lang_mix", ["5m"]))
    assert decode_interop_value(raw)["window"] == "5m"  # decodable without any key


def test_republish_refreshes_stale_values(publisher, backend, store):
    publisher.publish_window("5m")
    key = generate_interop_key(NAMESPACE, "posts_per_minute", ["5m"])
    before = decode_interop_value(backend.get(key))
    store.add(PostFeatures(ts=NOW, lang="en", hashtags=[], links=[], emoji=[], sentiment=None))
    publisher.publish_window("5m")  # invalidate + recompute, not a cache hit
    after = decode_interop_value(backend.get(key))
    assert after["total_posts"] == before["total_posts"] + 1


def test_failed_recompute_keeps_the_live_key(publisher, backend, store, monkeypatch):
    # Regression: publish was invalidate-then-recompute, so a recompute failure left
    # the key deleted (a miss on the metered-miss path). Recompute now runs first, so
    # a failure leaves the previously published entry intact.
    publisher.publish_window("5m")
    key = generate_interop_key(NAMESPACE, "trending_hashtags", ["5m"])
    assert backend.get(key) is not None

    def boom(*_a, **_k):
        raise RuntimeError("recompute failed")

    monkeypatch.setattr(store, "build_value", boom)
    assert publisher.publish_window("5m") == 0  # every op fails its recompute probe
    assert backend.get(key) is not None  # ...and the live key was NOT invalidated
