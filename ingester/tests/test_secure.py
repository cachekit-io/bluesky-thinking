"""AC-6 groundwork: the secure sentiment cache stores ciphertext only."""

from skyline_ingester import NAMESPACE
from skyline_ingester.publisher import Publisher

from .conftest import MASTER_KEY, NOW


def test_secure_cache_stores_ciphertext_only(publisher, backend):
    publisher.publish_window("1h")  # the secure op rides the 1h tick
    secure_keys = [k for k in backend.stored_keys() if k.startswith(f"ns:{NAMESPACE}:func:")]
    assert len(secure_keys) == 1, "expected exactly one auto-mode (secure) key"
    raw = backend.get(secure_keys[0])
    # zero-knowledge: no plaintext fragments of the value in the stored bytes
    for marker in (b"langs", b"generated_at", b"avg", b'"en"', b"window"):
        assert marker not in raw


def test_secure_roundtrip_decrypts_through_the_sdk(store, backend):
    publisher = Publisher(store, backend, master_key=MASTER_KEY, now_fn=lambda: NOW)
    publisher.publish_window("1h")
    value = publisher._secure_fn("1h")  # cache hit -> decrypts the stored entry
    assert value["window"] == "1h"
    assert value["langs"]["en"]["n"] > 0
    assert -1.0 <= value["langs"]["en"]["avg"] <= 1.0


def test_no_master_key_disables_secure_cache(store, backend):
    publisher = Publisher(store, backend, now_fn=lambda: NOW)
    assert not publisher.secure_enabled
    assert publisher.publish_window("1h") == 5  # the five interop ops still publish
    assert not [k for k in backend.stored_keys() if k.startswith("ns:")]


def test_live_mode_without_master_key_fails_closed():
    """Epic decision (ray, 2026-07-24): a live deploy missing the secure-cache
    master key must refuse to start, not come up with AC-6 silently absent."""
    import pytest
    from pydantic import SecretStr

    from skyline_ingester.__main__ import build_publisher
    from skyline_ingester.config import Settings
    from skyline_ingester.windows import WindowStore

    settings = Settings(cachekit_api_key=SecretStr("ck_test_not_a_real_key"), cachekit_master_key=None)
    with pytest.raises(RuntimeError, match="fail closed"):
        build_publisher(settings, WindowStore())  # raises before any backend is constructed


def test_live_mode_builds_backend_from_env(monkeypatch):
    """Live mode must construct CachekitIOBackend via the SDK's env-config path.

    Regression (LAB-737): passing api_key alone to the constructor raises
    "Both api_url and api_key required if using manual config", so the
    pre-Stage-3 live path could never start. Env config also carries the
    CACHEKIT_API_URL / CACHEKIT_ALLOW_CUSTOM_HOST overrides the dev instance
    (api.dev.cachekit.io — not in the SDK's SSRF host allowlist) needs.
    """
    from pydantic import SecretStr

    from skyline_ingester.__main__ import build_publisher
    from skyline_ingester.config import Settings
    from skyline_ingester.windows import WindowStore

    monkeypatch.setenv("CACHEKIT_API_KEY", "ck_test_not_a_real_key")
    monkeypatch.setenv("CACHEKIT_API_URL", "https://api.dev.cachekit.io")
    monkeypatch.setenv("CACHEKIT_ALLOW_CUSTOM_HOST", "true")
    settings = Settings(
        cachekit_api_key=SecretStr("ck_test_not_a_real_key"),
        cachekit_master_key=SecretStr("a" * 64),
    )
    # Old code raised ValueError here; construction makes no network calls.
    publisher = build_publisher(settings, WindowStore())
    assert publisher.secure_enabled
