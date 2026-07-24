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
