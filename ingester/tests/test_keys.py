"""Key derivation: the byte-locked interop/v1 vectors from docs/architecture.md."""

import re

from cachekit import generate_interop_key

from skyline_ingester import NAMESPACE
from skyline_ingester.windows import OPERATIONS, WINDOW_TTLS

# Verified 3-way (py/ts/rs) by the LAB-735 spike; locked in docs/architecture.md.
LOCKED_VECTORS = {
    (
        "trending_hashtags",
        "5m",
    ): "bluesky-thinking:trending_hashtags:230037def14c9a89b18603f313d982d6a3f7acd4af5147b2f6ae2c257b82ce57",
    (
        "trending_hashtags",
        "1h",
    ): "bluesky-thinking:trending_hashtags:17092aa9bfa2cc2fa567c40b8d5a23d93ee9f148f7754467eeb90bd0168d9301",
    (
        "trending_hashtags",
        "24h",
    ): "bluesky-thinking:trending_hashtags:587d262535cbfca724700a52f210eaa396da79f44e0cb3135afdd2eecb3907f3",
    (
        "posts_per_minute",
        "5m",
    ): "bluesky-thinking:posts_per_minute:230037def14c9a89b18603f313d982d6a3f7acd4af5147b2f6ae2c257b82ce57",
}


def test_byte_locked_vectors():
    for (operation, window), expected in LOCKED_VECTORS.items():
        assert generate_interop_key(NAMESPACE, operation, [window]) == expected


def test_args_hash_is_shared_across_operations():
    # Same canonical argument array -> same hash; the operation segment is the identity.
    for window in WINDOW_TTLS:
        hashes = {generate_interop_key(NAMESPACE, op, [window]).rsplit(":", 1)[1] for op in OPERATIONS}
        assert len(hashes) == 1


def test_all_fifteen_keys_are_well_formed():
    pattern = re.compile(rf"^{NAMESPACE}:[a-z_]+:[0-9a-f]{{64}}$")
    keys = {generate_interop_key(NAMESPACE, op, [w]) for op in OPERATIONS for w in WINDOW_TTLS}
    assert len(keys) == 15
    assert all(pattern.match(k) for k in keys)
