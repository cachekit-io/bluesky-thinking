"""Checkpoint/restore: a restart must not zero the 24h window."""

from skyline_ingester.publisher import Publisher
from skyline_ingester.windows import WindowStore

from .conftest import FIXTURE_TOTALS, MASTER_KEY, NOW


def test_restart_recovers_the_24h_window(publisher, backend):
    publisher.checkpoint()

    # "restart": fresh store + fresh Publisher, same backend
    store2 = WindowStore()
    publisher2 = Publisher(store2, backend, master_key=MASTER_KEY, now_fn=lambda: NOW)
    restored = publisher2.restore_checkpoint()

    assert restored > 0
    for window, total in FIXTURE_TOTALS.items():
        assert store2.merged(window, NOW).n == total
    # trending survives with counts intact (fixture is far below truncation K)
    tags = {d["tag"]: d["count"] for d in store2.build_value("trending_hashtags", "24h", NOW)["hashtags"]}
    assert tags["cachekit"] == 4 and tags["daily"] == 8


def test_cold_start_restores_nothing(backend):
    publisher = Publisher(WindowStore(), backend, now_fn=lambda: NOW)
    assert publisher.restore_checkpoint() == 0


def test_stale_buckets_are_not_restored(publisher, backend):
    publisher.checkpoint()
    store2 = WindowStore()
    publisher2 = Publisher(store2, backend, now_fn=lambda: NOW + 26 * 3600)
    publisher2.restore_checkpoint()
    assert store2.merged("24h", NOW + 26 * 3600).n == 0


def test_snapshot_truncates_per_bucket_counters(store):
    snap = store.snapshot(NOW)
    assert snap["v"] == 1 and snap["saved_at"] == int(NOW)
    for _minute, d in snap["buckets"]:
        assert len(d["tags"]) <= 20 and len(d["links"]) <= 20 and len(d["emoji"]) <= 10


def test_restore_rejects_unknown_version(store):
    assert store.restore({"v": 999, "buckets": []}, NOW) == 0
    assert store.restore({}, NOW) == 0
