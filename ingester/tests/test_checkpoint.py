"""Checkpoint/restore: a restart must not zero the 24h window."""

import logging

from skyline_ingester.publisher import Publisher
from skyline_ingester.windows import SNAPSHOT_VERSION, WindowStore

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


def test_snapshot_omits_sentiment_for_zero_knowledge(store):
    # ZK: `sent` is the cleartext source of the @cache.secure value; the plaintext
    # checkpoint must not carry it, or the backend reconstructs avg = sum / count.
    snap = store.snapshot(NOW)
    assert snap["buckets"], "fixture stream should produce buckets"
    assert all("sent" not in d for _minute, d in snap["buckets"])


def test_restore_tolerates_malformed_checkpoints(caplog):
    # A corrupt / partial checkpoint must degrade to a skip, never raise — a raise
    # here propagates through asyncio.run and crashes startup into a boot loop.
    good = int(NOW // 60)
    bad = [
        {"v": SNAPSHOT_VERSION, "buckets": "not-a-list"},
        {"v": SNAPSHOT_VERSION, "buckets": [[good]]},  # item is not a (minute, dict) pair
        {"v": SNAPSHOT_VERSION, "buckets": [[good, "not-a-dict"]]},
        {"v": SNAPSHOT_VERSION, "buckets": [["not-an-int", {}]]},
        {"v": SNAPSHOT_VERSION, "buckets": [[good, {"n": "x"}]]},  # non-numeric count
        {"v": SNAPSHOT_VERSION, "buckets": [[good, {"sent": {"en": [1.0]}}]]},  # bad sent pair
    ]
    for snap in bad:
        assert WindowStore().restore(snap, NOW) == 0  # skipped, no raise
    # a valid bucket alongside a broken one is still restored — and the skip is
    # logged, not silent: an operator must be able to see checkpoint corruption.
    mixed = {"v": SNAPSHOT_VERSION, "buckets": [[good, {"n": 5}], [good - 1, "broken"]]}
    with caplog.at_level(logging.WARNING, logger="skyline_ingester.windows"):
        assert WindowStore().restore(mixed, NOW) == 1
    assert any("corrupt checkpoint bucket" in r.getMessage() for r in caplog.records)


def test_restore_rejects_poisoned_but_valid_counter_values():
    # Panel round-2 MAJ: a structurally valid checkpoint with a non-numeric counter
    # VALUE used to pass restore() and detonate later in merged()/most_common(),
    # where the publisher's except turns it into silent misses for up to 24h.
    # It must fail at restore, skipping only the poisoned bucket.
    good = int(NOW // 60)
    poisoned = {
        "v": SNAPSHOT_VERSION,
        "buckets": [
            [good, {"n": 3, "tags": {"x": "not-a-number"}}],  # poisoned value -> skipped
            [good - 1, {"n": 2, "tags": {"ok": 2}, "langs": {1: 2}}],  # non-str key -> coerced
            [good - 2, {"n": -1_000_000}],  # negative count skews ppm -> skipped
            [good - 3, {"n": 1, "tags": {"neg": -5}}],  # negative counter value -> skipped
            [good + 10_000, {"n": 1, "tags": {"future": 1}}],  # future minute parks forever -> skipped
            [good - 4, {"n": 1, "sent": {"en": [0.5, -3]}}],  # negative sentiment count -> skipped
        ],
    }
    store = WindowStore()
    assert store.restore(poisoned, NOW) == 1
    merged = store.merged("24h", NOW)  # must never raise
    assert merged.tags.most_common(5) == [("ok", 2)]
    assert merged.langs == {"1": 2}
    assert merged.n == 2
