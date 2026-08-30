"""Checkpoint/restore: a restart must not zero the 24h window."""

import logging

from skyline_ingester.extract import PostFeatures
from skyline_ingester.policy import NORMALIZATION_VERSION
from skyline_ingester.publisher import Publisher
from skyline_ingester.windows import SNAPSHOT_VERSION, WINDOW_MINUTES, WindowStore

from .conftest import FIXTURE_TOTALS, MASTER_KEY, NOW, NOW_MIN


def _snapshot(buckets):
    return {
        "v": SNAPSHOT_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "buckets": buckets,
    }


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
    assert snap["v"] == SNAPSHOT_VERSION and snap["saved_at"] == int(NOW)
    assert snap["normalization_version"] == NORMALIZATION_VERSION
    for _minute, d in snap["buckets"]:
        assert len(d["tags"]) <= 20 and len(d["links"]) <= 20 and len(d["domains"]) <= 20 and len(d["emoji"]) <= 10


def test_restore_rejects_unknown_version(store):
    assert store.restore({"v": 999, "buckets": []}, NOW) == 0
    assert store.restore({}, NOW) == 0


def test_restore_ignores_legacy_sent():
    # ZK (panel round 3): the plaintext checkpoint is operator-poisonable, so a
    # restored `sent` would let the backend operator choose the plaintext of the
    # next @cache.secure publish. Sentiment must come from live ingestion only.
    good = int(NOW // 60)
    legacy = _snapshot([[good, {"n": 2, "sent": {"en": [999.0, 1]}}]])
    store = WindowStore()
    assert store.restore(legacy, NOW) == 1  # the bucket's counts still restore
    assert store.sentiment_value("1h", NOW)["langs"] == {}


def test_secure_sentiment_value_omits_public_checkpoint_derived_fields(store):
    value = store.sentiment_value("1h", NOW)
    assert set(value) == {"window", "generated_at", "normalization_version", "langs"}


def test_restore_checkpoint_never_crashes_startup(publisher, backend, monkeypatch):
    # The boot-loop guard end-to-end: nothing a poisoned checkpoint triggers inside
    # restore() may propagate through asyncio.run and crash startup — the bad
    # checkpoint outlives the crash (26h TTL), so a raise here loops until the TTL.
    publisher.checkpoint()
    store2 = WindowStore()
    publisher2 = Publisher(store2, backend, master_key=MASTER_KEY, now_fn=lambda: NOW)

    def boom(snap, now):
        raise RuntimeError("poisoned checkpoint detonated inside restore")

    monkeypatch.setattr(store2, "restore", boom)
    assert publisher2.restore_checkpoint() == 0


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
    structurally_bad = [
        {**_snapshot([]), "buckets": "not-a-list"},
        _snapshot([[good]]),  # item is not a (minute, dict) pair
        _snapshot([[good, "not-a-dict"]]),
        _snapshot([["not-an-int", {}]]),
    ]
    for snap in structurally_bad:
        assert WindowStore().restore(snap, NOW) == 0  # skipped, no raise
    # Invalid scalar values are dropped in place; they no longer erase the
    # otherwise recoverable minute.
    for value in ("x", float("inf")):
        store = WindowStore()
        assert store.restore(_snapshot([[good, {"n": value}]]), NOW) == 1
        merged = store.merged("24h", NOW)
        assert merged.n == 0
        assert merged.excluded["checkpoint_invalid_count"] == 1
    # a valid bucket alongside a broken one is still restored — and the skip is
    # logged, not silent: an operator must be able to see checkpoint corruption.
    mixed = _snapshot([[good, {"n": 5}], [good - 1, "broken"]])
    with caplog.at_level(logging.WARNING, logger="skyline_ingester.windows"):
        assert WindowStore().restore(mixed, NOW) == 1
    assert any("corrupt checkpoint bucket" in r.getMessage() for r in caplog.records)


def test_restore_drops_only_poisoned_entries_and_keeps_the_bucket():
    # Panel round-2 MAJ: a structurally valid checkpoint with a non-numeric counter
    # VALUE used to pass restore() and detonate later in merged()/most_common(),
    # where the publisher's except turns it into silent misses for up to 24h.
    # Each unsafe entry must be omitted without erasing unrelated minute totals.
    good = int(NOW // 60)
    poisoned = _snapshot(
        [
            [good, {"n": 3, "tags": {"x": "not-a-number"}}],  # poisoned value -> skipped
            [good - 1, {"n": 2, "tags": {"ok": 2}, "langs": {1: 2}}],  # non-str key -> coerced
            [good - 2, {"n": -1_000_000}],  # negative count skews ppm -> skipped
            [good - 3, {"n": 1, "tags": {"neg": -5}}],  # negative counter value -> skipped
            [good - 4, {"n": 1, "tags": {"nsfw": 1}}],  # filtered public tag -> skipped
            [good - 5, {"n": 1, "links": {"javascript:alert(1)": 1}}],  # unsafe URL -> skipped
            [good - 6, {"n": 1, "tags": {"safe": 1}, "tag_labels": {"safe": {"Other": 1}}}],
            [good - 7, {"n": 1, "excluded": {"invented_reason": 1}}],  # transparency poison -> skipped
            [good + 10_000, {"n": 1, "tags": {"future": 1}}],  # future minute parks forever -> skipped
        ]
    )
    store = WindowStore()
    assert store.restore(poisoned, NOW) == 8
    merged = store.merged("24h", NOW)  # must never raise
    assert merged.tags.most_common(5) == [("ok", 2), ("safe", 1)]
    assert merged.langs == {}
    assert merged.n == 10
    assert merged.excluded == {
        "checkpoint_invalid_count": 3,
        "checkpoint_invalid_exclusion": 1,
        "checkpoint_invalid_label": 1,
        "checkpoint_invalid_lang": 1,
        "checkpoint_invalid_tag": 1,
        "checkpoint_invalid_url": 1,
    }
    assert merged.signal_candidates == sum(merged.excluded.values())


def test_restore_filters_poisoned_language_and_emoji_without_losing_totals():
    good = int(NOW // 60)
    snap = _snapshot(
        [
            [
                good,
                {
                    "n": 500,
                    "langs": {"en": 450, "<script>alert(1)</script>": 50},
                    "emoji": {"🔥": 2, "CONTACT ops@evil.example": 99},
                },
            ]
        ]
    )
    store = WindowStore()
    assert store.restore(snap, NOW) == 1
    assert store.build_value("lang_mix", "5m", NOW)["langs"] == {"en": 1.0}
    assert store.build_value("top_emoji", "5m", NOW)["emoji"] == [{"emoji": "🔥", "count": 2}]
    merged = store.merged("5m", NOW)
    assert merged.n == 500
    assert merged.excluded["checkpoint_invalid_lang"] == 1
    assert merged.excluded["checkpoint_invalid_emoji"] == 1
    assert merged.signal_candidates == 2


def test_restore_filters_unsafe_aliases_without_losing_minute():
    good = int(NOW // 60)
    unsafe_uri = "http://[64:ff9b::a9fe:a9fe]/latest/meta-data/"
    unsafe_domain = "64:ff9b::a9fe:a9fe"
    unsafe_alias_uri = "http://169.254.169.254.sslip.io/latest/meta-data/"
    unsafe_alias_domain = "169.254.169.254.sslip.io"
    snap = _snapshot(
        [
            [
                good,
                {
                    "n": 500,
                    "langs": {"en": 500},
                    "emoji": {"🔥": 2},
                    "links": {unsafe_uri: 99, unsafe_alias_uri: 98},
                    "domains": {unsafe_domain: 99, unsafe_alias_domain: 98},
                },
            ]
        ]
    )
    store = WindowStore()
    assert store.restore(snap, NOW) == 1
    merged = store.merged("5m", NOW)
    assert merged.n == 500 and merged.langs == {"en": 500} and merged.emoji == {"🔥": 2}
    assert merged.links == {} and merged.domains == {}
    assert merged.excluded["checkpoint_invalid_url"] == 2
    assert merged.excluded["checkpoint_invalid_domain"] == 2


def test_restore_caps_untrusted_bucket_and_counter_cardinality():
    good = int(NOW // 60)
    tags = {f"tag{index}": index + 1 for index in range(100)}
    store = WindowStore(max_minutes=2)
    snap = _snapshot([[good, {"n": 1, "tags": tags}]] * 10)
    assert store.restore(snap, NOW) == 3
    merged = store.merged("5m", NOW)
    assert len(merged.tags) == 20
    assert merged.excluded["checkpoint_invalid_tag"] == 80

    real = {f"real{index}": index + 1 for index in range(20)}
    for invalid_count in (500, 512, 513, 600, 1_004):
        invalid = {f"#invalid{index}": 1 for index in range(invalid_count)}
        store = WindowStore()
        assert store.restore(_snapshot([[good, {"tags": {**invalid, **real}}]]), NOW) == 1
        merged = store.merged("5m", NOW)
        assert merged.tags == real
        assert merged.excluded["checkpoint_invalid_tag"] == invalid_count

    invalid_labels = {f"invalid{index}": "not-a-map" for index in range(500)}
    real_labels = {f"real{index}": {f"Real{index}": index + 1} for index in range(20)}
    store = WindowStore()
    assert store.restore(_snapshot([[good, {"tag_labels": {**invalid_labels, **real_labels}}]]), NOW) == 1
    merged = store.merged("5m", NOW)
    assert {canonical for canonical, _display in merged.tag_labels} == set(real_labels)
    assert merged.excluded["checkpoint_invalid_label"] == 500

    oversized = {f"#invalid{index}": 1 for index in range(1_005)}
    store = WindowStore()
    assert store.restore(_snapshot([[good, {"n": 6_000, "tags": {**oversized, **real}}]]), NOW) == 0
    assert store.merged("5m", NOW).n == 0

    # The tag_labels budget bounds the WHOLE structure: an outer map inside the
    # cap whose nested display maps multiply the entry count rejects the bucket,
    # instead of scheduling outer x inner NFKC validations at startup.
    nested_bomb = {f"tag{index}": {f"Display{index}-{j}": 1 for j in range(600)} for index in range(2)}
    store = WindowStore()
    assert store.restore(_snapshot([[good, {"n": 6_000, "tag_labels": nested_bomb}]]), NOW) == 0
    assert store.merged("5m", NOW).n == 0


def test_restore_rejects_overlong_emoji_keys():
    good = int(NOW // 60)
    overlong = "😀" + "‍😀" * 299
    store = WindowStore()
    assert store.restore(_snapshot([[good, {"emoji": {overlong: 30}}]]), NOW) == 1
    merged = store.merged("5m", NOW)
    assert merged.emoji == {}
    assert merged.excluded["checkpoint_invalid_emoji"] == 1


# --- Hour-coarsening (LAB-1933): aged minutes fold into hour units at snapshot time ---


def _minute_post(minute: int, *, tags=(), lang="en") -> PostFeatures:
    return PostFeatures(
        ts=minute * 60.0,
        lang=lang,
        hashtags=list(tags),
        links=[],
        emoji=[],
        sentiment=None,
        hashtag_labels={tag: tag for tag in tags},
    )


def _day_store(span_minutes: int = WINDOW_MINUTES["24h"]) -> WindowStore:
    """One bucket per retained minute over `span_minutes`, distinct source per event.

    At the full 24h span the newest add prunes the oldest minute (the age floor
    is `newest - max`), so the store retains span buckets, not span + 1.
    """
    store = WindowStore()
    for offset in range(span_minutes, -1, -1):
        minute = NOW_MIN - offset
        store.add(_minute_post(minute, tags=(f"t{minute % 7}",)), source_id=f"did:plc:{offset}")
    return store


def test_snapshot_coarsens_aged_buckets_to_hour_units():
    # THE bound this ticket delivers: a full 24h window serializes as ~85 units
    # (last hour minute-keyed + one unit per aged hour), not ~1,441 — the size
    # cut that was sized against Render's 5 GB/month egress allowance. The lab
    # k3s cluster is unmetered, but the bound still holds and still matters:
    # it is what keeps the checkpoint small enough to write every 300 s.
    store = _day_store()
    snap = store.snapshot(NOW)
    minutes = [minute for minute, _d in snap["buckets"]]
    coarse_floor = NOW_MIN - WINDOW_MINUTES["1h"]
    fresh = [m for m in minutes if m > coarse_floor]
    coarse = [m for m in minutes if m <= coarse_floor]
    assert len(fresh) == WINDOW_MINUTES["1h"]  # the live hour stays minute-keyed
    # Retained span starts one past the age floor: the newest add pruned the
    # oldest minute (see _day_store), so the range must not include it.
    expected_hours = {m // 60 for m in range(NOW_MIN - WINDOW_MINUTES["24h"] + 1, coarse_floor + 1)}
    assert len(coarse) == len(expected_hours) <= 25
    assert all(m % 60 == 0 for m in coarse), "hour units must be keyed at the hour floor"
    assert len(minutes) == len(set(minutes)), "hour keys must not collide with fresh minutes"


def test_coarsened_round_trip_preserves_window_totals_exactly():
    # Aggregates only ever SUM buckets, so folding an hour into one unit keyed
    # inside the window changes no 24h total; 5m/1h restore minute-exact. The
    # store spans 23h so every hour floor is in-window — the full-24h tail
    # (whose oldest hour has partially slid out) is pinned by the
    # expires-early-never-late test below, not smoothed over here.
    span = WINDOW_MINUTES["24h"] - 60
    store = _day_store(span)
    # Mint a real exclusion in the AGED region so the excluded assert below is
    # non-vacuous: the same source repeating a tag inside the dedupe horizon
    # yields duplicate_source_tag, which must survive the hour fold.
    for _repeat in range(2):
        store.add(_minute_post(NOW_MIN - 100, tags=("dup",)), source_id="did:plc:dup")
    before = store.merged("24h", NOW)
    assert before.excluded == {"duplicate_source_tag": 1}
    snap = store.snapshot(NOW)

    restored = WindowStore()
    assert restored.restore(snap, NOW) == len(snap["buckets"])
    after = restored.merged("24h", NOW)
    assert after.n == before.n == span + 3
    assert after.signal_candidates == before.signal_candidates
    assert after.excluded == before.excluded
    assert after.tags == before.tags  # 7 distinct tags, all inside every top-K
    for window in ("5m", "1h"):
        assert restored.merged(window, NOW).n == store.merged(window, NOW).n

    # Re-checkpointing a restored store is stable: hour units re-fold into the
    # same hour, so a restart chain cannot drift the checkpoint shape or size.
    snap2 = restored.snapshot(NOW)
    assert [m for m, _d in snap2["buckets"]] == [m for m, _d in snap["buckets"]]


def test_coarsened_restore_expires_the_tail_early_never_late():
    # The recovery-semantics tradeoff, pinned: an hour unit is keyed at the hour
    # FLOOR, so after a restore the 24h trailing edge drops events up to 59 min
    # early — and never serves an event older than 24h as in-window.
    hour = NOW_MIN // 60 - 3
    event_min = hour * 60 + 59  # aged (>1h old at NOW), last minute of its hour
    store = WindowStore()
    store.add(_minute_post(event_min, tags=("edge",)), source_id="did:plc:edge")
    snap = store.snapshot(NOW)
    assert [m for m, _d in snap["buckets"]] == [hour * 60]

    restored = WindowStore()
    assert restored.restore(snap, NOW) == 1
    window = WINDOW_MINUTES["24h"]
    # Inside the hour-step bound the event is still served...
    assert restored.merged("24h", (event_min + window - 60) * 60.0).tags["edge"] == 1
    # ...up to 59 min early it may be gone (here: 40 min before its true expiry)...
    assert "edge" not in restored.merged("24h", (event_min + window - 40) * 60.0).tags
    # ...and once truly out of window it can never reappear.
    assert "edge" not in restored.merged("24h", (event_min + window + 1) * 60.0).tags
