"""Window aggregation and expiry against the recorded fixture stream."""

import sys
import threading
import time
from collections import Counter

from skyline_ingester.extract import PostFeatures
from skyline_ingester.policy import NORMALIZATION_VERSION
from skyline_ingester.windows import (
    MAX_SOURCE_LEDGER_ENTRIES,
    MAX_SOURCE_LEDGER_ENTRIES_PER_SOURCE,
    SNAPSHOT_VERSION,
    SOURCE_DEDUPE_SECONDS,
    WindowStore,
)

from .conftest import FIXTURE_TOTALS, NOW


def test_restore_bounds_backtracking_hostile_emoji_keys():
    # Round-10 CRIT: an ambiguous EMOJI_RE made the ANCHORED fullmatch in the
    # checkpoint emoji validator backtrack exponentially — 100 hostile keys
    # blocked restore() for ~7.7 s while holding the store lock, and restore()
    # runs before the health port binds, so a poisoned 26h-TTL checkpoint was
    # a permanent boot loop. Hostile shape (panel): CORE (ZWJ CORE EXT)^k + "x",
    # k=20, inside the 64-codepoint cap so MAX_EMOJI_LENGTH cannot mitigate it.
    hostile = {chr(0x1F300 + index) + "‍\U0001f600\U0001f3fb" * 20 + "x": 1 for index in range(100)}
    snap = {
        "v": SNAPSHOT_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "saved_at": int(NOW),
        "buckets": [[int(NOW // 60), {"n": 1, "emoji": hostile}]],
    }
    store = WindowStore()
    start = time.perf_counter()
    assert store.restore(snap, NOW) == 1
    elapsed = time.perf_counter() - start
    # Pre-fix ~7.7 s, post-fix milliseconds; the slack keeps slow CI green
    # while still failing an exponential regex by a factor of ~4.
    assert elapsed < 2.0, f"restore took {elapsed:.2f}s on backtracking-hostile emoji keys"
    merged = store.merged("5m", NOW)
    assert merged.excluded["checkpoint_invalid_emoji"] == 100


def test_trending_hashtags_per_window(store):
    v5 = store.build_value("trending_hashtags", "5m", NOW)
    top = {d["tag"]: d["count"] for d in v5["hashtags"]}
    assert top["cachekit"] == 3
    assert top["bluesky"] == 3
    assert "onehour" not in top  # 30 min old — outside 5m

    v1h = store.build_value("trending_hashtags", "1h", NOW)
    top1h = {d["tag"]: d["count"] for d in v1h["hashtags"]}
    assert top1h["onehour"] == 3
    assert top1h["cachekit"] == 4  # 3 recent + 1 in the hourly band

    v24 = store.build_value("trending_hashtags", "24h", NOW)
    top24 = {d["tag"]: d["count"] for d in v24["hashtags"]}
    assert top24["daily"] == 8  # 5 via facets + 3 via the regex fallback
    assert "ancient" not in top24  # 25 h old — outside 24h


def test_trending_links(store):
    links = {d["uri"]: d["count"] for d in store.build_value("trending_links", "5m", NOW)["links"]}
    assert links == {"https://example.com/a": 3}  # 2 link facets + 1 external embed
    hourly = {d["uri"]: d["count"] for d in store.build_value("trending_links", "1h", NOW)["links"]}
    assert hourly["https://example.com/hourly"] == 2


def test_lang_mix_shares_sum_to_one(store):
    value = store.build_value("lang_mix", "5m", NOW)
    langs = value["langs"]
    assert value["total_posts"] == FIXTURE_TOTALS["5m"]
    assert set(langs) == {"en", "ja", "es", "und"}
    assert abs(sum(langs.values()) - 1.0) < 0.01
    assert langs["en"] == 0.5  # 6 of 12


def test_posts_per_minute(store):
    assert store.build_value("posts_per_minute", "5m", NOW)["ppm"] == FIXTURE_TOTALS["5m"] / 5
    assert store.build_value("posts_per_minute", "1h", NOW)["ppm"] == round(FIXTURE_TOTALS["1h"] / 60, 3)


def test_top_emoji(store):
    emoji = {d["emoji"]: d["count"] for d in store.build_value("top_emoji", "5m", NOW)["emoji"]}
    assert emoji["🔥"] == 3
    assert emoji["👨‍👩‍👧"] == 1  # ZWJ family counted as one emoji


def test_expired_source_cleanup_is_bounded_per_event(monkeypatch):
    store = WindowStore(dedupe_key=b"x" * 32)
    store._seen = {index.to_bytes(16): -300.0 for index in range(5_000)}
    store._seen_expiry = [(0.0, index.to_bytes(16)) for index in range(5_000)]
    monkeypatch.setattr("skyline_ingester.windows.time.monotonic", lambda: 1.0)
    store.add(PostFeatures(NOW, "en", [], [], [], None), source_id="did:plc:test")
    assert len(store._seen) == 904
    assert len(store._seen_expiry) == 904


def test_tag_labels_use_one_flat_counter_per_bucket(store):
    assert store._buckets
    for bucket in store._buckets.values():
        assert isinstance(bucket.tag_labels, Counter)
        assert all(isinstance(key, tuple) and len(key) == 2 for key in bucket.tag_labels)
        assert all(isinstance(count, int) for count in bucket.tag_labels.values())


def test_global_ledger_cap_refuses_new_contributions(monkeypatch):
    monkeypatch.setattr("skyline_ingester.windows.MAX_SOURCE_LEDGER_ENTRIES", 3)
    store = WindowStore(dedupe_key=b"x" * 32)
    # Distinct sources: the global cap must refuse the newcomer, never evict a
    # live tuple — eviction both re-credited an already-counted signal and
    # refilled its source's per-source budget (round-10 CRIT).
    for index in range(3):
        assert store._accept_signal(bytes([index]) * 16, "tag", f"tag{index}", float(index)) is None
    assert store._accept_signal(bytes([3]) * 16, "tag", "tag3", 3.0) == "rate_limited_global_tag"
    assert len(store._seen) == 3
    assert len(store._seen_expiry) == 3
    # Every pre-cap tuple is still live: replays stay denied inside the horizon.
    for index in range(3):
        assert store._accept_signal(bytes([index]) * 16, "tag", f"tag{index}", 4.0) == "duplicate_source_tag"
    # Expiry (not eviction) frees capacity for new contributions.
    later = SOURCE_DEDUPE_SECONDS + 5.0
    store._expire_seen(later)
    assert store._accept_signal(bytes([3]) * 16, "tag", "tag3", later) is None


def test_global_cap_pressure_cannot_recredit_capped_source(monkeypatch):
    # Round-10 CRIT reproduction (frozen clock, so nothing expires): source A
    # fills its per-source cap, 600 other DIDs push the global cap, and A's
    # live tuples must NOT be re-credited nor its per-source budget refilled.
    monkeypatch.setattr("skyline_ingester.windows.MAX_SOURCE_LEDGER_ENTRIES_PER_SOURCE", 8)
    monkeypatch.setattr("skyline_ingester.windows.MAX_SOURCE_LEDGER_ENTRIES", 50)
    monkeypatch.setattr("skyline_ingester.windows.time.monotonic", lambda: 100.0)
    store = WindowStore(dedupe_key=b"x" * 32)

    def post(tags: list[str], source: str) -> None:
        store.add(
            PostFeatures(ts=NOW, lang="en", hashtags=tags, links=[], emoji=[], sentiment=None),
            source_id=source,
        )

    for index in range(8):
        post([f"a{index}"], "did:plc:capped")
    post(["a-overflow"], "did:plc:capped")  # 9th -> rate_limited_source_tag
    for index in range(600):
        post([f"flood{index}"], f"did:plc:flood{index}")
    # A's live tuples survived the flood: replaying every accepted tag is
    # still a duplicate, and A's per-source budget was not refilled.
    for index in range(8):
        post([f"a{index}"], "did:plc:capped")
    post(["a-still-capped"], "did:plc:capped")

    merged = store.merged("5m", NOW)
    for index in range(8):
        assert merged.tags[f"a{index}"] == 1
    assert "a-overflow" not in merged.tags and "a-still-capped" not in merged.tags
    assert merged.excluded["duplicate_source_tag"] == 8
    assert merged.excluded["rate_limited_source_tag"] == 2
    assert merged.excluded["rate_limited_global_tag"] == 600 - (50 - 8)
    assert sum(merged.tags.values()) == 50


def test_per_source_ledger_cap_refuses_instead_of_evicting(monkeypatch):
    assert MAX_SOURCE_LEDGER_ENTRIES_PER_SOURCE == 1_024
    assert MAX_SOURCE_LEDGER_ENTRIES == 100_000
    monkeypatch.setattr("skyline_ingester.windows.MAX_SOURCE_LEDGER_ENTRIES_PER_SOURCE", 2)
    store = WindowStore(dedupe_key=b"x" * 32)
    campaign = b"a" * 16
    attacker = b"x" * 16
    assert store._accept_signal(campaign, "url", "https://campaign.example/a", 1.0) is None
    # The attacker's own cap refuses further inserts; nothing is evicted, so a
    # source can never free its earlier tuples (its own or anyone else's) with junk.
    assert store._accept_signal(attacker, "tag", "junk0", 1.0) is None
    assert store._accept_signal(attacker, "tag", "junk1", 1.0) is None
    assert store._accept_signal(attacker, "tag", "junk2", 1.0) == "rate_limited_source_tag"
    assert store._accept_signal(attacker, "tag", "junk0", 1.0) == "duplicate_source_tag"
    assert len(store._seen_by_source[attacker]) == 2
    assert store._accept_signal(campaign, "url", "https://campaign.example/a", 1.0) == "duplicate_source_url"
    assert len(store._seen) == 3
    # Expiry frees per-source capacity: the bound is a rate, not a lifetime total.
    later = 1.0 + SOURCE_DEDUPE_SECONDS
    store._expire_seen(later)
    assert store._accept_signal(attacker, "tag", "junk2", later) is None


def test_source_cannot_flush_own_ledger_to_replay_a_signal(monkeypatch):
    # Round-9 CRIT reproducer: with own-oldest eviction, 40 boost posts
    # interleaved with junk each re-credited the same tag (count 40). With
    # refuse-at-cap the boost tuple survives and the count stays 1.
    monkeypatch.setattr("skyline_ingester.windows.MAX_SOURCE_LEDGER_ENTRIES_PER_SOURCE", 4)
    store = WindowStore(dedupe_key=b"x" * 32)
    junk_index = 0
    for _round in range(40):
        store.add(
            PostFeatures(ts=NOW, lang="en", hashtags=["boostme"], links=[], emoji=[], sentiment=None),
            source_id="did:plc:booster",
        )
        for _ in range(33):
            store.add(
                PostFeatures(ts=NOW, lang="en", hashtags=[f"junk{junk_index}"], links=[], emoji=[], sentiment=None),
                source_id="did:plc:booster",
            )
            junk_index += 1
    merged = store.merged("5m", NOW)
    assert merged.tags["boostme"] == 1
    assert merged.excluded["duplicate_source_tag"] == 39
    assert merged.excluded["rate_limited_source_tag"] == 40 * 33 - 3


def test_emoji_are_source_bounded_like_every_other_signal(store):
    # Emoji were the one family bypassing the ledger; one source repeating an
    # emoji inside the horizon must count once, and no source means no count.
    before = store.merged("5m", NOW).emoji["🔥"]
    for _ in range(5):
        store.add(
            PostFeatures(ts=NOW, lang="en", hashtags=[], links=[], emoji=["🔥"], sentiment=None),
            source_id="did:plc:emojirepeat",
        )
    store.add(PostFeatures(ts=NOW, lang="en", hashtags=[], links=[], emoji=["🔥"], sentiment=None))
    merged = store.merged("5m", NOW)
    assert merged.emoji["🔥"] == before + 1
    assert merged.excluded["duplicate_source_emoji"] == 4
    assert merged.excluded["missing_source_emoji"] == 1


def test_windows_expire(store):
    # 6 minutes later every 5m-window fixture post has aged out.
    later = NOW + 6 * 60
    assert store.merged("5m", later).n == 0
    # At +37 min the hourly band (30 min old at NOW) has left the 1h window;
    # the recent dozen (≤ 4 min old at NOW) are still inside it.
    at_37 = store.merged("1h", NOW + 37 * 60)
    assert "onehour" not in at_37.tags
    assert at_37.n == FIXTURE_TOTALS["5m"]
    # At +65 min the 1h window is empty; the 24h window still holds everything.
    assert store.merged("1h", NOW + 65 * 60).n == 0
    assert store.merged("24h", NOW + 65 * 60).n == FIXTURE_TOTALS["24h"]


def test_memo_is_not_resurrected_by_a_concurrent_add(store):
    # Regression (CodeRabbit on PR #5): merged() computes outside the lock; if an
    # add() lands mid-merge it clears the memo, and blindly re-inserting the
    # pre-add() result would serve it stale to every same-second caller. The
    # generation counter must suppress that memo insert.
    before = store.merged("5m", NOW).n

    orig = store._copy_range

    def add_mid_merge(lo, hi):
        copies = orig(lo, hi)
        store.add(
            PostFeatures(ts=NOW, lang="en", hashtags=[], links=[], emoji=[], sentiment=None),
            source_id="did:plc:midmerge",
        )
        return copies

    store._copy_range = add_mid_merge
    try:
        stale = store.merged("5m", NOW + 1)  # computed from the pre-add copies...
    finally:
        store._copy_range = orig
    assert stale.n == before
    # ...but NOT memoised: the next same-second call recomputes and sees the add.
    assert store.merged("5m", NOW + 1).n == before + 1


def test_memo_does_not_leak_across_now(store):
    a = store.merged("5m", NOW)
    b = store.merged("5m", NOW + 6 * 60)
    assert a.n == FIXTURE_TOTALS["5m"] and b.n == 0
    assert store.merged("5m", NOW).n == FIXTURE_TOTALS["5m"]  # memoised value still correct


def test_prune_drops_buckets_beyond_24h():
    s = WindowStore()
    base = 20_000_000 * 60.0
    s.add(
        PostFeatures(ts=base, lang="en", hashtags=["old"], links=[], emoji=[], sentiment=None),
        source_id="did:plc:old",
    )
    s.add(
        PostFeatures(ts=base + 1441 * 60, lang="en", hashtags=["new"], links=[], emoji=[], sentiment=None),
        source_id="did:plc:new",
    )
    assert len(s._buckets) == 1  # the 1441-min-old bucket was pruned on insert
    assert "new" in s.merged("24h", base + 1441 * 60).tags


def test_prune_recovers_after_a_future_timestamp():
    # Regression: max()-anchored pruning let one bogus far-future event set a
    # permanent retention floor that dropped every subsequent real event on insert
    # (window stuck at zero until restart). Anchoring the floor to the minute being
    # added lets the window recover; the stray future bucket is excluded by merged().
    s = WindowStore()
    base_min = 20_000_000
    s.add(
        PostFeatures(
            ts=(base_min + 10_000_000) * 60.0,
            lang="en",
            hashtags=["bogus"],
            links=[],
            emoji=[],
            sentiment=None,
        ),
        source_id="did:plc:bogus",
    )
    for source in range(3):  # distinct real sources must all register
        s.add(
            PostFeatures(ts=base_min * 60.0, lang="en", hashtags=["real"], links=[], emoji=[], sentiment=None),
            source_id=f"did:plc:real{source}",
        )
    m = s.merged("5m", base_min * 60.0)
    assert m.n == 3
    assert m.tags["real"] == 3
    assert "bogus" not in m.tags


def test_concurrent_add_and_read_is_race_free():
    # Regression: consume() calls add() on the event-loop thread while the publish/
    # checkpoint loops read via asyncio.to_thread. Unsynchronised, iterating _buckets
    # while add() inserts/prunes raised "RuntimeError: dictionary changed size during
    # iteration". A tiny GIL switch interval forces a thread hand-off mid-iteration so
    # the race is deterministic without the lock; with the lock it can never happen.
    def _post(offset: int) -> PostFeatures:
        return PostFeatures(
            ts=(20_000_000 + offset) * 60.0,
            lang="en",
            hashtags=[f"t{offset % 30}"],
            links=[],
            emoji=["🔥"],
            sentiment=0.5,
        )

    store = WindowStore(max_minutes=400)
    for i in range(400):  # seed buckets so one iteration spans several switch points
        store.add(_post(i))

    errors: list[str] = []
    start = threading.Barrier(2)

    def writer():
        start.wait()
        try:
            for i in range(5000):
                store.add(_post(400 + i))
        except Exception as exc:
            errors.append(repr(exc))

    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-9)
    # daemon: a genuinely deadlocked writer must fail the is_alive() assert
    # below, not wedge interpreter shutdown after the join times out.
    t = threading.Thread(target=writer, daemon=True)
    try:
        t.start()
        start.wait()
        for i in range(2000):
            now = (20_000_400 + i) * 60.0
            store.snapshot(now)
            store.merged("24h", now)
    except Exception as exc:
        errors.append(repr(exc))
    finally:
        t.join(timeout=10)
        sys.setswitchinterval(old_interval)

    assert not t.is_alive(), "writer thread did not finish (possible deadlock)"
    assert not errors, f"race detected: {errors[:3]}"
