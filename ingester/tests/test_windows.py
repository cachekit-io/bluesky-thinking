"""Window aggregation and expiry against the recorded fixture stream."""

import sys
import threading
import time
from collections import Counter

from skyline_ingester.extract import PostFeatures
from skyline_ingester.jetstream import MAX_FUTURE_SKEW_SECONDS
from skyline_ingester.policy import EXCLUSION_REASONS, NORMALIZATION_VERSION
from skyline_ingester.windows import (
    _COMPACT_LANGS,
    _FULL_FIDELITY_MINUTES,
    _K_DOMAINS,
    _K_EMOJI,
    _K_LINKS,
    _K_TAGS,
    _MAX_FUTURE_SKEW_MINUTES,
    MAX_SOURCE_LEDGER_ENTRIES,
    MAX_SOURCE_LEDGER_ENTRIES_PER_SOURCE,
    SNAPSHOT_VERSION,
    SOURCE_DEDUPE_SECONDS,
    WINDOW_MINUTES,
    Bucket,
    WindowStore,
)

from .conftest import FIXTURE_TOTALS, NOW, NOW_MIN


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


# --- LAB-1775: age-based compaction bounds the retained window -----------------


def _post(minute, *, tags=(), links=(), domains=(), lang="en", sentiment=None, labels=None):
    # domains is explicit, not derived from links: add() spends one ledger entry
    # per domain the extractor supplies, so a test that omits them measures a
    # cheaper post than production sends.
    return PostFeatures(
        ts=minute * 60.0,
        lang=lang,
        hashtags=list(tags),
        links=list(links),
        emoji=[],
        sentiment=sentiment,
        domains=list(domains),
        hashtag_labels=labels if labels is not None else {tag: tag for tag in tags},
    )


def _fill(store, minute, count, *, prefix="tag"):
    for index in range(count):
        store.add(_post(minute, tags=[f"{prefix}{index}"]), source_id=f"did:plc:{prefix}{minute}-{index}")


def _age_out(store, minute):
    """Create a newer bucket so `minute` falls past the full-fidelity window."""
    store.add(_post(minute + _FULL_FIDELITY_MINUTES + 1, tags=["trigger"]), source_id="did:plc:trigger")


def test_aged_buckets_compact_while_the_5m_window_stays_exact():
    # The whole LAB-1775 fix in one assertion: resident cost is
    # (retained minutes x keys per minute), so the 1,435 minutes nobody reads at
    # full fidelity get truncated and the 5 the live view reads do not.
    store = WindowStore()
    base = NOW_MIN - 10
    for offset in range(11):
        _fill(store, base + offset, 200, prefix=f"m{offset}t")
    with store._lock:
        sizes = {minute: len(bucket.tags) for minute, bucket in store._buckets.items()}
    newest = max(sizes)
    aged = {minute: size for minute, size in sizes.items() if minute <= newest - _FULL_FIDELITY_MINUTES}
    live = {minute: size for minute, size in sizes.items() if minute > newest - _FULL_FIDELITY_MINUTES}
    assert aged and live
    assert set(aged.values()) == {_K_TAGS}
    assert set(live.values()) == {200}


def test_compaction_keeps_the_most_frequent_keys_with_exact_counts():
    # Arrival order is the INVERSE of frequency here: an insert-time cap (the
    # alternative mitigation) keeps the first K keys and would retain exactly
    # the wrong twenty. Compaction is frequency-ordered, so it keeps the right
    # ones AND their counts stay exact — truncation only ever drops keys.
    store = WindowStore()
    aged = NOW_MIN - 10
    for index in range(60):
        for repeat in range(index + 1):
            store.add(_post(aged, tags=[f"tag{index:02d}"]), source_id=f"did:plc:{index}-{repeat}")
    _age_out(store, aged)
    with store._lock:
        survivors = dict(store._buckets[aged].tags)
    assert set(survivors) == {f"tag{index:02d}" for index in range(40, 60)}
    assert survivors["tag59"] == 60
    assert survivors["tag40"] == 41


def test_compaction_never_touches_the_exact_aggregates():
    # posts_per_minute and the exclusion denominators are exact in every window,
    # above the cap as well as below it: compaction drops counter KEYS and never
    # n, signal_candidates or excluded.
    store = WindowStore()
    base = NOW_MIN - 30
    posts = 0
    for offset in range(31):
        minute = base + offset
        for index in range(50):
            store.add(
                _post(minute, tags=[f"t{minute}x{index}"], links=[f"https://e{index}.example.com/{minute}"]),
                source_id=f"did:plc:{minute}-{index}",
            )
            posts += 1
    now = (base + 30) * 60.0 + 59
    merged = store.merged("1h", now)
    assert merged.n == posts
    assert merged.signal_candidates == posts * 2
    assert store.build_value("posts_per_minute", "1h", now)["ppm"] == round(posts / 60, 3)


def test_an_aged_bucket_that_regrows_is_recompacted():
    # Compaction is deliberately stateless. With an "already compacted" flag, a
    # late but in-skew event landing in an aged minute would let that bucket
    # grow unbounded for the rest of the 24h retention — the same leak, just
    # harder to find.
    store = WindowStore()
    aged = NOW_MIN - 10
    _fill(store, aged, 200, prefix="a")
    _age_out(store, aged)
    with store._lock:
        assert len(store._buckets[aged].tags) == _K_TAGS
    _fill(store, aged, 200, prefix="b")
    with store._lock:
        assert len(store._buckets[aged].tags) > _K_TAGS
    store.add(_post(aged + _FULL_FIDELITY_MINUTES + 2, tags=["trigger2"]), source_id="did:plc:trigger2")
    with store._lock:
        assert len(store._buckets[aged].tags) == _K_TAGS


def test_compaction_bounds_langs_and_sent_together():
    # sent is keyed by language, so leaving it out of compaction would just move
    # the unbounded axis one field to the right (the round-9 lesson).
    store = WindowStore()
    aged = NOW_MIN - 10
    for index in range(200):
        lang = f"x{chr(97 + index // 26)}{chr(97 + index % 26)}"
        store.add(_post(aged, lang=lang, sentiment=0.5), source_id=f"did:plc:l{index}")
    _age_out(store, aged)
    with store._lock:
        bucket = store._buckets[aged]
        # Literal, not just the constant: `== _COMPACT_LANGS` alone still passes
        # if someone raises it past the 200-language fill, i.e. with langs
        # compaction effectively switched off (panel finding).
        assert len(bucket.langs) == 32 == _COMPACT_LANGS
        assert set(bucket.sent) <= set(bucket.langs)


def test_compaction_drops_display_labels_to_canonical_never_to_a_wrong_one():
    store = WindowStore()
    aged = NOW_MIN - 10
    for index in range(60):
        tag = f"tag{index:02d}"
        for repeat in range(index + 1):
            store.add(_post(aged, tags=[tag], labels={tag: tag.upper()}), source_id=f"did:plc:{index}-{repeat}")
    _age_out(store, aged)
    with store._lock:
        bucket = store._buckets[aged]
        assert len(bucket.tag_labels) <= len(bucket.tags)
        assert {canonical for canonical, _display in bucket.tag_labels} <= set(bucket.tags)
    for entry in store.build_value("trending_hashtags", "24h", (aged + 6) * 60.0 + 59)["hashtags"]:
        assert entry["display"] in (entry["tag"], entry["tag"].upper())


def test_full_ledger_stops_new_counter_keys_from_being_minted(monkeypatch):
    # The head of the window (the uncompacted minutes) is bounded by the GLOBAL
    # contribution ledger, not by a second cap: at most MAX_SOURCE_LEDGER_ENTRIES
    # accepted contributions per SOURCE_DEDUPE_SECONDS. Raising that constant
    # without redoing the memory arithmetic reopens LAB-1775, so pin the
    # behaviour rather than the number.
    #
    # Count EVERY family the accepted contribution mints, not just tags: one
    # accepted hashtag mints two retained keys (tags and tag_labels), so the
    # ledger's entry budget is NOT a one-for-one key budget. Asserting on tags
    # alone hid a 2x undercount in the memory arithmetic this test exists to
    # protect — tags + tag_labels are 56% of live keys at measured rates.
    ledger = 16
    monkeypatch.setattr("skyline_ingester.windows.MAX_SOURCE_LEDGER_ENTRIES", ledger)
    store = WindowStore()
    for index in range(200):
        store.add(_post(NOW_MIN, tags=[f"t{index}"]), source_id=f"did:plc:{index}")
    with store._lock:
        bucket = store._buckets[NOW_MIN]
        assert len(bucket.tags) == ledger
        assert len(bucket.tag_labels) == ledger
        # The whole retained cost of a full ledger, stated as one number: two
        # key families per accepted tag, plus the single "en" lang key.
        minted = len(bucket.tags) + len(bucket.tag_labels) + len(bucket.langs)
        assert minted == 2 * ledger + 1


def test_a_concentrated_ledger_mints_keys_in_every_eligible_family(monkeypatch):
    # The sibling case: a real post carries a tag, a link and a domain, so one
    # post spends THREE ledger entries and mints FOUR retained keys. A test that
    # only ever offers hashtags measures a third of the per-post key cost and
    # makes the head's budget look three times roomier than it is.
    ledger = 30
    monkeypatch.setattr("skyline_ingester.windows.MAX_SOURCE_LEDGER_ENTRIES", ledger)
    store = WindowStore()
    for index in range(200):
        store.add(
            _post(
                NOW_MIN,
                tags=[f"t{index}"],
                links=[f"https://e{index}.example.com/{index}"],
                domains=[f"e{index}.example.com"],
            ),
            source_id=f"did:plc:{index}",
        )
    with store._lock:
        bucket = store._buckets[NOW_MIN]
        accepted = len(bucket.tags) + len(bucket.links) + len(bucket.domains)
        assert accepted == ledger, "the ledger bounds accepted CONTRIBUTIONS across families, not per family"
        # Keys outrun ledger entries: tag_labels rides along on every tag.
        assert accepted + len(bucket.tag_labels) > ledger


def test_stats_reports_live_sizes():
    store = WindowStore()
    _fill(store, NOW_MIN, 5)
    stats = store.stats()
    assert stats["buckets"] == 1
    assert stats["ledger_entries"] == 5
    # 5 tags + 5 (tag, label) pairs + 1 lang
    assert stats["counter_keys"] == 11


def test_compaction_can_never_reach_into_the_live_5m_window():
    # Executable oracle for a cross-module coupling, derived rather than
    # restated: _prune anchors the compaction floor on the minute being ADDED
    # (deliberately — a far-future stamp must never become a permanent retention
    # anchor), so the horizon has to clear the largest future skew ingest_raw
    # accepts. Rather than assert the constants against each other, derive the
    # worst reachable floor from the skew and check it against the oldest minute
    # merged("5m") actually reads — the property that matters.
    assert _MAX_FUTURE_SKEW_MINUTES * 60 >= MAX_FUTURE_SKEW_SECONDS

    # Sweep every sub-minute alignment: minute rounding, not just the raw
    # seconds, decides how far ahead an accepted event's bucket can land.
    ahead = max(
        int((second + MAX_FUTURE_SKEW_SECONDS) // 60) - int(second // 60)
        for second in range(60)  # wall-clock second within the current minute
    )
    worst_compact_floor = ahead - _FULL_FIDELITY_MINUTES  # relative to now_min
    oldest_minute_in_5m = -WINDOW_MINUTES["5m"] + 1  # merged reads (now-5, now]
    assert worst_compact_floor < oldest_minute_in_5m, (
        f"compaction floor reaches now_min{worst_compact_floor:+d}, but the 5m window starts at now_min{oldest_minute_in_5m:+d}"
    )


def test_one_future_dated_post_cannot_truncate_the_live_5m_window():
    # Panel CRIT (LAB-1775): jetstream accepts events up to MAX_FUTURE_SKEW_SECONDS
    # ahead, so before the horizon carried slack a SINGLE such post dragged
    # compact_floor into the live window — measured 1,000 -> 100 distinct tags,
    # repeatable every minute, silently falsifying "the 5m window is bit-exact".
    def build():
        store = WindowStore()
        for offset in range(WINDOW_MINUTES["5m"]):
            _fill(store, NOW_MIN - 4 + offset, 200, prefix=f"m{offset}t")
        return store

    now = NOW_MIN * 60.0 + 59
    baseline = len(build().merged("5m", now).tags)
    assert baseline == 200 * WINDOW_MINUTES["5m"]

    hostile = build()
    skewed = NOW_MIN + (MAX_FUTURE_SKEW_SECONDS // 60)
    hostile.add(_post(skewed, tags=["evil"]), source_id="did:plc:attacker")
    assert len(hostile.merged("5m", now).tags) == baseline


def test_descending_stale_timestamps_cannot_grow_the_bucket_map():
    # The mirror image of the future-skew CRIT above, and the reason add() has a
    # staleness floor at all. ingest_raw bounds event time from ABOVE
    # (MAX_FUTURE_SKEW_SECONDS) but from below only by time_us >= 0, while
    # _prune's retention floor is anchored on the minute being ADDED — so an
    # OLDER minute lowered the floor instead of being caught by it. A descending
    # run of stale timestamps therefore minted one retained bucket per minute
    # that no later prune could ever reach, and snapshot() copies every bucket
    # into the checkpoint: unbounded on exactly the axis this module bounds.
    #
    # Asserted as the invariant, not a count: every add() either leaves a
    # retained bucket behind or is refused and counted, and the retained set
    # never outgrows the horizon. Pre-fix this retained all 500.
    total = 500
    store = WindowStore(max_minutes=60)
    store.add(_post(NOW_MIN, tags=["live"]), source_id="did:plc:live")
    for step in range(1, total):
        store.add(_post(NOW_MIN - step, tags=[f"s{step}"]), source_id=f"did:plc:s{step}")
    # The window, plus the future-skew slack ingest_raw can legitimately fill.
    limit = store._max + _MAX_FUTURE_SKEW_MINUTES + 1
    with store._lock:
        retained = len(store._buckets)
        oldest = min(store._buckets)
    assert retained <= limit, f"{retained} buckets retained against a {limit}-bucket cap"
    # The stale run evicted itself, not the live minute it arrived behind.
    assert oldest >= NOW_MIN - limit
    assert NOW_MIN in store._buckets
    # Evictions are counted, never silent: a feed replaying past the horizon is
    # otherwise indistinguishable from a feed that went quiet.
    assert store.stats()["evicted_buckets"] == total - retained


def test_backfill_inside_the_horizon_is_still_accepted():
    # Guards the staleness floor against over-rejecting: out-of-order events
    # within the retention window are normal Jetstream behaviour, and dropping
    # them would silently lose real posts to fix a hostile-input bug.
    store = WindowStore(max_minutes=60)
    store.add(_post(NOW_MIN, tags=["live"]), source_id="did:plc:live")
    store.add(_post(NOW_MIN - 30, tags=["backfill"]), source_id="did:plc:backfill")
    with store._lock:
        assert NOW_MIN - 30 in store._buckets
    assert store.stats()["evicted_buckets"] == 0


def test_checkpoint_round_trip_preserves_the_compaction_language_bound():
    # The live store and the checkpoint must agree on how many languages a
    # bucket keeps. They did not: live kept 32, a checkpoint round-trip silently
    # dropped it to 15 — on the restart path this whole ticket exists to
    # survive, and against two doc surfaces that state 32 (panel finding).
    store = WindowStore()
    aged = NOW_MIN - 20
    for index in range(200):
        lang = f"x{chr(97 + index // 26)}{chr(97 + index % 26)}"
        store.add(_post(aged, lang=lang), source_id=f"did:plc:l{index}")
    _age_out(store, aged)
    with store._lock:
        live_langs = len(store._buckets[aged].langs)
    assert live_langs == _COMPACT_LANGS

    now = (aged + _FULL_FIDELITY_MINUTES + 1) * 60.0
    restored = WindowStore()
    assert restored.restore(store.snapshot(now), now) >= 1
    with restored._lock:
        assert len(restored._buckets[aged].langs) == live_langs


def test_a_full_24h_window_stays_inside_its_absolute_key_budget():
    # THE bound this ticket delivers, asserted as an absolute number rather than
    # against the constants that produce it. `len(x) == _K_TAGS` passes for any
    # _K_TAGS; this fails if any per-family K is raised, if a new Bucket counter
    # family is added uncapped, or if compaction is disabled outright.
    #
    # The per-bucket budget is DERIVED from the compaction constants rather than
    # hand-picked, so raising any K moves the budget with it and the assertion
    # keeps measuring the thing it claims to. Every family _compact_bucket
    # truncates is listed; tag_labels keeps at most one spelling per surviving
    # tag, and sent inherits langs' bound.
    # Measured at this shape: 41.6 MiB steady, 48.0 MiB at the 24h merge peak.
    budget_per_bucket = (
        _K_TAGS  # tags
        + _K_TAGS  # tag_labels: one surviving spelling per surviving tag
        + _K_LINKS
        + _K_DOMAINS
        + _K_EMOJI
        + _COMPACT_LANGS  # langs
        + _COMPACT_LANGS  # sent, keyed by language
        + len(EXCLUSION_REASONS)  # excluded is never truncated, but it is finite
    )
    # Deriving the budget would be tautological on its own — raise a K and the
    # budget rises with it. So pin the DERIVED sum against the absolute number
    # the 512 MiB arithmetic was actually done against (200 keys/bucket x 1440
    # buckets x ~299 B/key = ~82 MiB of retained counters). Raising any K, or
    # adding a key family, fails here rather than as a production OOMKill.
    assert budget_per_bucket <= 200, (
        f"compaction constants now admit {budget_per_bucket} keys/bucket; redo the memory arithmetic before raising this"
    )
    store = WindowStore()
    newest = NOW_MIN
    for offset in range(WINDOW_MINUTES["24h"]):
        minute = newest - WINDOW_MINUTES["24h"] + 1 + offset
        bucket = Bucket(n=1200, signal_candidates=4000)
        for index in range(60):  # 60 distinct keys per family, i.e. above every K
            bucket.tags[f"tag{minute}x{index}"] = index + 1
            bucket.links[f"https://e{index}.example.com/{minute}"] = index + 1
            bucket.domains[f"e{minute}x{index}.example.com"] = index + 1
            bucket.emoji[f"e{minute}x{index}"] = index + 1
            bucket.langs[f"g{minute}x{index}"] = index + 1
            bucket.tag_labels[(f"tag{minute}x{index}", f"Tag{index}")] = index + 1
            bucket.sent[f"g{minute}x{index}"] = [0.5, 1]
        store._buckets[minute] = bucket
    with store._lock:
        store._prune(newest)  # the pass that compacts everything past the horizon

    keys = store.stats()["counter_keys"]
    assert keys <= budget_per_bucket * WINDOW_MINUTES["24h"], f"retained {keys} counter keys"


def test_the_uncompacted_head_stays_inside_a_ledger_derived_key_budget(monkeypatch):
    # The head keeps every distinct key, so its budget is not a free parameter —
    # it is the global contribution ledger, and the arithmetic in windows.py
    # rests on it. Derived from the constant and driven through add() rather than
    # hand-built buckets, so the path that actually mints keys is the path under
    # test: a ledger raise, or a new key family riding along on an accepted
    # contribution, fails here instead of as a production OOMKill.
    #
    # Two keys per accepted contribution, not one: an accepted hashtag mints its
    # tags entry AND its tag_labels entry. That factor was missing from the
    # original arithmetic.
    ledger = 500
    monkeypatch.setattr("skyline_ingester.windows.MAX_SOURCE_LEDGER_ENTRIES", ledger)
    store = WindowStore()
    for offset in range(_FULL_FIDELITY_MINUTES):
        minute = NOW_MIN - offset
        for index in range(400):
            store.add(
                _post(
                    minute,
                    tags=[f"t{offset}x{index}"],
                    links=[f"https://e{index}.example.com/{offset}"],
                    domains=[f"e{index}.example.com"],
                ),
                source_id=f"did:plc:{offset}x{index}",
            )
    with store._lock:
        assert len(store._buckets) == _FULL_FIDELITY_MINUTES, "nothing should have compacted or been evicted"
    # Per bucket the head also carries one lang key and, once the ledger starts
    # refusing, the public exclusion counters — both finite and independent of load.
    budget = 2 * ledger + _FULL_FIDELITY_MINUTES * (1 + len(EXCLUSION_REASONS))
    keys = store.stats()["counter_keys"]
    assert keys <= budget, f"head retained {keys} keys against a ledger-derived budget of {budget}"
