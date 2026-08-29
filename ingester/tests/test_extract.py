"""Event filtering and feature extraction."""

import json

from skyline_ingester.extract import extract_post, score_sentiment
from skyline_ingester.jetstream import _advanced_cursor, ingest_raw, subscribe_url
from skyline_ingester.windows import WindowStore

from .conftest import FIXTURE_TOTALS


def _posts(fixture_lines):
    return [f for f in (extract_post(json.loads(ln)) for ln in fixture_lines) if f is not None]


def test_non_post_events_are_dropped(fixture_lines):
    events = [json.loads(ln) for ln in fixture_lines]
    kept = _posts(fixture_lines)
    # 32 fixture events: 29 post creates + identity + delete + like
    assert len(events) == 32
    assert len(kept) == 29
    dropped = [e for e in events if extract_post(e) is None]
    assert {e["kind"] for e in dropped} == {"identity", "commit"}


def test_facet_tags_and_links(fixture_lines):
    posts = _posts(fixture_lines)
    tags = [t for p in posts for t in p.hashtags]
    links = [u for p in posts for u in p.links]
    assert tags.count("cachekit") == 4  # facet tags, case-normalized
    assert "日本語" in tags
    assert links.count("https://example.com/a") == 3  # 2 link facets + 1 external embed


def test_regex_fallback_when_no_facets(fixture_lines):
    fallback = next(p for p in _posts(fixture_lines) if "textonly" in p.hashtags)
    assert fallback.hashtags == ["textonly", "bluesky"]
    assert fallback.lang == "und"  # no langs on the record


def test_lang_normalization(fixture_lines):
    langs = {p.lang for p in _posts(fixture_lines)}
    assert "ja" in langs  # "ja-JP" collapses to primary subtag
    assert "ja-jp" not in langs and "ja-JP" not in langs


def test_invalid_language_is_collapsed_to_und(fixture_lines):
    event = json.loads(fixture_lines[0])
    event["commit"]["record"]["langs"] = ["<script>alert(1)</script>"]
    features = extract_post(event)
    assert features is not None and features.lang == "und"


def test_invalid_facet_falls_back_to_text_and_candidate_work_is_bounded(fixture_lines):
    event = json.loads(fixture_lines[0])
    event["commit"]["record"]["text"] = "#body " + " #tag" * 10_000
    event["commit"]["record"]["facets"] = [{"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "#invalid"}]}]
    features = extract_post(event)
    assert features is not None
    assert features.hashtags[0] == "body"
    assert len(features.hashtags) <= 32
    assert features.exclusions["malformed_tag"] == 1
    assert features.exclusions["candidate_limit_tag"] > 0


def test_facet_and_feature_overflow_is_visible(fixture_lines):
    event = json.loads(fixture_lines[0])
    record = event["commit"]["record"]
    record["text"] = ""
    record["facets"] = [{"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": f"tag{index}"} for index in range(200)]}]
    features = extract_post(event)
    assert features is not None
    assert len(features.hashtags) == 32
    assert features.exclusions["candidate_limit_tag"] == 32

    record["facets"] = [
        {"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": f"tag{facet}_{index}"} for index in range(20)]}
        for facet in range(200)
    ]
    features = extract_post(event)
    assert features is not None
    assert len(features.hashtags) + features.exclusions["candidate_limit_tag"] == 64

    record["facets"] = [{"features": [{"$type": "app.bsky.richtext.facet#mention", "did": "did:plc:x"}]} for _ in range(100)]
    features = extract_post(event)
    assert features is not None and features.exclusions == {}

    record["facets"] = [{"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": None} for _ in range(10)]}]
    features = extract_post(event)
    assert features is not None
    assert features.exclusions == {"malformed_tag": 1}

    record["facets"] = []
    record["embed"] = {"$type": "app.bsky.embed.external", "external": {"uri": ""}}
    features = extract_post(event)
    assert features is not None
    assert features.exclusions == {"malformed_url": 1}


def test_emoji_extraction_counts_zwj_sequence_once(fixture_lines):
    family = next(p for p in _posts(fixture_lines) if "👨‍👩‍👧" in p.emoji)
    assert family.emoji == ["👨‍👩‍👧", "❤️"]

    event = json.loads(fixture_lines[0])
    at_limit = "😀" + "‍😀" * 31
    event["commit"]["record"]["text"] = at_limit
    features = extract_post(event)
    assert features is not None and features.emoji == [at_limit]

    event["commit"]["record"]["text"] = "😀" + "‍😀" * 32
    features = extract_post(event)
    assert features is not None and features.emoji == []


def test_emoji_are_deduped_and_capped_per_post(fixture_lines):
    event = json.loads(fixture_lines[0])
    event["commit"]["record"]["text"] = "😂😂😂"
    features = extract_post(event)
    assert features is not None
    assert features.emoji == ["😂"]
    assert features.exclusions["duplicate_in_event_emoji"] == 2

    # 18 distinct emoji: 16 kept, 2 charged — rotating distinct ZWJ chains
    # cannot mint hundreds of counter keys from one legal post.
    distinct = ["😀", "😁", "😂", "😃", "😄", "😅", "😆", "😇", "😈", "😉", "😊", "😋", "😌", "😍", "😎", "😏", "😐", "😑"]
    event["commit"]["record"]["text"] = "".join(distinct)
    features = extract_post(event)
    assert features is not None
    assert features.emoji == distinct[:16]
    assert features.exclusions["candidate_limit_emoji"] == 2

    # Round-10 CRIT: a beyond-cap emoji is charged candidate_limit_emoji ONCE
    # per distinct token; repeats fall to duplicate_in_event_emoji. Charging
    # per occurrence let one 4,096-character post add 4,096 to the public
    # total_signal_candidates denominator against a fixture whose total is 34.
    event["commit"]["record"]["text"] = "".join(distinct) + "😒" * 100
    features = extract_post(event)
    assert features is not None
    assert features.emoji == distinct[:16]
    assert features.exclusions["candidate_limit_emoji"] == 3
    assert features.exclusions["duplicate_in_event_emoji"] == 99


def test_text_nfkc_output_is_capped_before_feature_work(fixture_lines, monkeypatch):
    captured = []
    monkeypatch.setattr("skyline_ingester.extract.score_sentiment", lambda text: captured.append(text))
    event = json.loads(fixture_lines[0])
    event["commit"]["record"]["text"] = "ﷺ" * 4_096
    assert extract_post(event) is not None
    assert len(captured) == 1 and len(captured[0]) == 4_096


def test_sentiment_signs():
    assert score_sentiment("I love this, great day 🎉") > 0
    assert score_sentiment("I hate this, worst day 😭") < 0
    assert score_sentiment("completely neutral text") is None
    # emoji-only valence (regression: the valence sets were once one giant string)
    assert score_sentiment("🔥🎉") > 0
    assert score_sentiment("😭") < 0


def test_ingest_raw_returns_cursor_and_skips_garbage():
    store = WindowStore()
    assert ingest_raw("not json{", store) is None
    assert ingest_raw('{"kind": "commit", "time_us": 123}', store) == 123


def test_ingest_raw_advances_cursor_past_recursive_or_downstream_poison(monkeypatch):
    cursor = 123_000_000
    nested = "[" * 20_000 + "0" + "]" * 20_000
    raw = f'{{"time_us": {cursor}, "nested": {nested}}}'
    assert ingest_raw(raw, WindowStore(), now_fn=lambda: 1_000.0) == cursor
    assert ingest_raw(f'{{"time_us": {cursor},', WindowStore(), now_fn=lambda: 1_000.0) is None

    def recurse(_event):
        raise RecursionError("poison")

    monkeypatch.setattr("skyline_ingester.jetstream.extract_post", recurse)
    ordinary = json.dumps({"kind": "commit", "time_us": cursor})
    assert ingest_raw(ordinary, WindowStore(), now_fn=lambda: 1_000.0) == cursor


def test_ingest_raw_drops_future_dated_events(fixture_lines):
    # Panel round-2 CRIT: one far-future time_us sets a retention floor in _prune
    # that wipes every real bucket (and, as a cursor, would skip everything on the
    # next reconnect). Future-dated frames are dropped whole at the ingest boundary.
    import json

    from .conftest import NOW

    store = WindowStore()
    now_fn = lambda: NOW  # noqa: E731
    for line in fixture_lines:
        ingest_raw(line, store, now_fn=now_fn)
    healthy = store.merged("24h", NOW).n
    assert healthy == FIXTURE_TOTALS["24h"]

    poison = json.loads(fixture_lines[0])
    poison["time_us"] = int((NOW + 10_000_000 * 60) * 1_000_000)  # ~19 years ahead
    assert ingest_raw(json.dumps(poison), store, now_fn=now_fn) is None  # no cursor advance
    # NOW + 1 forces a fresh merge (same minute, different memo key) — asserting at
    # NOW would just re-read the memoised Bucket and pass even on a wiped store.
    assert store.merged("24h", NOW + 1).n == healthy  # window NOT wiped
    assert store.snapshot(NOW)["buckets"], "checkpoint still has the real buckets"

    # small clock skew stays acceptable
    slight = json.loads(fixture_lines[0])
    slight["time_us"] = int((NOW + 60) * 1_000_000)
    assert ingest_raw(json.dumps(slight), store, now_fn=now_fn) == slight["time_us"]


def test_fixture_totals_match_windows(store):
    from .conftest import NOW

    for window, total in FIXTURE_TOTALS.items():
        assert store.merged(window, NOW).n == total


def test_resume_cursor_never_rewinds():
    assert _advanced_cursor(None, 100) == 100
    assert _advanced_cursor(100, 1) == 100
    assert _advanced_cursor(100, 101) == 101


def test_ingest_raw_drops_astronomical_time_us_without_raising():
    # int/float on a 300+-digit int raises OverflowError, and the
    # _cursor_is_usable call sits outside every try: unguarded, one such frame
    # escapes to consume()'s blanket handler and replays forever (the cursor
    # never advances past it). It must be dropped like any other bad frame.
    huge = 10**400
    raw = json.dumps({"kind": "commit", "time_us": huge})
    assert ingest_raw(raw, WindowStore(), now_fn=lambda: 1_000.0) is None


def test_subscribe_url():
    url = subscribe_url("wss://host/subscribe")
    assert url == "wss://host/subscribe?wantedCollections=app.bsky.feed.post"
    assert subscribe_url("wss://host/subscribe", cursor=42).endswith("&cursor=42")
