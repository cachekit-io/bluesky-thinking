"""Event filtering and feature extraction."""

import json

from skyline_ingester.extract import extract_post, score_sentiment
from skyline_ingester.jetstream import ingest_raw, subscribe_url
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


def test_emoji_extraction_counts_zwj_sequence_once(fixture_lines):
    family = next(p for p in _posts(fixture_lines) if "👨‍👩‍👧" in p.emoji)
    assert family.emoji == ["👨‍👩‍👧", "❤️"]


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
    assert store.merged("24h", NOW).n == healthy  # window NOT wiped
    assert store.snapshot(NOW)["buckets"], "checkpoint still has the real buckets"

    # small clock skew stays acceptable
    slight = json.loads(fixture_lines[0])
    slight["time_us"] = int((NOW + 60) * 1_000_000)
    assert ingest_raw(json.dumps(slight), store, now_fn=now_fn) == slight["time_us"]


def test_fixture_totals_match_windows(store):
    from .conftest import NOW

    for window, total in FIXTURE_TOTALS.items():
        assert store.merged(window, NOW).n == total


def test_subscribe_url():
    url = subscribe_url("wss://host/subscribe")
    assert url == "wss://host/subscribe?wantedCollections=app.bsky.feed.post"
    assert subscribe_url("wss://host/subscribe", cursor=42).endswith("&cursor=42")
