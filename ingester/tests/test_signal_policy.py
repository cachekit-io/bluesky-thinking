"""Normalization, safety, source bounding, and transparency policy."""

import json
from collections import Counter
from pathlib import Path

import pytest

from skyline_ingester.extract import PostFeatures, extract_post
from skyline_ingester.jetstream import ingest_raw
from skyline_ingester.policy import NORMALIZATION_VERSION, normalize_hashtag, normalize_link
from skyline_ingester.windows import SOURCE_DEDUPE_SECONDS, WindowStore

from .conftest import NOW

QUALITY_FIXTURE = Path(__file__).parent / "fixtures" / "signal_quality_events.jsonl"


def _quality_events() -> list[dict]:
    return [json.loads(line) for line in QUALITY_FIXTURE.read_text().splitlines()]


def test_hashtag_nfkc_casefold_and_display_preservation():
    fullwidth, reason = normalize_hashtag("ＣａｃｈｅＫｉｔ")
    assert reason is None
    assert fullwidth is not None
    assert fullwidth.canonical == "cachekit"
    assert fullwidth.display == "CacheKit"

    sharp_s, reason = normalize_hashtag("Straße")
    assert reason is None
    assert sharp_s is not None and sharp_s.canonical == "strasse" and sharp_s.display == "Straße"


@pytest.mark.parametrize("value", ["cache kit", "cache!", "-cache", "cache-", "_", 123])
def test_hashtag_facets_require_one_complete_token(value):
    tag, reason = normalize_hashtag(value)
    assert tag is None and reason == "malformed_tag"


def test_text_hashtag_boundaries_and_unicode_normalization():
    event = {
        "kind": "commit",
        "time_us": int(NOW * 1_000_000),
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "record": {"text": "inside#ignored ##ignored #Cafe\u0301, #good-tag! #नमस्ते"},
        },
    }
    features = extract_post(event)
    assert features is not None
    assert features.hashtags == ["café", "good-tag", "नमस्ते"]
    assert features.hashtag_labels == {"café": "Café", "good-tag": "good-tag", "नमस्ते": "नमस्ते"}


def test_link_canonicalization_preserves_resource_identity():
    link, reason = normalize_link("HTTPS://BÜCHER.example.com:443/story?ref=meaningful&utm_source=test&gclid=abc#fragment")
    assert reason is None
    assert link is not None
    assert link.uri == "https://xn--bcher-kva.example.com/story?ref=meaningful"
    assert link.domain == "xn--bcher-kva.example.com"

    # Ambiguous parameters and their original ordering/encoding are retained.
    meaningful, _ = normalize_link("https://Example.com:8443/item?source=book&ref=a%2Fb&campaign=spring")
    assert meaningful is not None
    assert meaningful.uri == "https://example.com:8443/item?source=book&ref=a%2Fb&campaign=spring"


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ("javascript:alert(1)", "unsafe_scheme"),
        ("ftp://example.com/file", "unsafe_scheme"),
        ("https://user:secret@example.com/", "unsafe_host"),
        ("http://127.0.0.1/admin", "unsafe_host"),
        ("http://127.1/admin", "unsafe_host"),
        ("http://localhost/admin", "unsafe_host"),
        ("https://spam.example.com/offer", "filtered_domain"),
        ("https://example.com/%not-escaped", "malformed_url"),
    ],
)
def test_dangerous_or_filtered_links_are_rejected(value, reason):
    link, actual = normalize_link(value)
    assert link is None and actual == reason


def test_recorded_before_after_keeps_broad_activity_above_one_repetitive_source():
    events = _quality_events()
    before_source_bound = Counter()
    store = WindowStore(dedupe_key=b"quality-fixture-key".ljust(32, b"0"))
    for event in events:
        features = extract_post(event)
        assert features is not None
        before_source_bound.update(features.hashtags)
        ingest_raw(json.dumps(event), store, now_fn=lambda: NOW)

    # The normalized-but-unbounded sample would put one automated source first.
    assert before_source_bound["flashsale"] == 6
    assert before_source_bound["community"] == 4

    hashtag_value = store.build_value("trending_hashtags", "5m", NOW)
    after = {item["canonical"]: item for item in hashtag_value["hashtags"]}
    assert after["community"] == {"tag": "Community", "canonical": "community", "count": 4}
    assert after["strasse"] == {"tag": "Straße", "canonical": "strasse", "count": 3}
    assert after["flashsale"] == {"tag": "FlashSale", "canonical": "flashsale", "count": 1}
    assert after["community"]["count"] > after["flashsale"]["count"]

    link_value = store.build_value("trending_links", "5m", NOW)
    assert {"uri": "https://news.example.com/story?id=42", "count": 1} in link_value["links"]
    assert {"domain": "news.example.com", "count": 1} in link_value["domains"]
    assert link_value["total_events_considered"] == len(events)
    assert link_value["normalization_version"] == NORMALIZATION_VERSION
    excluded = link_value["excluded_count_by_reason"]
    assert excluded["duplicate_source_tag"] == 5
    assert excluded["duplicate_source_url"] == 5
    assert excluded["duplicate_source_domain"] == 5
    assert excluded["filtered_tag"] == 2
    assert excluded["unsafe_scheme"] == 2
    assert excluded["filtered_domain"] == 1
    assert excluded["unsafe_host"] == 1
    assert excluded["malformed_tag"] == 1


def test_source_bound_is_rolling_and_allows_a_new_contribution_at_the_horizon():
    store = WindowStore(dedupe_key=b"x" * 32)
    features = PostFeatures(
        ts=NOW,
        lang="en",
        hashtags=["broad"],
        links=[],
        emoji=[],
        sentiment=None,
        hashtag_labels={"broad": "Broad"},
    )
    store.add(features, source_id="did:plc:one")
    features.ts = NOW + SOURCE_DEDUPE_SECONDS - 1
    store.add(features, source_id="did:plc:one")
    features.ts = NOW + SOURCE_DEDUPE_SECONDS
    store.add(features, source_id="did:plc:one")

    merged = store.merged("1h", NOW + SOURCE_DEDUPE_SECONDS)
    assert merged.tags["broad"] == 2
    assert merged.excluded["duplicate_source_tag"] == 1


def test_missing_source_cannot_bypass_public_trend_bound():
    event = _quality_events()[0]
    event.pop("did")
    store = WindowStore(dedupe_key=b"x" * 32)
    ingest_raw(json.dumps(event), store, now_fn=lambda: NOW)

    tags = store.build_value("trending_hashtags", "5m", NOW)
    links = store.build_value("trending_links", "5m", NOW)
    assert tags["hashtags"] == []
    assert links["links"] == [] and links["domains"] == []
    assert tags["total_events_considered"] == 1
    assert tags["excluded_count_by_reason"]["missing_source_tag"] == 1
    assert links["excluded_count_by_reason"]["missing_source_url"] == 1
    assert links["excluded_count_by_reason"]["missing_source_domain"] == 1


def test_source_identifiers_never_enter_buckets_checkpoints_or_public_values():
    events = _quality_events()
    store = WindowStore(dedupe_key=b"x" * 32)
    for event in events:
        ingest_raw(json.dumps(event), store, now_fn=lambda: NOW)

    snapshot = store.snapshot(NOW)
    values = {
        operation: store.build_value(operation, "5m", NOW)
        for operation in ("trending_hashtags", "trending_links", "lang_mix", "posts_per_minute", "top_emoji")
    }
    serialized = json.dumps({"snapshot": snapshot, "values": values}, ensure_ascii=False, sort_keys=True)
    for event in events:
        assert event["did"] not in serialized
    assert all(isinstance(digest, bytes) and len(digest) == 16 for digest in store._seen)
    assert "_seen" not in serialized and "dedupe_key" not in serialized

    # Restarts deliberately restore aggregates but not linkable source state.
    restored = WindowStore(dedupe_key=b"y" * 32)
    assert restored.restore(snapshot, NOW) > 0
    assert restored._seen == {} and restored._seen_expiry == []
