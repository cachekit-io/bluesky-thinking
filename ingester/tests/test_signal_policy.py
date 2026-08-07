"""Normalization, safety, source bounding, and transparency policy."""

import json
import logging
from collections import Counter
from pathlib import Path

import pytest

from skyline_ingester import policy as signal_policy
from skyline_ingester.extract import PostFeatures, extract_post
from skyline_ingester.health import HealthState
from skyline_ingester.jetstream import ingest_raw
from skyline_ingester.policy import NORMALIZATION_VERSION, normalize_hashtag, normalize_link
from skyline_ingester.windows import SOURCE_DEDUPE_SECONDS, WindowStore

from .conftest import NOW

QUALITY_FIXTURE = Path(__file__).parent / "fixtures" / "signal_quality_events.jsonl"
HOST_SWEEP_FIXTURE = Path(__file__).parent / "fixtures" / "host_provider_sweep.json"


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

    combining, reason = normalize_hashtag("ß́")
    assert reason is None
    assert combining is not None and combining.canonical == "sś"
    renormalized, reason = normalize_hashtag(combining.canonical)
    assert reason is None
    assert renormalized is not None and renormalized.canonical == combining.canonical

    too_long, reason = normalize_hashtag("ß" * 40)
    assert too_long is None and reason == "malformed_tag"


@pytest.mark.parametrize("value", ["cache kit", "cache!", "-cache", "cache-", "_", 123])
def test_hashtag_facets_require_one_complete_token(value):
    tag, reason = normalize_hashtag(value)
    assert tag is None and reason == "malformed_tag"


def test_oversized_hashtag_is_rejected_before_nfkc(monkeypatch):
    def unexpected_normalize(*_args):
        raise AssertionError("oversized input reached Unicode normalization")

    with monkeypatch.context() as patch:
        patch.setattr("skyline_ingester.policy.unicodedata.normalize", unexpected_normalize)
        tag, reason = normalize_hashtag("ﬃ" * 65)
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
        ("http://169.254.169．254/latest/meta-data/", "unsafe_host"),
        ("http://127。0。0。1/admin", "unsafe_host"),
        ("http://192.168.1｡1/admin", "unsafe_host"),
        ("http://a.b．local/admin", "unsafe_host"),
        ("http://169.254.169.254.sslip.io/latest/meta-data/", "unsafe_host"),
        ("http://169-254-169-254.sslip.io/latest/meta-data/", "unsafe_host"),
        ("http://my-169-254-169-254.sslip.io/latest/meta-data/", "unsafe_host"),
        ("http://a9fea9fe.sslip.io/latest/meta-data/", "unsafe_host"),
        ("http://64-ff9b--a9fe-a9fe.sslip.io/latest/meta-data/", "unsafe_host"),
        ("http://0--1.sslip.io/admin", "unsafe_host"),
        ("http://127.0.0.1.nip.io/admin", "unsafe_host"),
        ("http://192.168.1.1.traefik.me/admin", "unsafe_host"),
        ("http://foo.traefik.me/admin", "unsafe_host"),
        ("http://api.localho.st/admin", "unsafe_host"),
        ("http://x.local.gd/admin", "unsafe_host"),
        ("http://a.localhost.direct/admin", "unsafe_host"),
        ("http://lvh.me/admin", "unsafe_host"),
        ("http://localtest.me/admin", "unsafe_host"),
        ("http://vcap.me/admin", "unsafe_host"),
        ("http://1u.ms/admin", "unsafe_host"),
        ("http://x-169-254-169-254.ip.es.io/admin", "unsafe_host"),
        ("http://svc-10-0-0-1.ip.es.io/admin", "unsafe_host"),
        ("http://0--1.backname.io/admin", "unsafe_host"),
        ("http://64-ff9b--a9fe-a9fe.backname.io/admin", "unsafe_host"),
        ("http://test.lndo.site/admin", "unsafe_host"),
        ("http://anything.deep.sub.lacolhost.com/admin", "unsafe_host"),
        ("http://localhst.co.uk/admin", "unsafe_host"),
        ("http://a.l0pb.me/admin", "unsafe_host"),
        ("http://test.l0pb.dev/admin", "unsafe_host"),
        ("http://router.home.arpa/admin", "unsafe_host"),
        ("http://intranet.corp/admin", "unsafe_host"),
        ("http://host.intranet/admin", "unsafe_host"),
        ("http://box.private/admin", "unsafe_host"),
        ("http://srv.intra/admin", "unsafe_host"),
        ("http://srv.localdomain/admin", "unsafe_host"),
        ("http://224.0.0.1/multicast", "unsafe_host"),
        ("http://192.88.99.1/relay", "unsafe_host"),
        ("http://[ff02::1]/multicast", "unsafe_host"),
        ("http://[5f00::1]/segment", "unsafe_host"),
        ("http://[64:ff9b:1::808:808]/resource", "unsafe_host"),
        ("https://ads.xvideos.com/offer", "filtered_domain"),
        ("https://example.com/%not-escaped", "malformed_url"),
    ],
)
def test_dangerous_or_filtered_links_are_rejected(value, reason):
    link, actual = normalize_link(value)
    assert link is None and actual == reason


def test_checked_in_provider_sweep_matches_host_policy():
    sweep = json.loads(HOST_SWEEP_FIXTURE.read_text())
    assert sweep["verified_on"] == "2026-08-07"
    providers = sweep["providers"]
    roots = {provider["root"] for provider in providers}
    assert roots == signal_policy._LOCAL_HOSTS
    assert set(sweep["known_live_provider_roots"]) < roots
    for provider in providers:
        assert provider["observed_answers"]
        link, reason = normalize_link(f"http://{provider['probe']}/admin")
        assert link is None and reason == "unsafe_host"
    for probe in sweep["local_suffix_probes"]:
        link, reason = normalize_link(f"http://{probe}/admin")
        assert link is None and reason == "unsafe_host"


def test_link_output_length_is_bounded_after_root_slash_insertion():
    prefix = "https://example.com?x="
    value = prefix + "a" * (2_048 - len(prefix))
    link, reason = normalize_link(value)
    assert link is None and reason == "malformed_url"


def test_ipv4_embedded_ipv6_keeps_global_targets_only():
    link, reason = normalize_link("https://[64:ff9b::808:808]/resource")
    assert reason is None
    assert link is not None and link.domain == "64:ff9b::808:808"

    alias, reason = normalize_link("https://8.8.8.8.sslip.io/resource")
    assert alias is None and reason == "unsafe_host"


@pytest.mark.parametrize("prefix", ["::", "64:ff9b::", "::ffff:0:"])
@pytest.mark.parametrize("target", ["10.0.0.1", "127.0.0.1", "169.254.169.254", "192.168.1.1"])
def test_ipv4_embedded_ipv6_rejects_non_global_targets(prefix, target):
    link, reason = normalize_link(f"http://[{prefix}{target}]/resource")
    assert link is None and reason == "unsafe_host"


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
    after = {item["tag"]: item for item in hashtag_value["hashtags"]}
    assert after["community"] == {"tag": "community", "display": "Community", "count": 4}
    assert after["strasse"] == {"tag": "strasse", "display": "Straße", "count": 3}
    assert after["flashsale"] == {"tag": "flashsale", "display": "FlashSale", "count": 1}
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
    accepted = sum(item["count"] for item in hashtag_value["hashtags"])
    accepted += sum(item["count"] for item in link_value["links"])
    accepted += sum(item["count"] for item in link_value["domains"])
    assert link_value["total_signal_candidates"] == accepted + sum(excluded.values())


def test_source_bound_is_rolling_on_monotonic_time(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("skyline_ingester.windows.time.monotonic", lambda: clock[0])
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
    clock[0] += SOURCE_DEDUPE_SECONDS - 1
    features.ts = NOW + SOURCE_DEDUPE_SECONDS - 1
    store.add(features, source_id="did:plc:one")
    clock[0] += 1
    features.ts = NOW + SOURCE_DEDUPE_SECONDS
    store.add(features, source_id="did:plc:one")

    merged = store.merged("1h", NOW + SOURCE_DEDUPE_SECONDS)
    assert merged.tags["broad"] == 2
    assert merged.excluded["duplicate_source_tag"] == 1


def test_future_event_timestamp_cannot_expire_source_ledger(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("skyline_ingester.windows.time.monotonic", lambda: clock[0])
    event = _quality_events()[0]
    store = WindowStore(dedupe_key=b"x" * 32)
    ingest_raw(json.dumps(event), store, now_fn=lambda: NOW)

    future = {**event, "time_us": int((NOW + SOURCE_DEDUPE_SECONDS) * 1_000_000)}
    ingest_raw(json.dumps(future), store, now_fn=lambda: NOW)
    clock[0] += 1
    again = {**event, "time_us": int((NOW + 1) * 1_000_000)}
    ingest_raw(json.dumps(again), store, now_fn=lambda: NOW)

    merged = store.merged("1h", NOW + SOURCE_DEDUPE_SECONDS)
    assert merged.tags["flashsale"] == 1
    assert merged.excluded["duplicate_source_tag"] == 2


def test_source_bound_rate_is_explicit_across_1h_and_24h(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("skyline_ingester.windows.time.monotonic", lambda: clock[0])
    store = WindowStore(dedupe_key=b"x" * 32)
    features = PostFeatures(
        ts=NOW,
        lang="en",
        hashtags=["bounded"],
        links=[],
        emoji=[],
        sentiment=None,
        hashtag_labels={"bounded": "Bounded"},
    )
    for interval in range(288):
        features.ts = NOW + interval * SOURCE_DEDUPE_SECONDS
        store.add(features, source_id="did:plc:one")
        clock[0] += SOURCE_DEDUPE_SECONDS

    latest = features.ts
    assert store.merged("1h", latest).tags["bounded"] == 12
    assert store.merged("24h", latest).tags["bounded"] == 288


def test_missing_source_cannot_bypass_public_trend_bound():
    event = _quality_events()[0]
    event.pop("did")
    store = WindowStore(dedupe_key=b"x" * 32)
    health = HealthState(now_fn=lambda: NOW)
    ingest_raw(json.dumps(event), store, now_fn=lambda: NOW, health=health)

    tags = store.build_value("trending_hashtags", "5m", NOW)
    links = store.build_value("trending_links", "5m", NOW)
    assert tags["hashtags"] == []
    assert links["links"] == [] and links["domains"] == []
    assert tags["total_events_considered"] == 1
    assert tags["excluded_count_by_reason"]["missing_source_tag"] == 1
    assert links["excluded_count_by_reason"]["missing_source_url"] == 1
    assert links["excluded_count_by_reason"]["missing_source_domain"] == 1
    assert health.snapshot()[1]["events_missing_source"] == 1


def test_missing_source_warning_is_aggregate_and_rate_limited(caplog):
    event = _quality_events()[0]
    event.pop("did")
    store = WindowStore(dedupe_key=b"x" * 32)
    health = HealthState(now_fn=lambda: NOW)
    with caplog.at_level(logging.WARNING, logger="skyline_ingester.jetstream"):
        for _ in range(1_001):
            ingest_raw(json.dumps(event), store, now_fn=lambda: NOW, health=health)
    messages = [record.getMessage() for record in caplog.records if "missing source DID" in record.getMessage()]
    assert messages == [
        "Jetstream posts missing source DID; trend signals excluded (count=1)",
        "Jetstream posts missing source DID; trend signals excluded (count=1000)",
    ]


def test_missing_source_without_health_does_not_rearm_warning(caplog):
    event = _quality_events()[0]
    event.pop("did")
    with caplog.at_level(logging.WARNING, logger="skyline_ingester.jetstream"):
        for _ in range(10):
            ingest_raw(json.dumps(event), WindowStore(), now_fn=lambda: NOW)
    assert not [record for record in caplog.records if "missing source DID" in record.getMessage()]


def test_filtered_facet_and_text_fallback_count_one_rejection():
    event = _quality_events()[0]
    event["commit"]["record"]["text"] = "#nsfw #spam"
    event["commit"]["record"]["facets"] = [
        {
            "features": [
                {"$type": "app.bsky.richtext.facet#tag", "tag": "nsfw"},
                {"$type": "app.bsky.richtext.facet#tag", "tag": "spam"},
            ]
        }
    ]
    features = extract_post(event)
    assert features is not None
    assert features.hashtags == []
    assert features.exclusions["filtered_tag"] == 2


def test_malformed_facet_and_text_fallback_count_one_rejection():
    event = _quality_events()[0]
    event["commit"]["record"]["text"] = "#___"
    event["commit"]["record"]["facets"] = [{"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "___"}]}]
    features = extract_post(event)
    assert features is not None
    assert features.hashtags == []
    assert features.exclusions["malformed_tag"] == 1


def test_hash_prefixed_filtered_facet_matches_repeated_body_fallback():
    event = _quality_events()[0]
    event["commit"]["record"]["text"] = "#nsfw #nsfw #nsfw"
    event["commit"]["record"]["facets"] = [{"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "#nsfw"}]}]
    features = extract_post(event)
    assert features is not None
    assert features.hashtags == []
    assert features.exclusions == {"filtered_tag": 1}


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
    # A fresh process can restore aggregate history without any source identifier.
    restored = WindowStore(dedupe_key=b"y" * 32)
    assert restored.restore(snapshot, NOW) > 0
