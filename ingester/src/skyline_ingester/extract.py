"""One Jetstream event -> privacy-safe features for window aggregation.

PostFeatures contains normalized counter inputs and aggregate exclusion reasons
only — never post text, an author DID, a record key, or a stable pseudonym.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

from skyline_ingester.policy import hashtag_candidate_fingerprint, hashtags_from_text, normalize_hashtag, normalize_link

POST_COLLECTION = "app.bsky.feed.post"
MAX_TEXT_LENGTH = 4_096
MAX_FACET_FEATURES = 64
MAX_TAG_CANDIDATES = 32
MAX_LINK_CANDIDATES = 16
_PRIMARY_LANGUAGE_RE = re.compile(r"[a-z]{2,8}")

# Common emoji blocks; a match is one emoji possibly extended by variation
# selectors, skin tones, and ZWJ sequences (so 👨‍👩‍👧 counts once, not thrice).
_EMOJI_CORE = (
    "\U0001f1e6-\U0001f1ff"  # regional indicators (flags)
    "\U0001f300-\U0001f5ff"
    "\U0001f600-\U0001f64f"
    "\U0001f680-\U0001f6ff"
    "\U0001f900-\U0001f9ff"
    "\U0001fa70-\U0001faff"
    "☀-⛿"
    "✀-➿"
)
_EXT = "[️\U0001f3fb-\U0001f3ff]"
EMOJI_RE = re.compile(f"[{_EMOJI_CORE}](?:{_EXT}|‍[{_EMOJI_CORE}]{_EXT}?)*")

# ponytail: ~40-word lexicon + emoji valence — demo-grade sentiment. Swap for a
# real model (e.g. vader / a small transformer) if the secure cache ever matters.
_POSITIVE = frozenset(
    [
        "love",
        "loved",
        "great",
        "good",
        "happy",
        "amazing",
        "awesome",
        "best",
        "beautiful",
        "wonderful",
        "excited",
        "win",
        "won",
        "thanks",
        "thank",
        "cool",
        "nice",
        "fun",
        "hope",
        "proud",
        "glad",
        "excellent",
        "perfect",
    ]
)
_NEGATIVE = frozenset(
    [
        "hate",
        "hated",
        "bad",
        "sad",
        "angry",
        "awful",
        "worst",
        "terrible",
        "horrible",
        "annoying",
        "lose",
        "lost",
        "fail",
        "failed",
        "scared",
        "sick",
        "tired",
        "wrong",
        "broken",
        "ugly",
        "disappointed",
    ]
)
# frozenset over a string = one entry per code point (these are all single-codepoint emoji)
_POS_EMOJI = frozenset("❤🧡💛💚💙💜😍😂🥰😊✨🔥🎉👍🙏😁")
_NEG_EMOJI = frozenset("😢😭😡👎💔😠🤮😞😰😨")


@dataclass(slots=True)
class PostFeatures:
    ts: float  # unix seconds (Jetstream event time)
    lang: str  # primary BCP-47 language subtag, "und" if unset
    hashtags: list[str]
    links: list[str]
    emoji: list[str]
    sentiment: float | None  # [-1.0, 1.0]; None when nothing scoreable
    domains: list[str] = field(default_factory=list)
    hashtag_labels: dict[str, str] = field(default_factory=dict)
    exclusions: Counter[str] = field(default_factory=Counter)


def extract_post(event: dict) -> PostFeatures | None:
    """Return features for an app.bsky.feed.post create commit, else None."""
    if not isinstance(event, dict) or event.get("kind") != "commit":
        return None
    commit = event.get("commit") or {}
    if not isinstance(commit, dict):
        return None
    if commit.get("operation") != "create" or commit.get("collection") != POST_COLLECTION:
        return None
    time_us = event.get("time_us")
    if not isinstance(time_us, int):
        return None
    record = commit.get("record") or {}
    if not isinstance(record, dict):
        return None
    text = record.get("text") or ""
    if not isinstance(text, str):
        text = ""
    text = text[:MAX_TEXT_LENGTH]

    tag_candidates: list[object] = []
    link_candidates: list[object] = []
    exclusions: Counter[str] = Counter()
    facets = record.get("facets") or []
    if not isinstance(facets, list):
        facets = []
    features_examined = 0
    bounded_facets = facets[:MAX_FACET_FEATURES]
    if len(facets) > len(bounded_facets):
        exclusions["candidate_limit_facet"] += len(facets) - len(bounded_facets)
    for facet in bounded_facets:
        if not isinstance(facet, dict):
            continue
        features = facet.get("features") or []
        if not isinstance(features, list):
            continue
        for feature_index, feature in enumerate(features):
            if features_examined >= MAX_FACET_FEATURES:
                exclusions["candidate_limit_feature"] += len(features) - feature_index
                break
            features_examined += 1
            if not isinstance(feature, dict):
                continue
            ftype = feature.get("$type")
            if ftype == "app.bsky.richtext.facet#tag" and feature.get("tag") is not None:
                if len(tag_candidates) < MAX_TAG_CANDIDATES:
                    tag_candidates.append(feature["tag"])
                else:
                    exclusions["candidate_limit_tag"] += 1
            elif ftype == "app.bsky.richtext.facet#link" and feature.get("uri") is not None:
                if len(link_candidates) < MAX_LINK_CANDIDATES:
                    link_candidates.append(feature["uri"])
                else:
                    exclusions["candidate_limit_url"] += 1
    embed = record.get("embed") or {}
    if isinstance(embed, dict) and embed.get("$type") == "app.bsky.embed.external":
        external = embed.get("external") or {}
        uri = external.get("uri") if isinstance(external, dict) else None
        if uri:
            if len(link_candidates) < MAX_LINK_CANDIDATES:
                link_candidates.append(uri)
            else:
                exclusions["candidate_limit_url"] += 1

    hashtags: list[str] = []
    hashtag_labels: dict[str, str] = {}
    rejected_facet_tags: set[tuple[str, str]] = set()

    def add_hashtag(candidate: object, *, from_fallback: bool = False) -> None:
        tag, reason = normalize_hashtag(candidate)
        if tag is None:
            candidate_key = hashtag_candidate_fingerprint(candidate)
            if from_fallback and candidate_key in rejected_facet_tags:
                return
            exclusions[reason or "malformed_tag"] += 1
            if not from_fallback and candidate_key is not None:
                rejected_facet_tags.add(candidate_key)
        elif tag.canonical in hashtag_labels:
            exclusions["duplicate_in_event_tag"] += 1
        else:
            hashtags.append(tag.canonical)
            hashtag_labels[tag.canonical] = tag.display

    for candidate in tag_candidates:
        add_hashtag(candidate)

    # A malformed facet must not suppress usable hashtags in the post body.
    # Fall back when no facet candidate survived normalization, just as clients
    # without facets do.
    if not hashtags:
        text_candidates = hashtags_from_text(text)
        if len(text_candidates) > MAX_TAG_CANDIDATES:
            exclusions["candidate_limit_tag"] += len(text_candidates) - MAX_TAG_CANDIDATES
        for candidate in text_candidates[:MAX_TAG_CANDIDATES]:
            add_hashtag(candidate, from_fallback=True)

    links: list[str] = []
    domains: list[str] = []
    seen_links: set[str] = set()
    seen_domains: set[str] = set()
    for candidate in link_candidates:
        link, reason = normalize_link(candidate)
        if link is None:
            exclusions[reason or "malformed_url"] += 1
            continue
        if link.uri in seen_links:
            exclusions["duplicate_in_event_url"] += 1
        else:
            seen_links.add(link.uri)
            links.append(link.uri)
        if link.domain in seen_domains:
            exclusions["duplicate_in_event_domain"] += 1
        else:
            seen_domains.add(link.domain)
            domains.append(link.domain)

    langs = record.get("langs") or []
    if not isinstance(langs, list):
        langs = []
    lang = normalize_language(langs[0] if langs else None)

    return PostFeatures(
        ts=time_us / 1_000_000,
        lang=lang,
        hashtags=hashtags,
        links=links,
        emoji=EMOJI_RE.findall(text),
        sentiment=score_sentiment(text),
        domains=domains,
        hashtag_labels=hashtag_labels,
        exclusions=exclusions,
    )


def score_sentiment(text: str) -> float | None:
    """Lexicon score in [-1, 1]: (positive - negative) / matched tokens."""
    words = re.findall(r"[a-z']+", text.lower())
    pos = sum(w in _POSITIVE for w in words)
    neg = sum(w in _NEGATIVE for w in words)
    for emoji in EMOJI_RE.findall(text):
        base = emoji.replace("️", "")[:1]  # valence lookup on the base char
        if base in _POS_EMOJI:
            pos += 1
        elif base in _NEG_EMOJI:
            neg += 1
    if pos + neg == 0:
        return None
    return (pos - neg) / (pos + neg)


def normalize_language(value: object) -> str:
    """Return a safe lowercase BCP-47 primary language subtag or ``und``."""
    if not isinstance(value, str):
        return "und"
    primary = value.partition("-")[0].lower()
    return primary if _PRIMARY_LANGUAGE_RE.fullmatch(primary) else "und"


def is_primary_language(value: str) -> bool:
    return _PRIMARY_LANGUAGE_RE.fullmatch(value) is not None
