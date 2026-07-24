"""One Jetstream event -> the features the windows aggregate.

Aggregate-only by design: PostFeatures carries counters' inputs (tags, links,
lang, emoji, a sentiment score) — never the post text, author DID, or rkey.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

POST_COLLECTION = "app.bsky.feed.post"

TAG_RE = re.compile(r"#(\w[\w-]*)")

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


def extract_post(event: dict) -> PostFeatures | None:
    """Return features for an app.bsky.feed.post create commit, else None."""
    if not isinstance(event, dict) or event.get("kind") != "commit":
        return None
    commit = event.get("commit") or {}
    if commit.get("operation") != "create" or commit.get("collection") != POST_COLLECTION:
        return None
    time_us = event.get("time_us")
    if not isinstance(time_us, int):
        return None
    record = commit.get("record") or {}
    text = record.get("text") or ""

    hashtags: list[str] = []
    links: list[str] = []
    for facet in record.get("facets") or []:
        for feature in facet.get("features") or []:
            ftype = feature.get("$type")
            if ftype == "app.bsky.richtext.facet#tag" and feature.get("tag"):
                hashtags.append(str(feature["tag"]).lower())
            elif ftype == "app.bsky.richtext.facet#link" and feature.get("uri"):
                links.append(str(feature["uri"]))
    if not hashtags:  # posts from clients that don't emit tag facets
        hashtags = [m.group(1).lower() for m in TAG_RE.finditer(text)]
    embed = record.get("embed") or {}
    if embed.get("$type") == "app.bsky.embed.external":
        uri = (embed.get("external") or {}).get("uri")
        if uri:
            links.append(str(uri))

    langs = record.get("langs") or []
    lang = "und"
    if langs and isinstance(langs[0], str) and langs[0]:
        lang = langs[0].split("-")[0].lower()

    return PostFeatures(
        ts=time_us / 1_000_000,
        lang=lang,
        hashtags=list(dict.fromkeys(hashtags)),
        links=list(dict.fromkeys(links)),
        emoji=EMOJI_RE.findall(text),
        sentiment=score_sentiment(text),
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
