"""Sliding minute-bucket windows and the five locked aggregates.

One minute of posts = one Bucket of counters. A window aggregate merges the
buckets inside (now - window, now]; merges are memoised per (window, now) so
one publish tick computes each window's merge once for all five operations.

ponytail: merge-on-demand walks up to 1440 buckets per 24h publish (~every
450 s). Move to incremental per-window running totals if that ever shows up
in a profile.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from skyline_ingester.extract import PostFeatures

WINDOW_MINUTES = {"5m": 5, "1h": 60, "24h": 1440}
# Locked TTLs (docs/architecture.md): 5m -> 60 s, 1h -> 300 s, 24h -> 900 s.
WINDOW_TTLS = {"5m": 60, "1h": 300, "24h": 900}
OPERATIONS = ("trending_hashtags", "trending_links", "lang_mix", "posts_per_minute", "top_emoji")

SNAPSHOT_VERSION = 1
# Checkpoint truncation: keep the per-minute head of each counter so the
# serialized snapshot stays small enough for one cache entry.
_K_TAGS, _K_LINKS, _K_EMOJI, _K_LANGS = 20, 20, 10, 15


@dataclass(slots=True)
class Bucket:
    n: int = 0
    tags: Counter = field(default_factory=Counter)
    links: Counter = field(default_factory=Counter)
    langs: Counter = field(default_factory=Counter)
    emoji: Counter = field(default_factory=Counter)
    sent: dict[str, list[float]] = field(default_factory=dict)  # lang -> [sum, count]


@dataclass(slots=True)
class Merged:
    n: int = 0
    tags: Counter = field(default_factory=Counter)
    links: Counter = field(default_factory=Counter)
    langs: Counter = field(default_factory=Counter)
    emoji: Counter = field(default_factory=Counter)
    sent: dict[str, list[float]] = field(default_factory=dict)


class WindowStore:
    """In-memory minute buckets covering at most the 24h window."""

    def __init__(self, max_minutes: int = WINDOW_MINUTES["24h"]):
        self._max = max_minutes
        self._buckets: dict[int, Bucket] = {}
        self._memo: dict[tuple[str, int], Merged] = {}

    def add(self, feats: PostFeatures) -> None:
        minute = int(feats.ts // 60)
        bucket = self._buckets.get(minute)
        if bucket is None:
            bucket = self._buckets[minute] = Bucket()
            self._prune(minute)
        bucket.n += 1
        bucket.tags.update(feats.hashtags)
        bucket.links.update(feats.links)
        bucket.langs[feats.lang] += 1
        bucket.emoji.update(feats.emoji)
        if feats.sentiment is not None:
            acc = bucket.sent.setdefault(feats.lang, [0.0, 0])
            acc[0] += feats.sentiment
            acc[1] += 1
        self._memo.clear()

    def _prune(self, newest_minute: int) -> None:
        floor = max((m for m in self._buckets), default=newest_minute)
        floor = max(floor, newest_minute) - self._max
        for minute in [m for m in self._buckets if m <= floor]:
            del self._buckets[minute]

    def merged(self, window: str, now: float) -> Merged:
        """Merge the buckets inside (now - window, now]."""
        key = (window, int(now))
        memo = self._memo.get(key)
        if memo is not None:
            return memo
        now_min = int(now // 60)
        lo = now_min - WINDOW_MINUTES[window]
        out = Merged()
        for minute, b in self._buckets.items():
            if lo < minute <= now_min:
                out.n += b.n
                out.tags.update(b.tags)
                out.links.update(b.links)
                out.langs.update(b.langs)
                out.emoji.update(b.emoji)
                for lang, (s, c) in b.sent.items():
                    acc = out.sent.setdefault(lang, [0.0, 0])
                    acc[0] += s
                    acc[1] += c
        if len(self._memo) > 8:
            self._memo.clear()
        self._memo[key] = out
        return out

    def build_value(self, operation: str, window: str, now: float, top_n: int = 50) -> dict:
        """The interop/v1 value for one (operation, window): a top-level map with string keys."""
        m = self.merged(window, now)
        value: dict = {"window": window, "generated_at": int(now), "total_posts": m.n}
        if operation == "trending_hashtags":
            value["hashtags"] = [{"tag": t, "count": c} for t, c in m.tags.most_common(top_n)]
        elif operation == "trending_links":
            value["links"] = [{"uri": u, "count": c} for u, c in m.links.most_common(top_n)]
        elif operation == "lang_mix":
            total = sum(m.langs.values())
            top = m.langs.most_common(25)
            langs = {lang: round(c / total, 4) for lang, c in top} if total else {}
            rest = total - sum(c for _, c in top)
            if rest:
                langs["other"] = round(rest / total, 4)
            value["langs"] = langs
        elif operation == "posts_per_minute":
            value["ppm"] = round(m.n / WINDOW_MINUTES[window], 3)
        elif operation == "top_emoji":
            value["emoji"] = [{"emoji": e, "count": c} for e, c in m.emoji.most_common(25)]
        else:
            raise ValueError(f"unknown operation: {operation}")
        return value

    def sentiment_value(self, window: str, now: float) -> dict:
        """Value for the secure per-language sentiment cache (AC-6 groundwork)."""
        m = self.merged(window, now)
        return {
            "window": window,
            "generated_at": int(now),
            "langs": {lang: {"avg": round(s / c, 4), "n": c} for lang, (s, c) in sorted(m.sent.items()) if c},
        }

    def snapshot(self, now: float) -> dict:
        """Truncated, msgpack-friendly dump of the buckets for checkpointing.

        Per-bucket counters are cut to their top-K entries, so long-tail counts
        are approximate after a restore; posts_per_minute and lang_mix totals
        stay exact (bucket n / langs are kept in full up to _K_LANGS languages).
        """
        return {
            "v": SNAPSHOT_VERSION,
            "saved_at": int(now),
            "buckets": [
                [
                    minute,
                    {
                        "n": b.n,
                        "tags": dict(b.tags.most_common(_K_TAGS)),
                        "links": dict(b.links.most_common(_K_LINKS)),
                        "langs": dict(b.langs.most_common(_K_LANGS)),
                        "emoji": dict(b.emoji.most_common(_K_EMOJI)),
                        "sent": {lang: [s, c] for lang, (s, c) in b.sent.items()},
                    },
                ]
                for minute, b in sorted(self._buckets.items())
            ],
        }

    def restore(self, snap: dict, now: float) -> int:
        """Load a snapshot(); returns the number of buckets restored (0 = nothing usable)."""
        if not isinstance(snap, dict) or snap.get("v") != SNAPSHOT_VERSION:
            return 0
        floor = int(now // 60) - self._max
        restored = 0
        for minute, d in snap.get("buckets") or []:
            if not isinstance(minute, int) or minute <= floor:
                continue
            b = Bucket(
                n=int(d.get("n", 0)),
                tags=Counter(d.get("tags") or {}),
                links=Counter(d.get("links") or {}),
                langs=Counter(d.get("langs") or {}),
                emoji=Counter(d.get("emoji") or {}),
                sent={lang: [float(s), int(c)] for lang, (s, c) in (d.get("sent") or {}).items()},
            )
            self._buckets[minute] = b
            restored += 1
        self._memo.clear()
        return restored
