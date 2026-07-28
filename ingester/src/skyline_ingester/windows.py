"""Sliding minute-bucket windows and the five locked aggregates.

One minute of posts = one Bucket of counters. A window aggregate merges the
buckets inside (now - window, now]; merges are memoised per (window, now), so
on a quiet stream one publish tick computes each window's merge once for all
five operations. The memo is best-effort: an add() landing mid-merge suppresses
it (typical under live firehose load) and each caller then recomputes — correct
either way, just without the shortcut.

ponytail: merge-on-demand walks up to 1440 buckets per 24h publish (~every
450 s). Move to incremental per-window running totals if that ever shows up
in a profile.
"""

from __future__ import annotations

import logging
import threading
from collections import Counter
from dataclasses import dataclass, field

from skyline_ingester.extract import PostFeatures

logger = logging.getLogger(__name__)

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
    """One minute of counters — also the shape a window merge accumulates into."""

    n: int = 0
    tags: Counter = field(default_factory=Counter)
    links: Counter = field(default_factory=Counter)
    langs: Counter = field(default_factory=Counter)
    emoji: Counter = field(default_factory=Counter)
    sent: dict[str, list[float]] = field(default_factory=dict)  # lang -> [sum, count]

    def copy(self) -> Bucket:
        # Shallow copies: enough isolation to read the copy while the original
        # keeps being mutated under the store lock. Counter.copy() into an empty
        # destination is a single C-level dict.update (Counter.update's empty
        # fast path), so each copy stays cheap enough to run under the lock.
        return Bucket(
            n=self.n,
            tags=self.tags.copy(),
            links=self.links.copy(),
            langs=self.langs.copy(),
            emoji=self.emoji.copy(),
            sent={lang: acc.copy() for lang, acc in self.sent.items()},
        )


class WindowStore:
    """In-memory minute buckets covering at most the 24h window."""

    def __init__(self, max_minutes: int = WINDOW_MINUTES["24h"]):
        self._max = max_minutes
        self._buckets: dict[int, Bucket] = {}
        self._memo: dict[tuple[str, int], Bucket] = {}
        # Bumped by every add(); a merge only memoises its result if no add()
        # landed since it started, so a cleared memo can't be resurrected with
        # a pre-add() view for the rest of that second.
        self._gen = 0
        # The Jetstream consumer calls add() on the event-loop thread while the
        # publish/checkpoint loops read the store from asyncio.to_thread workers;
        # every access to _buckets/_memo is serialised through this lock.
        self._lock = threading.Lock()

    def add(self, feats: PostFeatures) -> None:
        minute = int(feats.ts // 60)
        with self._lock:
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
            self._gen += 1

    def _prune(self, newest_minute: int) -> None:
        # Caller holds self._lock. Anchor the retention floor to the minute being
        # added, NOT max(self._buckets): one bogus far-future timestamp must not
        # become a permanent anchor that evicts every real bucket forever. With
        # this anchor a stray future bucket is excluded from every merged() query
        # (which bounds by `now`) and real minutes re-accumulate on the next event.
        floor = newest_minute - self._max
        for minute in [m for m in self._buckets if m <= floor]:
            del self._buckets[minute]

    def merged(self, window: str, now: float) -> Bucket:
        """Merge the buckets inside (now - window, now] into one Bucket.

        Lock contract: self._lock is a non-reentrant threading.Lock — never call
        merged()/snapshot()/add() while holding it. The lock is held only for
        C-speed per-bucket copies; the O(window) Counter merge runs outside it so
        add() on the event-loop thread never stalls behind a full 24h merge. The
        returned (memoised) Bucket is read lock-free by callers and MUST NOT be
        mutated.
        """
        key = (window, int(now))
        now_min = int(now // 60)
        lo = now_min - WINDOW_MINUTES[window]
        with self._lock:
            memo = self._memo.get(key)
            if memo is not None:
                return memo
            gen = self._gen
        out = Bucket()
        for _minute, b in self._copy_range(lo, now_min):
            out.n += b.n
            out.tags.update(b.tags)
            out.links.update(b.links)
            out.langs.update(b.langs)
            out.emoji.update(b.emoji)
            for lang, (s, c) in b.sent.items():
                acc = out.sent.setdefault(lang, [0.0, 0])
                acc[0] += s
                acc[1] += c
        with self._lock:
            # Memoise only if no add() landed since the merge started: add()
            # cleared the memo, and re-inserting this pre-add() view would serve
            # it stale to every same-second caller.
            if self._gen == gen:
                if len(self._memo) > 8:
                    self._memo.clear()
                self._memo[key] = out
        return out

    # 16 buckets/chunk keeps each lock hold ~1-2 ms even at firehose-dense
    # buckets; the per-chunk lock overhead itself is microseconds.
    _COPY_CHUNK = 16

    def _copy_range(self, lo: float = float("-inf"), hi: float = float("inf")) -> list[tuple[int, Bucket]]:
        """Copy the buckets in (lo, hi] in chunks, releasing the lock between chunks.

        Copy, don't reference: add() mutates hot buckets' Counters in place, and
        iterating a Counter that grows mid-merge raises "dictionary changed size
        during iteration" (the round-1 bug class). Chunking bounds add()'s worst
        stall to one chunk's copy (~few ms) instead of a full-window copy; a bucket
        created or pruned between chunks simply lands in or out of this tick's view,
        which periodic analytics tolerates.
        """
        with self._lock:
            keys = [m for m in self._buckets if lo < m <= hi]
        copies: list[tuple[int, Bucket]] = []
        for i in range(0, len(keys), self._COPY_CHUNK):
            with self._lock:
                for m in keys[i : i + self._COPY_CHUNK]:
                    b = self._buckets.get(m)
                    if b is not None:  # pruned between chunks
                        copies.append((m, b.copy()))
        return copies

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

        Per-language sentiment (`sent`) is deliberately NOT persisted: it is the
        cleartext source of the @cache.secure sentiment cache, and this checkpoint
        is stored unencrypted. Writing it here would let the backend reconstruct
        the zero-knowledge value (avg = sum / count). The secure 1h window
        repopulates within an hour of a restart; the restart-critical aggregate
        counts below are unaffected.
        """
        # Same lock discipline as merged(): chunked copy-under-lock; the
        # most_common() sorts and dict building run outside.
        copies = sorted(self._copy_range())
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
                    },
                ]
                for minute, b in copies
            ],
        }

    def restore(self, snap: dict, now: float) -> int:
        """Load a snapshot(); returns the number of buckets restored (0 = nothing usable).

        The checkpoint is untrusted input (plaintext, integrity-unprotected in the
        backend), so every entry is validated and a malformed one is skipped with a
        warning rather than raising — a corrupt or partial checkpoint must never
        crash startup into a permanent boot loop. Legacy checkpoints may still carry
        `sent`; it is IGNORED entirely: the checkpoint is operator-poisonable, and
        restoring `sent` would let the backend operator choose the plaintext that the
        next @cache.secure publish encrypts — the exact value the zero-knowledge
        boundary exists to protect. Sentiment repopulates from live ingestion only.
        """
        if not isinstance(snap, dict) or snap.get("v") != SNAPSHOT_VERSION:
            logger.warning("ignoring checkpoint with unexpected shape/version: %.80r", snap)
            return 0
        buckets = snap.get("buckets")
        if not isinstance(buckets, list):
            logger.warning("ignoring checkpoint with malformed buckets: %.80r", buckets)
            return 0
        now_min = int(now // 60)
        floor = now_min - self._max
        ceiling = now_min + 1  # a checkpoint can't legitimately hold future minutes
        restored = 0
        with self._lock:
            for item in buckets:
                try:
                    minute, d = item
                    if not isinstance(minute, int) or minute <= floor or minute > ceiling:
                        continue
                    # Coerce keys/values, not just presence: a poisoned-but-valid
                    # checkpoint (e.g. a counter value of "not-a-number", or a
                    # negative count that skews ppm/lang_mix) would pass restore
                    # and detonate later inside merged()/most_common(), where the
                    # publisher's except swallows it into silent misses for up to
                    # 24h. A bad entry must fail HERE, skipping only its bucket.
                    b = Bucket(
                        n=_non_negative(int(d.get("n", 0))),
                        tags=_coerced_counter(d.get("tags")),
                        links=_coerced_counter(d.get("links")),
                        langs=_coerced_counter(d.get("langs")),
                        emoji=_coerced_counter(d.get("emoji")),
                    )
                except (ValueError, TypeError, AttributeError, OverflowError) as exc:
                    # %.120r: entries come from the untrusted checkpoint and can
                    # be arbitrarily large — cap what one bad bucket puts in a log.
                    logger.warning("skipping corrupt checkpoint bucket: %s: %.120r", exc, item)
                    continue
                self._buckets[minute] = b
                restored += 1
            self._memo.clear()
            self._gen += 1
        return restored


def _non_negative(value: int) -> int:
    if value < 0:
        raise ValueError("negative count in checkpoint")
    return value


def _coerced_counter(data) -> Counter:
    # str keys / non-negative int values, or ValueError|TypeError|OverflowError
    # (int(float("inf"))) — restore() skips the bucket.
    return Counter({str(k): _non_negative(int(v)) for k, v in (data or {}).items()})
