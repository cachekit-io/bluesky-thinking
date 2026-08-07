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

import hashlib
import heapq
import logging
import secrets
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from itertools import islice

from skyline_ingester.extract import EMOJI_RE, PostFeatures, is_primary_language
from skyline_ingester.policy import (
    EXCLUSION_REASONS,
    NORMALIZATION_VERSION,
    normalize_domain,
    normalize_hashtag,
    normalize_link,
)

logger = logging.getLogger(__name__)

WINDOW_MINUTES = {"5m": 5, "1h": 60, "24h": 1440}
# Locked TTLs (docs/architecture.md): 5m -> 60 s, 1h -> 300 s, 24h -> 900 s.
WINDOW_TTLS = {"5m": 60, "1h": 300, "24h": 900}
OPERATIONS = ("trending_hashtags", "trending_links", "lang_mix", "posts_per_minute", "top_emoji")

SNAPSHOT_VERSION = 2
SOURCE_DEDUPE_SECONDS = 5 * 60
_EXPIRY_SWEEP_LIMIT = 4_096
# Checkpoint truncation: keep the per-minute head of each counter so the
# serialized snapshot stays small enough for one cache entry.
_K_TAGS, _K_LINKS, _K_DOMAINS, _K_EMOJI, _K_LANGS = 20, 20, 20, 10, 15
_MAX_CHECKPOINT_COUNT = 10_000_000


@dataclass(slots=True)
class Bucket:
    """One minute of counters — also the shape a window merge accumulates into."""

    n: int = 0
    signal_candidates: int = 0
    tags: Counter = field(default_factory=Counter)
    links: Counter = field(default_factory=Counter)
    domains: Counter = field(default_factory=Counter)
    langs: Counter = field(default_factory=Counter)
    emoji: Counter = field(default_factory=Counter)
    tag_labels: dict[str, Counter] = field(default_factory=dict)
    excluded: Counter = field(default_factory=Counter)
    sent: dict[str, list[float]] = field(default_factory=dict)  # lang -> [sum, count]

    def copy(self) -> Bucket:
        # Shallow copies: enough isolation to read the copy while the original
        # keeps being mutated under the store lock. Counter.copy() into an empty
        # destination is a single C-level dict.update (Counter.update's empty
        # fast path), so each copy stays cheap enough to run under the lock.
        return Bucket(
            n=self.n,
            signal_candidates=self.signal_candidates,
            tags=self.tags.copy(),
            links=self.links.copy(),
            domains=self.domains.copy(),
            langs=self.langs.copy(),
            emoji=self.emoji.copy(),
            tag_labels={tag: labels.copy() for tag, labels in self.tag_labels.items()},
            excluded=self.excluded.copy(),
            sent={lang: acc.copy() for lang, acc in self.sent.items()},
        )


class WindowStore:
    """In-memory minute buckets covering at most the 24h window."""

    def __init__(self, max_minutes: int = WINDOW_MINUTES["24h"], *, dedupe_key: bytes | None = None):
        self._max = max_minutes
        self._buckets: dict[int, Bucket] = {}
        self._memo: dict[tuple[str, int], Bucket] = {}
        # Privacy boundary: only keyed digests of (source, signal family, value)
        # live here, for five minutes. The random key, digests, and expiry heap
        # are never copied into Bucket, snapshot(), build_value(), or logs.
        self._dedupe_key = dedupe_key or secrets.token_bytes(32)
        self._seen: dict[bytes, float] = {}
        self._seen_expiry: list[tuple[float, bytes]] = []
        # Bumped by every add(); a merge only memoises its result if no add()
        # landed since it started, so a cleared memo can't be resurrected with
        # a pre-add() view for the rest of that second.
        self._gen = 0
        # The Jetstream consumer calls add() on the event-loop thread while the
        # publish/checkpoint loops read the store from asyncio.to_thread workers;
        # every access to _buckets/_memo is serialised through this lock.
        self._lock = threading.Lock()

    def add(self, feats: PostFeatures, *, source_id: object = None) -> None:
        minute = int(feats.ts // 60)
        with self._lock:
            bucket = self._buckets.get(minute)
            if bucket is None:
                bucket = self._buckets[minute] = Bucket()
                self._prune(minute)
            bucket.n += 1
            bucket.excluded.update(feats.exclusions)
            bucket.signal_candidates += (
                sum(feats.exclusions.values()) + len(feats.hashtags) + len(feats.links) + len(feats.domains)
            )
            source_digest = self._source_digest(source_id)
            ledger_now = time.monotonic()
            self._expire_seen(ledger_now)
            for tag in feats.hashtags:
                if self._accept_signal(source_digest, "tag", tag, ledger_now):
                    bucket.tags[tag] += 1
                    label = feats.hashtag_labels.get(tag, tag)
                    bucket.tag_labels.setdefault(tag, Counter())[label] += 1
                else:
                    reason = "missing_source_tag" if source_digest is None else "duplicate_source_tag"
                    bucket.excluded[reason] += 1
            for link in feats.links:
                if self._accept_signal(source_digest, "url", link, ledger_now):
                    bucket.links[link] += 1
                else:
                    reason = "missing_source_url" if source_digest is None else "duplicate_source_url"
                    bucket.excluded[reason] += 1
            for domain in feats.domains:
                if self._accept_signal(source_digest, "domain", domain, ledger_now):
                    bucket.domains[domain] += 1
                else:
                    reason = "missing_source_domain" if source_digest is None else "duplicate_source_domain"
                    bucket.excluded[reason] += 1
            bucket.langs[feats.lang] += 1
            bucket.emoji.update(feats.emoji)
            if feats.sentiment is not None:
                acc = bucket.sent.setdefault(feats.lang, [0.0, 0])
                acc[0] += feats.sentiment
                acc[1] += 1
            self._memo.clear()
            self._gen += 1

    def _source_digest(self, source_id: object) -> bytes | None:
        if not isinstance(source_id, str) or not source_id or len(source_id) > 2_048:
            return None
        try:
            encoded = source_id.encode("utf-8")
        except UnicodeError:
            return None
        return hashlib.blake2b(encoded, key=self._dedupe_key, digest_size=16).digest()

    def _expire_seen(self, now: float) -> None:
        """Bound expiry work so reconnect recovery cannot stall ingestion."""
        swept = 0
        while self._seen_expiry and self._seen_expiry[0][0] <= now and swept < _EXPIRY_SWEEP_LIMIT:
            expiry, digest = heapq.heappop(self._seen_expiry)
            seen_at = self._seen.get(digest)
            if seen_at is not None and seen_at + SOURCE_DEDUPE_SECONDS == expiry:
                del self._seen[digest]
            swept += 1

    def _accept_signal(self, source_digest: bytes | None, family: str, value: str, now: float) -> bool:
        """Accept one source/signal contribution per rolling five minutes.

        Caller holds self._lock. The digest is process-keyed, non-portable, and
        discarded after the horizon. Restarts intentionally start with an empty
        ledger rather than persisting a stable identity boundary.
        """
        if source_digest is None:
            return False
        material = source_digest + b"\0" + family.encode("ascii") + b"\0" + value.encode("utf-8")
        digest = hashlib.blake2b(material, key=self._dedupe_key, digest_size=16).digest()
        seen_at = self._seen.get(digest)
        if seen_at is not None and seen_at + SOURCE_DEDUPE_SECONDS > now:
            return False
        self._seen[digest] = now
        heapq.heappush(self._seen_expiry, (now + SOURCE_DEDUPE_SECONDS, digest))
        return True

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
            out.signal_candidates += b.signal_candidates
            out.tags.update(b.tags)
            out.links.update(b.links)
            out.domains.update(b.domains)
            out.langs.update(b.langs)
            out.emoji.update(b.emoji)
            out.excluded.update(b.excluded)
            for tag, labels in b.tag_labels.items():
                out.tag_labels.setdefault(tag, Counter()).update(labels)
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
        value: dict = {
            "window": window,
            "generated_at": int(now),
            "total_posts": m.n,
            "total_events_considered": m.n,
            "total_signal_candidates": m.signal_candidates,
            "excluded_count_by_reason": dict(sorted(m.excluded.items())),
            "normalization_version": NORMALIZATION_VERSION,
        }
        if operation == "trending_hashtags":
            value["hashtags"] = [
                {
                    "tag": tag,
                    "display": _display_label(m.tag_labels.get(tag), tag),
                    "count": count,
                }
                for tag, count in m.tags.most_common(top_n)
            ]
        elif operation == "trending_links":
            value["links"] = [{"uri": u, "count": c} for u, c in m.links.most_common(top_n)]
            value["domains"] = [{"domain": d, "count": c} for d, c in m.domains.most_common(top_n)]
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
            "normalization_version": NORMALIZATION_VERSION,
            "langs": {lang: {"avg": round(s / c, 4), "n": c} for lang, (s, c) in sorted(m.sent.items()) if c},
        }

    def snapshot(self, now: float) -> dict:
        """Truncated, msgpack-friendly dump of the buckets for checkpointing.

        Per-bucket counters are cut to their top-K entries, so long-tail counts
        are approximate after a restore; post and signal-candidate totals stay
        exact. Long-tail language
        counts, like the trend counters, are approximate after restore.

        Per-language sentiment (`sent`) is deliberately NOT persisted: it is the
        cleartext source of the @cache.secure sentiment cache, and this checkpoint
        is stored unencrypted. Writing it here would let the backend reconstruct
        the zero-knowledge value (avg = sum / count). The secure 1h window
        repopulates within an hour of a restart; the restart-critical aggregate
        counts below are unaffected.

        The process-keyed source-contribution ledger is also deliberately absent.
        A restart rotates its random key and starts a fresh five-minute horizon;
        no source identifier or stable pseudonym enters this checkpoint.
        """
        # Same lock discipline as merged(): chunked copy-under-lock; the
        # most_common() sorts and dict building run outside.
        copies = sorted(self._copy_range())
        return {
            "v": SNAPSHOT_VERSION,
            "normalization_version": NORMALIZATION_VERSION,
            "saved_at": int(now),
            "buckets": [
                [
                    minute,
                    {
                        "n": b.n,
                        "signal_candidates": b.signal_candidates,
                        "tags": dict(b.tags.most_common(_K_TAGS)),
                        "links": dict(b.links.most_common(_K_LINKS)),
                        "domains": dict(b.domains.most_common(_K_DOMAINS)),
                        "langs": dict(b.langs.most_common(_K_LANGS)),
                        "emoji": dict(b.emoji.most_common(_K_EMOJI)),
                        "tag_labels": {
                            tag: dict(b.tag_labels.get(tag, Counter()).most_common(3))
                            for tag, _count in b.tags.most_common(_K_TAGS)
                        },
                        "excluded": dict(b.excluded),
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
        if (
            not isinstance(snap, dict)
            or snap.get("v") != SNAPSHOT_VERSION
            or snap.get("normalization_version") != NORMALIZATION_VERSION
        ):
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
        bucket_limit = self._max + 1
        if len(buckets) > bucket_limit:
            logger.warning("checkpoint has %d buckets; considering only %d", len(buckets), bucket_limit)
        with self._lock:
            for item in islice(buckets, bucket_limit):
                try:
                    minute, d = item
                    if not isinstance(minute, int) or minute <= floor or minute > ceiling:
                        continue
                    if not isinstance(d, dict):
                        raise TypeError("checkpoint bucket payload is not a map")
                    rejected: Counter[str] = Counter()
                    excluded = _coerced_counter(
                        d.get("excluded"),
                        key_validator=EXCLUSION_REASONS.__contains__,
                        reject_reason="checkpoint_invalid_exclusion",
                        rejected=rejected,
                        max_entries=len(EXCLUSION_REASONS),
                    )
                    b = Bucket(
                        n=_coerced_count(d.get("n", 0), rejected),
                        signal_candidates=_coerced_count(d.get("signal_candidates", 0), rejected),
                        tags=_coerced_counter(
                            d.get("tags"),
                            key_validator=_is_canonical_tag,
                            reject_reason="checkpoint_invalid_tag",
                            rejected=rejected,
                            max_entries=_K_TAGS,
                        ),
                        links=_coerced_counter(
                            d.get("links"),
                            key_validator=_is_canonical_link,
                            reject_reason="checkpoint_invalid_url",
                            rejected=rejected,
                            max_entries=_K_LINKS,
                        ),
                        domains=_coerced_counter(
                            d.get("domains"),
                            key_validator=_is_canonical_domain,
                            reject_reason="checkpoint_invalid_domain",
                            rejected=rejected,
                            max_entries=_K_DOMAINS,
                        ),
                        langs=_coerced_counter(
                            d.get("langs"),
                            key_validator=is_primary_language,
                            reject_reason="checkpoint_invalid_lang",
                            rejected=rejected,
                            max_entries=_K_LANGS,
                        ),
                        emoji=_coerced_counter(
                            d.get("emoji"),
                            key_validator=lambda value: EMOJI_RE.fullmatch(value) is not None,
                            reject_reason="checkpoint_invalid_emoji",
                            rejected=rejected,
                            max_entries=_K_EMOJI,
                        ),
                        tag_labels=_coerced_tag_labels(d.get("tag_labels"), rejected),
                        excluded=excluded,
                    )
                    b.excluded.update(rejected)
                    b.signal_candidates += sum(rejected.values())
                except (ValueError, TypeError, AttributeError, OverflowError) as exc:
                    # %.120r: entries come from the untrusted checkpoint and can
                    # be arbitrarily large — cap what one bad bucket puts in a log.
                    logger.warning("skipping corrupt checkpoint bucket: %s: %.120r", exc, item)
                    continue
                self._buckets[minute] = b
                restored += 1
            self._memo.clear()
            self._seen.clear()
            self._seen_expiry.clear()
            self._gen += 1
        return restored


def _coerced_count(value, rejected: Counter[str]) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > _MAX_CHECKPOINT_COUNT:
        rejected["checkpoint_invalid_count"] += 1
        return 0
    return value


def _coerced_counter(data, *, key_validator, reject_reason: str, rejected: Counter[str], max_entries: int) -> Counter:
    """Restore safe entries and account for each rejected entry independently."""
    output = Counter()
    if data is None:
        return output
    if not isinstance(data, dict):
        rejected[reject_reason] += 1
        return output
    if len(data) > max_entries:
        rejected[reject_reason] += len(data) - max_entries
    for raw_key, value in islice(data.items(), max_entries):
        if not isinstance(raw_key, str) or not key_validator(raw_key):
            rejected[reject_reason] += 1
            continue
        before = rejected["checkpoint_invalid_count"]
        count = _coerced_count(value, rejected)
        if rejected["checkpoint_invalid_count"] != before:
            continue
        output[raw_key] = count
    return output


def _coerced_tag_labels(data, rejected: Counter[str]) -> dict[str, Counter]:
    output: dict[str, Counter] = {}
    if data is None:
        return output
    if not isinstance(data, dict):
        rejected["checkpoint_invalid_label"] += 1
        return output
    if len(data) > _K_TAGS:
        rejected["checkpoint_invalid_label"] += len(data) - _K_TAGS
    for canonical, labels in islice(data.items(), _K_TAGS):
        if not isinstance(canonical, str) or not _is_canonical_tag(canonical):
            rejected["checkpoint_invalid_label"] += 1
            continue

        def valid_display(display: str, *, expected: str = canonical) -> bool:
            tag, _reason = normalize_hashtag(display)
            return tag is not None and tag.canonical == expected

        counter = _coerced_counter(
            labels,
            key_validator=valid_display,
            reject_reason="checkpoint_invalid_label",
            rejected=rejected,
            max_entries=3,
        )
        output[canonical] = counter
    return output


def _is_canonical_tag(value: str) -> bool:
    tag, _reason = normalize_hashtag(value)
    return tag is not None and tag.canonical == value


def _is_canonical_link(value: str) -> bool:
    link, _reason = normalize_link(value)
    return link is not None and link.uri == value


def _is_canonical_domain(value: str) -> bool:
    domain, _reason = normalize_domain(value)
    return domain == value


def _display_label(labels: Counter | None, canonical: str) -> str:
    if not labels:
        return canonical
    # Most frequent normalized spelling wins; lexical tie-break makes output
    # independent of arrival/dict insertion order.
    return min(labels.items(), key=lambda item: (-item[1], item[0]))[0]
