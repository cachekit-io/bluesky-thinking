"""Memory soak harness for WindowStore (LAB-1775).

Two modes, both stdlib + the ingester's own modules — no CacheKit credentials:
the publisher path is not exercised, only extract -> WindowStore, which is the
axis the OOM lives on.

  live      Drive the real public Jetstream firehose through ingest_raw() for a
            fixed wall-clock duration, sampling process RSS and live counter-key
            cardinality. Reports observed rates, per-minute distinct-key
            cardinality by family, and the marginal bytes-per-key implied by the
            RSS/key-count regression. This is the measured basis for a 24h
            projection.

  saturate  Synthetic worst case: fill N minute buckets to a fixed per-bucket
            cardinality and measure resident set plus the merged() transient.
            With --per-bucket at the production cap this is the post-fix
            worst-case number; with --per-bucket at the measured live
            cardinality it is the pre-fix projection, measured rather than
            extrapolated.

Usage:
    uv run python tools/soak_memory.py live --seconds 600
    uv run python tools/soak_memory.py saturate --minutes 1440 --per-bucket 200
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import statistics
import time
from collections import Counter

import websockets

from skyline_ingester import windows
from skyline_ingester.extract import PostFeatures
from skyline_ingester.health import _peak_rss_mib, _rss_mib
from skyline_ingester.jetstream import ingest_raw, subscribe_url
from skyline_ingester.windows import WINDOW_MINUTES, WindowStore

MIB = 1024 * 1024
FAMILIES = ("tags", "links", "domains", "langs", "emoji", "tag_labels", "excluded", "sent")


def rss_bytes() -> float:
    """Current resident set, falling back to the high-water mark off Linux.

    Deliberately reuses /health's own helpers rather than keeping a second copy:
    ru_maxrss's KiB-on-Linux/bytes-on-macOS split is subtle enough that the two
    copies had already drifted apart once, which would have silently scaled
    every number a macOS soak reports — and those numbers are quoted in the
    docs — by 1024x.
    """
    return (_rss_mib() or _peak_rss_mib()) * MIB


def peak_rss_bytes() -> float:
    return _peak_rss_mib() * MIB


def live_key_counts(store: WindowStore) -> tuple[int, dict[str, int]]:
    """(total live counter keys, per-family totals) across every retained bucket."""
    per_family: dict[str, int] = dict.fromkeys(FAMILIES, 0)
    with store._lock:  # noqa: SLF001 - measurement tool, deliberately reads internals
        buckets = list(store._buckets.values())  # noqa: SLF001
    for bucket in buckets:
        for family in FAMILIES:
            per_family[family] += len(getattr(bucket, family))
    return sum(per_family.values()), per_family


def ledger_size(store: WindowStore) -> int:
    with store._lock:  # noqa: SLF001
        return len(store._seen)  # noqa: SLF001


def report_sample(elapsed: float, store: WindowStore, events: int) -> tuple[float, float, int]:
    total_keys, per_family = live_key_counts(store)
    with store._lock:  # noqa: SLF001
        buckets = len(store._buckets)  # noqa: SLF001
    rss = rss_bytes()
    families = " ".join(f"{name}={per_family[name]}" for name in FAMILIES if per_family[name])
    print(
        f"t={elapsed:6.0f}s rss={rss / MIB:7.1f}MiB buckets={buckets:4d} "
        f"keys={total_keys:8d} ledger={ledger_size(store):6d} events={events:7d} | {families}",
        flush=True,
    )
    return elapsed, rss, total_keys


async def run_live(url: str, seconds: float, sample_interval: float) -> None:
    store = WindowStore()
    samples: list[tuple[float, float, int]] = []
    events = 0
    posts = 0
    link_posts = 0
    per_minute_keys: dict[int, Counter] = {}
    started = time.monotonic()
    next_sample = started + sample_interval
    deadline = started + seconds

    real_add = store.add

    def counting_add(feats: PostFeatures, *, source_id: object = None) -> None:
        nonlocal posts, link_posts
        posts += 1
        if feats.links:
            link_posts += 1
        real_add(feats, source_id=source_id)

    store.add = counting_add  # type: ignore[method-assign]

    print(f"# live soak: {url} for {seconds:.0f}s, sampling every {sample_interval:.0f}s", flush=True)
    samples.append(report_sample(0.0, store, events))
    # Jetstream drops long-lived subscribers routinely — reproduced at t=240s of
    # a 360s soak — so a soak long enough to be useful is long enough to get
    # dropped. Reconnect with bounded back-off instead of ending the run: the
    # RSS/key regression needs the POST-warm-up samples, which are exactly the
    # ones a first-drop exit throws away. Initial connect failures take the same
    # path, so a flaky start degrades the sample count rather than the run.
    backoff = 1.0
    while time.monotonic() < deadline:
        try:
            async with websockets.connect(url) as ws:
                backoff = 1.0
                while time.monotonic() < deadline:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(1.0, deadline - time.monotonic()))
                    ingest_raw(raw, store)
                    events += 1
                    now = time.monotonic()
                    if now >= next_sample:
                        samples.append(report_sample(now - started, store, events))
                        next_sample = now + sample_interval
        except (TimeoutError, websockets.WebSocketException, OSError) as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            print(
                f"# stream dropped ({type(exc).__name__}) at {events} events — reconnecting in {backoff:.0f}s",
                flush=True,
            )
            await asyncio.sleep(min(backoff, remaining))
            backoff = min(backoff * 2, 30.0)

    elapsed = time.monotonic() - started
    samples.append(report_sample(elapsed, store, events))

    # Per-minute cardinality: exclude the first and last bucket, both partial.
    with store._lock:  # noqa: SLF001
        buckets = sorted(store._buckets.items())  # noqa: SLF001
    for minute, bucket in buckets:
        per_minute_keys[minute] = Counter({family: len(getattr(bucket, family)) for family in FAMILIES})
    ordered = sorted(per_minute_keys.items())[1:-1]
    # Split the report at the compaction horizon. Blending the two would hide
    # the entire effect being measured: a "mean tags/minute" averaged across
    # full and truncated buckets describes neither.
    newest = max(per_minute_keys, default=0)
    whole = [c for minute, c in ordered if minute > newest - windows._FULL_FIDELITY_MINUTES]
    compacted = [c for minute, c in ordered if minute <= newest - windows._FULL_FIDELITY_MINUTES]
    total_per_minute = [sum(c.values()) for c in whole]

    print("\n# ---- live soak results ----")
    print(f"elapsed_seconds        {elapsed:.1f}")
    print(f"jetstream_frames       {events}  ({events / elapsed:.1f}/s)")
    print(f"posts_aggregated       {posts}  ({posts / elapsed:.1f}/s)")
    print(f"link_posts_aggregated  {link_posts}  ({link_posts / elapsed:.2f}/s)")
    print(f"full_fidelity_buckets  {len(whole)}")
    print(f"compacted_buckets      {len(compacted)}")
    for label, group in (("full", whole), ("compacted", compacted)):
        if not group:
            continue
        for family in FAMILIES:
            values = sorted(c[family] for c in group)
            print(
                f"  {label:<9} per-minute {family:<11} mean={statistics.fmean(values):8.1f} "
                f"median={statistics.median(values):8.1f} max={values[-1]:8d}"
            )
        totals = [sum(c.values()) for c in group]
        print(f"  {label:<9} per-minute {'TOTAL':<11} mean={statistics.fmean(totals):8.1f} max={max(totals):8d}")

    # Marginal bytes/key from the stable region: drop the first 5 minutes, over
    # which the 5-minute dedupe ledger is still filling and dominates the slope.
    stable = [s for s in samples if s[0] >= 300.0]
    if len(stable) < 3:
        print("marginal_bytes_per_key n/a  (need a soak past 300s plus 3 samples for the fit)")
    elif stable[-1][2] <= stable[0][2]:
        # Compaction is doing its job: retained keys have stopped growing, so
        # the window is bounded and a linear extrapolation of it is meaningless
        # (fitting a line to the sawtooth yields a NEGATIVE slope). Report the
        # plateau instead — the bound is the result, not a projection.
        retained = [s[2] for s in stable]
        print(f"retained_keys_plateau  min={min(retained)} max={max(retained)} (bounded — no 24h extrapolation applies)")
        print(f"rss_over_stable_region {stable[0][1] / MIB:.1f} -> {stable[-1][1] / MIB:.1f} MiB")
    else:
        slope, intercept = statistics.linear_regression([s[2] for s in stable], [s[1] for s in stable])
        print(f"marginal_bytes_per_key {slope:.1f}  (fit over {len(stable)} samples past the 5-min ledger warm-up)")
        print(f"fit_intercept_mib      {intercept / MIB:.1f}")
        if whole:
            projected = intercept + slope * statistics.fmean(total_per_minute) * WINDOW_MINUTES["24h"]
            print(f"projected_24h_rss_mib  {projected / MIB:.1f}  (steady state, before the merged() transient)")
            print(f"projected_24h_peak_mib {projected * 2 / MIB:.1f}  (merged() transiently copies the window)")
    print(f"final_rss_mib          {rss_bytes() / MIB:.1f}")
    print(f"peak_rss_mib           {peak_rss_bytes() / MIB:.1f}")


def synth_features(minute: int, index: int, ts: float) -> PostFeatures:
    """One post's worth of all-distinct signals — the adversarial cardinality case."""
    stamp = f"{minute}-{index}"
    return PostFeatures(
        ts=ts,
        lang="en",
        hashtags=[f"tag{stamp}"],
        links=[f"https://example{index}.com/{stamp}/path/to/a/reasonably-long-article-slug"],
        emoji=[],
        sentiment=None,
        domains=[f"example{index}.com"],
        hashtag_labels={f"tag{stamp}": f"tag{stamp}"},
        exclusions=Counter(),
    )


class _SimulatedClock:
    """A monotonic clock that advances one minute per simulated minute.

    WindowStore.add() stamps every contribution-ledger entry with
    time.monotonic(). A synthetic fill runs thousands of simulated minutes per
    real second, so against a REAL clock nothing in the ledger ever expires and
    it saturates on contact — which is why this harness used to clear the ledger
    outright once per simulated minute. That clear models a ONE-minute horizon:
    five times more permissive than the production SOURCE_DEDUPE_SECONDS, so it
    admitted workloads the real store refuses and the resulting saturation bound
    was not a production-reachable number.

    Advancing a fake clock instead lets the store's own _expire_seen enforce the
    real horizon, so refusals happen here exactly where they happen in prod.
    """

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


def run_saturate(minutes: int, per_bucket: int, *, compaction: bool = True, head_per_bucket: int = 0) -> None:
    if not compaction:
        # The pre-LAB-1775 shape, measured rather than extrapolated: push the
        # full-fidelity horizon past the whole window so nothing ever compacts.
        windows._FULL_FIDELITY_MINUTES = minutes + 1
    print(f"# saturate: {minutes} buckets x {per_bucket} offered posts/bucket (compaction={compaction})", flush=True)
    print(
        "# each synthetic post offers 3 ledger-eligible signals (tag, url, domain); "
        f"the ledger admits {windows.MAX_SOURCE_LEDGER_ENTRIES} over {windows.SOURCE_DEDUPE_SECONDS:.0f}s",
        flush=True,
    )
    store = WindowStore(max_minutes=minutes)
    base_minute = int(time.time() // 60) - minutes + 1
    baseline = rss_bytes()
    clock = _SimulatedClock()
    real_time = windows.time
    windows.time = clock  # type: ignore[assignment]  # add() uses only .monotonic()
    try:
        # The worst case for the uncompacted head is a QUIET TAIL THEN A BURST:
        # the tail leaves the ledger near-empty, so the head minutes can each
        # draw on the full MAX_SOURCE_LEDGER_ENTRIES rather than on the
        # steady-state rate. Offering the heavy load only to the head measures
        # that directly instead of paying 1,440 buckets of it.
        head_starts_at = minutes - windows._FULL_FIDELITY_MINUTES
        for offset in range(minutes):
            minute = base_minute + offset
            ts = float(minute * 60)
            clock.now = float(offset * 60)  # one simulated minute of ledger expiry
            offered = head_per_bucket if (head_per_bucket and offset >= head_starts_at) else per_bucket
            for index in range(offered):
                # A distinct synthetic source per post: the per-source cap must not
                # be what refuses the cardinality we are trying to build.
                store.add(synth_features(minute, index, ts), source_id=f"did:plc:synthetic{minute}-{index}")
            if offset % 240 == 0 or offset == minutes - 1:
                total_keys, _ = live_key_counts(store)
                print(
                    f"  bucket {offset + 1:5d}/{minutes} rss={rss_bytes() / MIB:7.1f}MiB keys={total_keys:9d}",
                    flush=True,
                )
    finally:
        windows.time = real_time  # type: ignore[assignment]

    gc.collect()
    steady = rss_bytes()
    total_keys, per_family = live_key_counts(store)
    with store._lock:  # noqa: SLF001
        refused = sum(sum(b.excluded.values()) for b in store._buckets.values())  # noqa: SLF001
    print("\n# ---- saturate results ----")
    print(f"buckets                {minutes}")
    print(f"offered_posts          {minutes * per_bucket}")
    print(f"refused_contributions  {refused}  (ledger refusals, i.e. load the real store would not accept)")
    print(f"live_counter_keys      {total_keys}")
    print(f"keys_per_bucket        {total_keys / max(1, minutes):.1f}  (achieved, not offered)")
    for family in FAMILIES:
        if per_family[family]:
            print(f"  {family:<12} {per_family[family]}")
    print(f"baseline_rss_mib       {baseline / MIB:.1f}")
    print(f"steady_rss_mib         {steady / MIB:.1f}")
    print(f"bytes_per_key          {(steady - baseline) / max(1, total_keys):.1f}")

    # Baseline the merge transient against the high-water mark as it stood BEFORE
    # merged() ran. peak_rss_bytes() is a process-lifetime maximum, so peak minus
    # STEADY also charges merged() for whatever bucket construction had already
    # peaked at — which is most of it on a 1,440-bucket fill.
    pre_merge_peak = peak_rss_bytes()
    merged = store.merged("24h", time.time())
    peak = peak_rss_bytes()
    print(f"merged_24h_keys        {sum(len(getattr(merged, f)) for f in FAMILIES)}")
    print(f"peak_rss_mib           {peak / MIB:.1f}  (process lifetime, includes the merged() transient copy)")
    print(f"merge_transient_mib    {(peak - pre_merge_peak) / MIB:.1f}  (high-water increase attributable to merged())")
    del merged


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)

    live = sub.add_parser("live", help="soak against the live public Jetstream firehose")
    live.add_argument("--url", default="wss://jetstream2.us-east.bsky.network/subscribe")
    live.add_argument("--seconds", type=float, default=600.0)
    live.add_argument("--sample-interval", type=float, default=30.0)

    sat = sub.add_parser("saturate", help="synthetic worst-case bucket fill")
    sat.add_argument("--minutes", type=int, default=WINDOW_MINUTES["24h"])
    sat.add_argument("--per-bucket", type=int, default=200)
    sat.add_argument("--no-compaction", action="store_true", help="measure the pre-LAB-1775 shape")
    sat.add_argument(
        "--head-per-bucket",
        type=int,
        default=0,
        help="offer this many posts/bucket to the uncompacted head only (worst case: quiet tail, then a burst)",
    )

    args = parser.parse_args()
    if args.mode == "live":
        asyncio.run(run_live(subscribe_url(args.url), args.seconds, args.sample_interval))
    else:
        run_saturate(
            args.minutes,
            args.per_bucket,
            compaction=not args.no_compaction,
            head_per_bucket=args.head_per_bucket,
        )


if __name__ == "__main__":
    main()
