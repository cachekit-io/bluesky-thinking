# Skyline public signal policy

Skyline's public trend lists are normalized, source-bounded, and safety-filtered
before publication. This policy is deterministic and versioned as
`skyline-normalization-v1`; every aggregate identifies the version that produced
it.

The policy improves a public network pulse. It is not account reputation,
individual moderation, a claim that every unfiltered result is safe, or an
attempt to classify people.

## Hashtags

For ranking, each candidate is:

1. normalized with Unicode NFKC (so compatibility forms such as full-width
   Latin characters share a key);
2. case-folded with Unicode `casefold()` and NFKC-normalized again for an
   idempotent canonical key;
3. limited to 64 characters in both display and canonical form, and one
   complete token of Unicode word characters,
   with hyphens allowed only between word-character groups.

For text fallback, `#` must begin at a non-word/non-`#` boundary. Whitespace
and punctuation other than an internal hyphen end the token. A facet is already
supposed to carry one bare tag, so a facet containing whitespace, surrounding
`#`, or other punctuation is rejected rather than truncated.

The existing public `tag` field remains the case-folded ranking key. The
additive `display` field is the most frequent NFKC-normalized spelling seen in
the selected window, with a lexical tie-break. Display case is therefore
preserved without changing the meaning of `tag` or splitting the count.

Work per post is bounded before normalization: at most 4,096 text characters,
32 hashtag candidates, and 16 link candidates are normalized. The ingester scans
the frame's declared facet features only to identify tag/link candidates; other
feature types do not enter the signal denominator. Genuine tag/link declarations
beyond their family cap are reported as `candidate_limit_tag` or
`candidate_limit_url`. If supplied tag facets produce no usable tag, the same
bounded text fallback is still applied. Repeated rejected spellings within one
post count once per normalized token, whether they came from facets or text.

The following canonical tags are explicitly excluded:
`adult`, `follow4follow`, `followforfollow`, `nsfw`, `porn`, `porno`,
`spam`, and `xxx`. Matches are exact, never substrings.

## Links and domains

Only syntactically valid `http` and `https` URLs are candidates. Canonical URLs:

- lowercase and IDNA-encode the host before every host-safety check;
- canonicalize global IP literals and apply the public-safety host rules below
  after IDNA;
- remove a trailing host dot and the default port (80 for HTTP, 443 for HTTPS);
- insert `/` when the input URL has an empty path, otherwise preserve the path
  and meaningful query components byte-for-byte;
- remove the fragment;
- remove only the reviewed attribution parameters below.

The stripped parameter names are:
`dclid`, `fbclid`, `gclid`, `gbraid`, `igshid`, `mc_cid`, `mc_eid`,
`msclkid`, `utm_campaign`, `utm_content`, `utm_id`, `utm_medium`,
`utm_source`, `utm_term`, and `wbraid`.

Ambiguous names such as `ref`, `source`, and `campaign` are deliberately
retained because sites can use them to identify a meaningful resource. Query
order, duplicate meaningful parameters, percent-encoding, and non-default ports
are also retained.

`trending_links` publishes both canonical individual URLs and a `domains`
ranking keyed by the normalized host. Tracking variants of one story therefore
share one URL key, while domain activity is visible alongside individual
resources.

## Source contribution bound and privacy

One source can contribute a given canonical hashtag, URL, or domain at most once
per rolling five minutes of process time. Untrusted event timestamps determine
the event's minute bucket but never expire this ledger. The limit is per signal
family: two distinct URLs on one domain can both enter the URL ranking, while
that source contributes only once to the domain ranking during the horizon.

The Jetstream DID exists only as a local argument at the ingestion boundary.
`WindowStore` immediately folds it into a 128-bit keyed BLAKE2 digest for the
complete `(source, signal family, canonical value)` tuple. The key is random for
each process. Only these opaque tuple digests and expiry timestamps live in the
five-minute ledger.

The key, digests, and raw DIDs are never put in minute buckets, checkpoints,
CacheKit values, logs, history, or health output. `/health` exposes only an
aggregate `events_missing_source` counter so a Jetstream schema change cannot
silently empty all public trend rankings. The ledger is not restored:
after a process restart the key rotates and the five-minute bound starts fresh.
That small, explicit continuity gap is preferable to creating a durable
pseudonymous author index. A post without a usable source can still count toward
volume, language, emoji, and sentiment aggregates, but its hashtag/link/domain
contributions are excluded so missing identity cannot bypass the public trend
bound.

Jetstream reconnects resume from the last cursor. If a backlog longer than five
minutes is delivered faster than real time, its trend signals share the current
process-time bound and can be under-counted; event-volume and language/emoji
aggregates remain exact. Event timestamps are deliberately not used to expire
the ledger because they are untrusted and previously allowed a source to erase
the bound.

## Public-safety exclusions

URL checks are local and syntactic. The ingestion hot path never resolves DNS,
opens a socket, follows a redirect, or fetches submitted content. Known
wildcard-DNS and loopback providers are denied as a class, and hostnames containing
dotted or dashed non-global IPv4 spellings are rejected as additional defence. An
arbitrary hostname controlled by an attacker can still resolve to a
private address: proving otherwise would require the DNS lookup this boundary
deliberately forbids. Published links therefore remain untrusted destinations for
viewer-side safe-link handling; this policy prevents the ingester itself from
performing SSRF, but cannot promise that every clickable hostname resolves public.

Skyline rejects:

- non-HTTP(S) schemes;
- credentials in an authority;
- control characters, whitespace, backslashes, bad percent escapes, invalid
  hosts/ports, browser-dependent numeric hosts, and overlong URLs;
- localhost, single-label or local-network names, non-global IP literals, and
  syntactic private-target names;
- an exact host or subdomain of the reviewed wildcard-DNS/loopback providers
  `1u.ms`, `local.gd`, `localho.st`, `localhost.direct`, `localtest.me`,
  `lvh.me`, `nip.io`, `sslip.io`, `traefik.me`, or `vcap.me`;
- an exact host or subdomain of `pornhub.com`, `redtube.com`, `xhamster.com`,
  `xnxx.com`, or `xvideos.com`.

This intentionally small explicit filter is reviewable. It is not a crawler,
page classifier, or permanent blocklist of people.

## Transparency fields

Every public aggregate includes:

- `normalization_version`: the policy version above;
- `total_events_considered`: structurally valid post-create events in the
  selected window (also retained as `total_posts` for compatibility);
- `total_signal_candidates`: accepted or excluded tag, URL, domain, and
  restored-checkpoint decisions in the selected window — the denominator for
  the exclusion counts; unrelated facet feature types never enter it;
- `excluded_count_by_reason`: aggregate counts of candidate signal
  contributions omitted for normalization, safety, in-event duplication,
  missing source, or the rolling source bound.

Exclusion counts are contribution counts, not unique people and not necessarily
unique events: one post can contain more than one excluded candidate.

## Recorded evaluation

`ingester/tests/fixtures/signal_quality_events.jsonl` contains case and Unicode
variants, tracking URLs, a repetitive source, broad distinct-source activity,
malformed/dangerous URLs, and representative explicit adult/spam terms. The
test evaluates the same recorded input before and after source bounding in the
5-minute window:

| Signal | Normalized, before source bound | Published after policy |
| :--- | ---: | ---: |
| `flashsale` from one repetitive source | 6 | 1 |
| `community` from four distinct sources | 4 | 4 |

The repetitive source no longer outranks broader activity in that 5-minute
snapshot. The rule is a rate bound, not a permanent per-source cap: a source can
contribute the same signal at most 12 times in 1 hour and 288 times in 24 hours
when it contributes once at each five-minute horizon. Those longer-window
limits have separate regression coverage; the policy does not claim to detect
sock puppets or rotated tag variants. The fixture also
asserts exact exclusion-reason totals and that no fixture DID appears in a
bucket checkpoint or any public aggregate.
