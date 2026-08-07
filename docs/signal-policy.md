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
2. case-folded with Unicode `casefold()` for its canonical key;
3. limited to 64 characters and one complete token of Unicode word characters,
   with hyphens allowed only between word-character groups.

For text fallback, `#` must begin at a non-word/non-`#` boundary. Whitespace
and punctuation other than an internal hyphen end the token. A facet is already
supposed to carry one bare tag, so a facet containing whitespace, surrounding
`#`, or other punctuation is rejected rather than truncated.

The public `canonical` field is the ranking key. The `tag` display field is the
most frequent NFKC-normalized spelling seen in the selected window, with a
lexical tie-break. Display case is therefore preserved without splitting the
count.

The following canonical tags are explicitly excluded:
`adult`, `follow4follow`, `followforfollow`, `nsfw`, `porn`, `porno`,
`spam`, and `xxx`. Matches are exact, never substrings.

## Links and domains

Only syntactically valid `http` and `https` URLs are candidates. Canonical URLs:

- lowercase and IDNA-encode the host;
- canonicalize global IP literals and reject non-global addresses;
- remove a trailing host dot and the default port (80 for HTTP, 443 for HTTPS);
- preserve the path and meaningful query components byte-for-byte;
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
per rolling five minutes. The limit is per signal family: two distinct URLs on
one domain can both enter the URL ranking, while that source contributes only
once to the domain ranking during the horizon.

The Jetstream DID exists only as a local argument at the ingestion boundary.
`WindowStore` immediately folds it into a 128-bit keyed BLAKE2 digest for the
complete `(source, signal family, canonical value)` tuple. The key is random for
each process. Only these opaque tuple digests and expiry timestamps live in the
five-minute ledger.

The key, digests, and raw DIDs are never put in minute buckets, checkpoints,
CacheKit values, logs, history, or health output. The ledger is not restored:
after a process restart the key rotates and the five-minute bound starts fresh.
That small, explicit continuity gap is preferable to creating a durable
pseudonymous author index. A post without a usable source can still count toward
volume, language, emoji, and sentiment aggregates, but its hashtag/link/domain
contributions are excluded so missing identity cannot bypass the public trend
bound.

## Public-safety exclusions

URL checks are local and syntactic. The ingestion hot path never resolves DNS,
opens a socket, follows a redirect, or fetches submitted content.

Skyline rejects:

- non-HTTP(S) schemes;
- credentials in an authority;
- control characters, whitespace, backslashes, bad percent escapes, invalid
  hosts/ports, browser-dependent numeric hosts, and overlong URLs;
- localhost, single-label/local-network names, and non-global IP literals;
- an exact host or subdomain of `pornhub.com`, `redtube.com`,
  `spam.example.com`, `xhamster.com`, `xnxx.com`, or `xvideos.com`.

This intentionally small explicit filter is reviewable. It is not a crawler,
page classifier, or permanent blocklist of people.

## Transparency fields

Every public aggregate includes:

- `normalization_version`: the policy version above;
- `total_events_considered`: structurally valid post-create events in the
  selected window (also retained as `total_posts` for compatibility);
- `excluded_count_by_reason`: aggregate counts of candidate signal
  contributions omitted for normalization, safety, in-event duplication,
  missing source, or the rolling source bound.

Exclusion counts are contribution counts, not unique people and not necessarily
unique events: one post can contain more than one excluded candidate.

## Recorded evaluation

`ingester/tests/fixtures/signal_quality_events.jsonl` contains case and Unicode
variants, tracking URLs, a repetitive source, broad distinct-source activity,
malformed/dangerous URLs, and representative explicit adult/spam terms. The
test evaluates the same recorded input before and after source bounding:

| Signal | Normalized, before source bound | Published after policy |
| :--- | ---: | ---: |
| `flashsale` from one repetitive source | 6 | 1 |
| `community` from four distinct sources | 4 | 4 |

The repetitive source no longer outranks broader activity. The fixture also
asserts exact exclusion-reason totals and that no fixture DID appears in a
bucket checkpoint or any public aggregate.
