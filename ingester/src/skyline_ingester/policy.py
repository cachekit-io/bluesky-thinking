"""Versioned, deterministic normalization and public-safety policy.

This module is deliberately pure: URL decisions are made from syntax and fixed
policy data only. The ingester never resolves a hostname or fetches a submitted
URL while processing Jetstream events.
"""

from __future__ import annotations

import ipaddress
import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import unquote_plus, urlsplit, urlunsplit

NORMALIZATION_VERSION = "skyline-normalization-v1"
MAX_TAG_LENGTH = 64
MAX_URL_LENGTH = 2_048

# Exact canonical-tag matches only. Substrings are intentionally not filtered:
# e.g. ``class`` must not disappear because it contains another short token.
FILTERED_TAGS = frozenset(
    {
        "adult",
        "follow4follow",
        "followforfollow",
        "nsfw",
        "porn",
        "porno",
        "spam",
        "xxx",
    }
)

# Exact host or subdomain matches. This is a public-output policy, not a claim
# about every page on a domain. Keep additions reviewed and intentionally small.
FILTERED_DOMAINS = frozenset(
    {
        "pornhub.com",
        "redtube.com",
        "spam.example.com",
        "xhamster.com",
        "xnxx.com",
        "xvideos.com",
    }
)

# Parameters with well-established cross-site attribution semantics. Ambiguous
# names such as ``ref``, ``source``, and ``campaign`` are preserved because they
# can identify a real resource on some sites.
TRACKING_PARAMETERS = frozenset(
    {
        "dclid",
        "fbclid",
        "gclid",
        "gbraid",
        "igshid",
        "mc_cid",
        "mc_eid",
        "msclkid",
        "utm_campaign",
        "utm_content",
        "utm_id",
        "utm_medium",
        "utm_source",
        "utm_term",
        "wbraid",
    }
)

EXCLUSION_REASONS = frozenset(
    {
        "duplicate_in_event_domain",
        "duplicate_in_event_tag",
        "duplicate_in_event_url",
        "duplicate_source_domain",
        "duplicate_source_tag",
        "duplicate_source_url",
        "filtered_domain",
        "filtered_tag",
        "malformed_tag",
        "malformed_url",
        "missing_source_domain",
        "missing_source_tag",
        "missing_source_url",
        "unsafe_host",
        "unsafe_scheme",
    }
)

_BAD_PERCENT_ESCAPE_RE = re.compile(r"%(?![0-9a-fA-F]{2})")
_DNS_LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_LOCAL_SUFFIXES = (".home", ".internal", ".lan", ".local", ".localhost")


@dataclass(frozen=True, slots=True)
class NormalizedHashtag:
    canonical: str
    display: str


@dataclass(frozen=True, slots=True)
class NormalizedLink:
    uri: str
    domain: str


def normalize_hashtag(value: object) -> tuple[NormalizedHashtag | None, str | None]:
    """Normalize one bare tag, returning ``(value, exclusion_reason)``.

    Facet values must be one complete token. Text extraction applies the same
    token grammar at ``#`` boundaries before calling this function.
    """
    if not isinstance(value, str):
        return None, "malformed_tag"
    display = unicodedata.normalize("NFKC", value).strip()
    parts = display.split("-")
    if (
        not display
        or len(display) > MAX_TAG_LENGTH
        or any(not part or any(not _is_tag_word_character(ch) for ch in part) for part in parts)
    ):
        return None, "malformed_tag"
    if not any(ch.isalnum() for ch in display):
        return None, "malformed_tag"
    canonical = display.casefold()
    if canonical in FILTERED_TAGS:
        return None, "filtered_tag"
    return NormalizedHashtag(canonical=canonical, display=display), None


def hashtags_from_text(text: str) -> list[str]:
    """Extract bare hashtag candidates at Unicode-aware text boundaries."""
    normalized = unicodedata.normalize("NFKC", text)
    candidates: list[str] = []
    index = 0
    while index < len(normalized):
        if normalized[index] != "#":
            index += 1
            continue
        if index and (_is_tag_word_character(normalized[index - 1]) or normalized[index - 1] == "#"):
            index += 1
            continue
        cursor = index + 1
        if cursor >= len(normalized) or not _is_tag_word_character(normalized[cursor]):
            index += 1
            continue
        while cursor < len(normalized):
            if _is_tag_word_character(normalized[cursor]):
                cursor += 1
                continue
            if normalized[cursor] == "-" and cursor + 1 < len(normalized) and _is_tag_word_character(normalized[cursor + 1]):
                cursor += 1
                continue
            break
        candidates.append(normalized[index + 1 : cursor])
        index = cursor
    return candidates


def normalize_link(value: object) -> tuple[NormalizedLink | None, str | None]:
    """Canonicalize a public HTTP(S) URL without DNS or network access."""
    if not isinstance(value, str) or not value or len(value) > MAX_URL_LENGTH:
        return None, "malformed_url"
    if "\\" in value or _BAD_PERCENT_ESCAPE_RE.search(value):
        return None, "malformed_url"
    if any(ch.isspace() or unicodedata.category(ch).startswith("C") for ch in value):
        return None, "malformed_url"

    try:
        parts = urlsplit(value)
        scheme = parts.scheme.casefold()
        if scheme not in {"http", "https"}:
            return None, "unsafe_scheme"
        if parts.username is not None or parts.password is not None:
            return None, "unsafe_host"
        raw_host = parts.hostname
        port = parts.port
    except (UnicodeError, ValueError):
        return None, "malformed_url"
    if not raw_host:
        return None, "malformed_url"
    if port == 0:
        return None, "malformed_url"

    host, reason = normalize_domain(raw_host)
    if host is None:
        return None, reason
    is_ipv6 = ":" in host

    default_port = 80 if scheme == "http" else 443
    authority = f"[{host}]" if is_ipv6 else host
    if port is not None and port != default_port:
        authority = f"{authority}:{port}"

    query = _strip_tracking_parameters(parts.query)
    uri = urlunsplit((scheme, authority, parts.path or "/", query, ""))
    return NormalizedLink(uri=uri, domain=host), None


def normalize_domain(value: object) -> tuple[str | None, str | None]:
    """Normalize one bare public host for domain ranking."""
    if not isinstance(value, str) or not value or len(value) > 253:
        return None, "unsafe_host"
    host, _is_ipv6 = _normalize_host(value)
    if host is None:
        return None, "unsafe_host"
    if _domain_matches(host, FILTERED_DOMAINS):
        return None, "filtered_domain"
    return host, None


def _normalize_host(raw_host: str) -> tuple[str | None, bool]:
    host = raw_host.rstrip(".").casefold()
    if not host or "%" in host:
        return None, False
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None:
        return (address.compressed, isinstance(address, ipaddress.IPv6Address)) if address.is_global else (None, False)

    # Reject browser-dependent numeric host spellings (e.g. 2130706433 or
    # 0x7f000001) instead of risking an alternate spelling of a local address.
    numeric_labels = host.split(".")
    if host.isdecimal() or host.startswith("0x") or all(label.isdecimal() or label.startswith("0x") for label in numeric_labels):
        return None, False
    if host == "localhost" or host.endswith(_LOCAL_SUFFIXES) or "." not in host:
        return None, False
    try:
        ascii_host = host.encode("idna").decode("ascii").casefold()
    except UnicodeError:
        return None, False
    if len(ascii_host) > 253:
        return None, False
    labels = ascii_host.split(".")
    if any(_DNS_LABEL_RE.fullmatch(label) is None for label in labels):
        return None, False
    return ascii_host, False


def _is_tag_word_character(value: str) -> bool:
    return value == "_" or unicodedata.category(value)[0] in {"L", "M", "N"}


def _domain_matches(host: str, domains: frozenset[str]) -> bool:
    return any(host == domain or host.endswith(f".{domain}") for domain in domains)


def _strip_tracking_parameters(query: str) -> str:
    if not query:
        return ""
    kept: list[str] = []
    for component in query.split("&"):
        raw_name = component.partition("=")[0]
        name = unquote_plus(raw_name).casefold()
        if name not in TRACKING_PARAMETERS:
            kept.append(component)
    return "&".join(kept)
