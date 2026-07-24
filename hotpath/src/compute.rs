//! Pure hot-path compute, target-independent so `cargo test` proves it
//! natively — no network, no credentials, no Workers runtime.
//!
//! Three jobs (LAB-746):
//! 1. interop/v1 key derivation for the five locked Skyline operations
//! 2. xxHash3-64 integrity verification of cached payloads
//! 3. in-edge aggregation: merging count-map window slices
//!
//! The contract (namespace, operations, windows, TTLs, byte-locked example
//! keys) is `docs/architecture.md` — locked, not re-derived here.

use std::collections::BTreeMap;

use cachekit::interop::{interop_key, serialize_value, InteropValue};

/// Locked interop namespace (key segment 1).
pub const NAMESPACE: &str = "bluesky-thinking";

/// The five locked operations (key segment 2).
pub const OPERATIONS: [&str; 5] = [
    "trending_hashtags",
    "trending_links",
    "lang_mix",
    "posts_per_minute",
    "top_emoji",
];

/// Locked window → TTL mapping; `None` for anything outside the contract.
#[must_use]
pub fn window_ttl_seconds(window: &str) -> Option<u64> {
    match window {
        "5m" => Some(60),
        "1h" => Some(300),
        "24h" => Some(900),
        _ => None,
    }
}

/// Derive the interop/v1 cache key for a locked operation + window.
///
/// Rejects anything outside the locked contract before touching the SDK, so
/// the worker can never mint a key the other two SDKs won't also derive.
pub fn derive_key(operation: &str, window: &str) -> Result<String, String> {
    if !OPERATIONS.contains(&operation) {
        return Err(format!(
            "unknown operation {operation:?}; locked operations: {}",
            OPERATIONS.join(", ")
        ));
    }
    if window_ttl_seconds(window).is_none() {
        return Err(format!(
            "unknown window {window:?}; locked windows: 5m, 1h, 24h"
        ));
    }
    interop_key(NAMESPACE, operation, &[InteropValue::from(window)]).map_err(|e| e.to_string())
}

/// Integrity report for a cached payload.
#[derive(Debug)]
pub struct VerifyReport {
    pub size_bytes: usize,
    /// xxHash3-64, big-endian, 16 lowercase hex chars — byte-identical to the
    /// checksum a `StorageEnvelope` would embed for the same payload.
    pub xxh3_64: String,
    /// Whether the payload parses as exactly one interop/v1 MessagePack
    /// document (no trailing bytes, not a Python-internal CK frame).
    pub valid_interop_value: bool,
    /// Decode diagnostic when `valid_interop_value` is false.
    pub interop_error: Option<String>,
    /// Comparison against a caller-supplied expected checksum, if given.
    pub matches_expected: Option<bool>,
}

/// Compute the xxHash3-64 integrity checksum + interop-format validity of a
/// payload, optionally comparing against an expected checksum.
#[must_use]
pub fn verify_payload(bytes: &[u8], expected: Option<[u8; 8]>) -> VerifyReport {
    let checksum = cachekit_core::checksum(bytes);
    let interop_result = cachekit::interop::deserialize::<serde::de::IgnoredAny>(bytes);
    VerifyReport {
        size_bytes: bytes.len(),
        xxh3_64: hex::encode(checksum),
        valid_interop_value: interop_result.is_ok(),
        interop_error: interop_result.err().map(|e| e.to_string()),
        matches_expected: expected.map(|e| cachekit_core::verify_checksum(bytes, &e)),
    }
}

/// Parse a 16-hex-char xxHash3-64 checksum into its big-endian bytes.
pub fn parse_checksum_hex(s: &str) -> Result<[u8; 8], String> {
    let bytes = hex::decode(s).map_err(|e| format!("invalid checksum hex: {e}"))?;
    <[u8; 8]>::try_from(bytes.as_slice())
        .map_err(|_| format!("checksum must be 8 bytes (16 hex chars), got {}", s.len()))
}

/// Result of merging count-map window slices at the edge.
#[derive(Debug)]
pub struct MergeResult {
    /// Merged counts, highest first (ties broken by key, ascending), truncated
    /// to the requested top-N.
    pub top: Vec<(String, i64)>,
    /// Canonical interop/v1 MessagePack encoding of the top-N map — suitable
    /// for writing back to the shared cache byte-identically from any SDK.
    pub canonical_msgpack: Vec<u8>,
    /// Distinct keys across all slices before truncation.
    pub total_keys: usize,
    pub slice_count: usize,
}

/// Merge N interop/v1 count-map slices (`{string: int}` MessagePack documents,
/// e.g. per-window hashtag counts) into one top-N ranking.
///
/// Every slice must be a strict interop document; a CK frame or trailing bytes
/// in any slice fails the whole merge — silently skipping a corrupt slice
/// would return wrong analytics as if they were right.
pub fn merge_count_slices(slices: &[Vec<u8>], top_n: usize) -> Result<MergeResult, String> {
    let mut merged: BTreeMap<String, i64> = BTreeMap::new();
    for (i, slice) in slices.iter().enumerate() {
        let counts: BTreeMap<String, i64> = cachekit::interop::deserialize(slice)
            .map_err(|e| format!("slice {i}: {e}"))?;
        for (key, count) in counts {
            let entry = merged.entry(key).or_insert(0);
            *entry = entry.saturating_add(count);
        }
    }
    let total_keys = merged.len();

    let mut ranked: Vec<(String, i64)> = merged.into_iter().collect();
    // BTreeMap iteration is already key-ascending, and the sort is stable, so
    // equal counts keep that key order.
    ranked.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
    ranked.truncate(top_n);

    let top_map: BTreeMap<String, InteropValue> = ranked
        .iter()
        .map(|(k, v)| (k.clone(), InteropValue::from(*v)))
        .collect();
    let canonical_msgpack =
        serialize_value(&InteropValue::Map(top_map)).map_err(|e| e.to_string())?;

    Ok(MergeResult {
        top: ranked,
        canonical_msgpack,
        total_keys,
        slice_count: slices.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode a `{str: int}` map as a canonical interop/v1 document — what the
    /// Python ingester's window slices look like on the wire.
    fn count_slice(pairs: &[(&str, i64)]) -> Vec<u8> {
        let map: BTreeMap<String, InteropValue> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), InteropValue::from(*v)))
            .collect();
        serialize_value(&InteropValue::Map(map)).unwrap()
    }

    // ── Key derivation: the byte-locked vectors from docs/architecture.md ────

    #[test]
    fn derives_byte_locked_keys() {
        // Verified 3-way in the LAB-735 spike (py 0.15.0, ts 0.1.3, rs 0.4.0).
        let locked = [
            (
                "trending_hashtags",
                "5m",
                "bluesky-thinking:trending_hashtags:230037def14c9a89b18603f313d982d6a3f7acd4af5147b2f6ae2c257b82ce57",
            ),
            (
                "trending_hashtags",
                "1h",
                "bluesky-thinking:trending_hashtags:17092aa9bfa2cc2fa567c40b8d5a23d93ee9f148f7754467eeb90bd0168d9301",
            ),
            (
                "trending_hashtags",
                "24h",
                "bluesky-thinking:trending_hashtags:587d262535cbfca724700a52f210eaa396da79f44e0cb3135afdd2eecb3907f3",
            ),
            (
                "posts_per_minute",
                "5m",
                "bluesky-thinking:posts_per_minute:230037def14c9a89b18603f313d982d6a3f7acd4af5147b2f6ae2c257b82ce57",
            ),
        ];
        for (op, window, expected) in locked {
            assert_eq!(derive_key(op, window).unwrap(), expected, "{op}/{window}");
        }
    }

    #[test]
    fn same_window_hashes_identically_across_all_operations() {
        // The args-hash covers only the argument array, so every operation
        // shares the same ("5m") suffix — the operation segment is the identity.
        let suffix = "230037def14c9a89b18603f313d982d6a3f7acd4af5147b2f6ae2c257b82ce57";
        for op in OPERATIONS {
            let key = derive_key(op, "5m").unwrap();
            assert_eq!(key, format!("{NAMESPACE}:{op}:{suffix}"));
        }
    }

    #[test]
    fn rejects_off_contract_inputs() {
        assert!(derive_key("sentiment", "5m").is_err());
        assert!(derive_key("trending_hashtags", "10m").is_err());
        assert!(derive_key("Trending_Hashtags", "5m").is_err());
    }

    #[test]
    fn ttl_mapping_matches_locked_contract() {
        assert_eq!(window_ttl_seconds("5m"), Some(60));
        assert_eq!(window_ttl_seconds("1h"), Some(300));
        assert_eq!(window_ttl_seconds("24h"), Some(900));
        assert_eq!(window_ttl_seconds("7d"), None);
    }

    // ── Integrity verification ────────────────────────────────────────────────

    #[test]
    fn verify_reports_checksum_and_validity() {
        let payload = count_slice(&[("rust", 42)]);
        let report = verify_payload(&payload, None);
        assert_eq!(report.size_bytes, payload.len());
        assert_eq!(report.xxh3_64, hex::encode(cachekit_core::checksum(&payload)));
        assert!(report.valid_interop_value);
        assert_eq!(report.interop_error, None);
        assert_eq!(report.matches_expected, None);
    }

    #[test]
    fn verify_detects_corruption_via_expected_checksum() {
        let payload = count_slice(&[("rust", 42)]);
        let good = cachekit_core::checksum(&payload);
        assert_eq!(verify_payload(&payload, Some(good)).matches_expected, Some(true));

        let mut corrupted = payload.clone();
        *corrupted.last_mut().unwrap() ^= 0x01;
        assert_eq!(verify_payload(&corrupted, Some(good)).matches_expected, Some(false));
    }

    #[test]
    fn verify_flags_ck_frames_and_trailing_bytes() {
        // Python auto-mode frame: not interop-readable, and the report says why.
        let report = verify_payload(b"CK\x03\x00\x00\x00\x02{}", None);
        assert!(!report.valid_interop_value);
        assert!(report.interop_error.unwrap().contains("CK"));

        let mut trailing = count_slice(&[("a", 1)]);
        trailing.push(0x00);
        let report = verify_payload(&trailing, None);
        assert!(!report.valid_interop_value);
        assert!(report.interop_error.unwrap().contains("trailing"));
    }

    #[test]
    fn checksum_hex_roundtrip_and_rejection() {
        let payload = b"skyline";
        let hex_str = hex::encode(cachekit_core::checksum(payload));
        assert_eq!(
            parse_checksum_hex(&hex_str).unwrap(),
            cachekit_core::checksum(payload)
        );
        assert!(parse_checksum_hex("zz").is_err());
        assert!(parse_checksum_hex("abcd").is_err()); // 4 bytes short
    }

    // ── Merge aggregation ─────────────────────────────────────────────────────

    #[test]
    fn merges_slices_and_ranks_by_count_then_key() {
        let slices = vec![
            count_slice(&[("alpha", 2), ("beta", 1)]),
            count_slice(&[("beta", 3), ("gamma", 2)]),
        ];
        let result = merge_count_slices(&slices, 10).unwrap();
        assert_eq!(result.slice_count, 2);
        assert_eq!(result.total_keys, 3);
        // beta=4 first; alpha/gamma tie at 2 → key-ascending.
        assert_eq!(
            result.top,
            vec![
                ("beta".to_owned(), 4),
                ("alpha".to_owned(), 2),
                ("gamma".to_owned(), 2),
            ]
        );
        // Canonical bytes are exactly what any SDK would write for that map.
        assert_eq!(
            result.canonical_msgpack,
            count_slice(&[("alpha", 2), ("beta", 4), ("gamma", 2)])
        );
    }

    #[test]
    fn merge_truncates_to_top_n() {
        let slices = vec![count_slice(&[("a", 1), ("b", 3), ("c", 2)])];
        let result = merge_count_slices(&slices, 2).unwrap();
        assert_eq!(result.top, vec![("b".to_owned(), 3), ("c".to_owned(), 2)]);
        assert_eq!(result.total_keys, 3);
        assert_eq!(
            result.canonical_msgpack,
            count_slice(&[("b", 3), ("c", 2)])
        );
    }

    #[test]
    fn merge_rejects_corrupt_slice_naming_its_index() {
        let slices = vec![count_slice(&[("a", 1)]), b"CK\x03junk".to_vec()];
        let err = merge_count_slices(&slices, 10).unwrap_err();
        assert!(err.starts_with("slice 1:"), "got: {err}");
    }

    #[test]
    fn merge_of_nothing_is_empty() {
        let result = merge_count_slices(&[], 10).unwrap();
        assert!(result.top.is_empty());
        assert_eq!(result.total_keys, 0);
        // Canonical encoding of an empty map: fixmap 0.
        assert_eq!(result.canonical_msgpack, vec![0x80]);
    }

    #[test]
    fn merge_saturates_instead_of_overflowing() {
        let slices = vec![
            count_slice(&[("x", i64::MAX)]),
            count_slice(&[("x", 1)]),
        ];
        let result = merge_count_slices(&slices, 1).unwrap();
        assert_eq!(result.top, vec![("x".to_owned(), i64::MAX)]);
    }
}
