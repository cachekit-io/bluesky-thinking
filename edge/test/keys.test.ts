/**
 * Byte-locked key vectors from docs/architecture.md (Stage 1, verified
 * 3-way across the Python, Rust and TS SDKs). If this test fails, the edge
 * would read keys the ingester never writes — do not "fix" the vectors,
 * find what changed in key derivation.
 */
import { describe, expect, it } from 'vitest';
import { generateInteropKey } from '@cachekit-io/cachekit';
import { NAMESPACE, OPERATIONS, WINDOWS } from '../src/handler.js';

const LOCKED_VECTORS: Record<string, [string, string]> = {
  'bluesky-thinking:trending_hashtags:230037def14c9a89b18603f313d982d6a3f7acd4af5147b2f6ae2c257b82ce57':
    ['trending_hashtags', '5m'],
  'bluesky-thinking:trending_hashtags:17092aa9bfa2cc2fa567c40b8d5a23d93ee9f148f7754467eeb90bd0168d9301':
    ['trending_hashtags', '1h'],
  'bluesky-thinking:trending_hashtags:587d262535cbfca724700a52f210eaa396da79f44e0cb3135afdd2eecb3907f3':
    ['trending_hashtags', '24h'],
  'bluesky-thinking:posts_per_minute:230037def14c9a89b18603f313d982d6a3f7acd4af5147b2f6ae2c257b82ce57':
    ['posts_per_minute', '5m'],
};

describe('interop/v1 key derivation (locked contract)', () => {
  it.each(Object.entries(LOCKED_VECTORS))('derives %s', (expected, [operation, window]) => {
    expect(generateInteropKey(NAMESPACE, operation, [window])).toBe(expected);
  });

  it('same window hashes identically across all operations', () => {
    // The args hash covers only the canonical argument array, so the hash
    // segment is shared; the operation segment provides identity.
    for (const window of WINDOWS) {
      const hashes = new Set(
        OPERATIONS.map((op) => generateInteropKey(NAMESPACE, op, [window]).split(':')[2]),
      );
      expect(hashes.size).toBe(1);
    }
  });
});
