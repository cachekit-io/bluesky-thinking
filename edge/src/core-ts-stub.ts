/**
 * Build-time stub for `@cachekit-io/cachekit-core-ts` (wrangler [alias]).
 *
 * The real package is a NAPI native module (.node binaries) that can never
 * run on Cloudflare Workers — bundling it is what breaks `wrangler deploy`
 * on @cachekit-io/cachekit 0.1.3 (0.1.4 replaces it with a WASM core, but
 * that bump is blocked upstream: LAB-780). The SDK only imports `ByteStorage`
 * from it statically, and only the compression path of the Cache class ever
 * constructs one. The edge never uses that path: it reads raw bytes via
 * `backend.get()` and decodes interop/v1 (plain MessagePack, no ByteStorage
 * envelope by contract). So the honest stub is one that throws on use —
 * dead weight is aliased away, and any future code path that would silently
 * depend on the native module fails loudly instead.
 */
export class ByteStorage {
  constructor() {
    throw new Error(
      'ByteStorage is not available on Cloudflare Workers: ' +
        '@cachekit-io/cachekit-core-ts is a native NAPI module (stubbed at build time). ' +
        'The edge reads interop/v1 payloads, which never use the ByteStorage envelope.',
    );
  }
}
