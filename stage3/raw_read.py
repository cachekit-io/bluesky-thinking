"""Independent raw reader for Stage-3 evidence (LAB-737).

Fetches cache entries straight off the SaaS HTTP API — no SDK, no decorator,
no decryption — so what it prints is exactly what the backend stores. Used
for three proofs:

- AC-2: the namespace is clean before the first live run (`--expect absent`)
- AC-4: the payload bytes a reader fetches are what the ingester wrote
  (prints xxHash3-64 big-endian hex, the StorageEnvelope convention)
- AC-6: the @cache.secure value is ciphertext (`--expect ciphertext` asserts
  the bytes are NOT a valid MessagePack document and contain none of the
  `--forbid` plaintext markers)

Env: CACHEKIT_API_KEY (required), CACHEKIT_API_URL (default api.cachekit.io).

    op run --env-file=../.op.apikey.env -- \
        uv run --with httpx --with xxhash --with msgpack \
        python raw_read.py [--expect absent|present|ciphertext] \
                           [--forbid TEXT ...] [--hexdump N] KEY [KEY ...]

Exit code 0 only if every key satisfies --expect.
"""

from __future__ import annotations

import argparse
import os
import sys
from urllib.parse import quote

import httpx
import msgpack
import xxhash


def fetch(client: httpx.Client, api_url: str, key: str) -> tuple[int, bytes]:
    resp = client.get(f"{api_url}/v1/cache/{quote(key, safe='')}")
    return resp.status_code, resp.content


def is_msgpack_document(data: bytes) -> bool:
    """Strict single-document check (interop/v1 rule: no trailing bytes)."""
    try:
        unpacker = msgpack.Unpacker(strict_map_key=False)
        unpacker.feed(data)
        unpacker.unpack()
        return unpacker.tell() == len(data)
    except Exception:
        return False


def check_key(client: httpx.Client, api_url: str, key: str, args: argparse.Namespace) -> bool:
    status, body = fetch(client, api_url, key)
    if status == 404:
        print(f"ABSENT  {key}")
        return args.expect == "absent"
    if status != 200:
        print(f"ERROR   {key}: HTTP {status} {body[:200]!r}")
        return False

    digest = xxhash.xxh3_64(body).intdigest()
    plain_msgpack = is_msgpack_document(body)
    print(f"PRESENT {key}")
    print(f"        size={len(body)} xxh3_64={digest:016x} msgpack_document={plain_msgpack}")
    if args.hexdump:
        prefix = body[: args.hexdump]
        print(f"        hex[:{len(prefix)}]={prefix.hex()}")

    if args.expect == "absent":
        return False
    ok = True
    for text in args.forbid:
        if text.encode() in body:
            print(f"        FORBIDDEN plaintext marker {text!r} found in stored bytes")
            ok = False
    if args.expect == "ciphertext" and plain_msgpack:
        print("        FAIL: stored bytes decode as a plain MessagePack document")
        ok = False
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("keys", nargs="+")
    parser.add_argument("--expect", choices=["absent", "present", "ciphertext"], default="present")
    parser.add_argument("--forbid", action="append", default=[], help="plaintext that must NOT appear in stored bytes")
    parser.add_argument("--hexdump", type=int, default=0, metavar="N", help="print first N stored bytes as hex")
    parser.add_argument("--delete", action="store_true", help="DELETE the keys instead of reading them (AC-2 cleanup)")
    args = parser.parse_args()

    api_key = os.environ.get("CACHEKIT_API_KEY")
    if not api_key:
        sys.exit("CACHEKIT_API_KEY not set")
    api_url = os.environ.get("CACHEKIT_API_URL", "https://api.cachekit.io").rstrip("/")

    headers = {
        "Authorization": f"Bearer {api_key}",
        # SDK-class keys require an L1 status on every cache request.
        "X-CacheKit-L1-Status": "disabled",
    }
    failures = 0
    with httpx.Client(headers=headers, timeout=10.0) as client:
        if args.delete:
            for key in args.keys:
                resp = client.delete(f"{api_url}/v1/cache/{quote(key, safe='')}")
                print(f"DELETE {resp.status_code} {key}")
                if resp.status_code not in (200, 204, 404):
                    failures += 1
            print(f"{'PASS' if failures == 0 else 'FAIL'}: deleted {len(args.keys) - failures}/{len(args.keys)} keys")
            return 1 if failures else 0
        for key in args.keys:
            if not check_key(client, api_url, key, args):
                failures += 1
    verdict = "PASS" if failures == 0 else "FAIL"
    print(f"{verdict}: {len(args.keys) - failures}/{len(args.keys)} keys satisfied --expect {args.expect}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
