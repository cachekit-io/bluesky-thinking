#!/usr/bin/env python3
"""Print a one-line status report; stdout is the output contract."""

import sys


def main() -> int:
    ok = len(sys.argv) > 1
    print("ok" if ok else "FAIL: no argument given", file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
