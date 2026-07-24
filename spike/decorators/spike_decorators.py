"""LAB-735 AC-3 spike: prove @cache.production / @cache.io / @cache.secure run on cachekit==0.15.0."""
import os
from cachekit import cache

# --- @cache.production ---
@cache.production
def add(a: int, b: int) -> int:
    return a + b

assert add(2, 3) == 5 and add(2, 3) == 5  # second call = cache hit path
print("PASS @cache.production: add(2,3) ->", add(2, 3))

# --- @cache.secure (zero-knowledge; needs a 64-hex master key) ---
MASTER_KEY = os.urandom(32).hex()

@cache.secure(master_key=MASTER_KEY)
def sensitive(x: int) -> dict:
    return {"secret": x * 2}

assert sensitive(21) == {"secret": 42} and sensitive(21) == {"secret": 42}
print("PASS @cache.secure: sensitive(21) ->", sensitive(21))

# --- @cache.io (CachekitIO SaaS backend) ---
try:
    @cache.io()
    def io_fn(x: int) -> int:
        return x + 1
    print("io decorated OK; calling...")
    print("PASS @cache.io: io_fn(1) ->", io_fn(1))
except Exception as e:
    print(f"@cache.io raised {type(e).__name__}: {e}")
