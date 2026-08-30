# LAB-1131 scratch: docs page reproduction

Reproduces the Python code fences Kody's base-image rule falsely flagged on
cachekit-py#250 (`docs/features/distributed-locking.md:63,207-208`). No
container manifest content anywhere in this file — the base-image pinning
rule must NOT fire here.

If a function is expensive enough that a stampede matters, decorate the async
variant:

```python
import asyncio
from cachekit import cache

@cache(ttl=300)
async def compute_report(report_id):
    return expensive_operation()  # Only one concurrent caller executes this

result = asyncio.run(compute_report("daily"))
assert result["computed"] is True
```

Backend construction, as flagged at lines 207-208 of the original page:

```python
from cachekit.backends import CachekitIOBackend

backend = CachekitIOBackend()
```
