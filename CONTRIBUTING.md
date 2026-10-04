# Contributing

User-facing docs in this repository follow CacheKit's shared rule on what belongs in them:
[What belongs in these docs](https://docs.cachekit.io/contributing/#what-belongs-in-these-docs).
`prek install` sets up hooks that reject internal references in README files, `docs/` and
commit messages. If you use Python `pre-commit`, use version 2.18.0 or later and run
`pre-commit install`; older versions skip the commit-message hook.
