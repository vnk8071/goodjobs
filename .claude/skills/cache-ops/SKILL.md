---
name: cache-ops
description: Inspect, reason about, and manage the Redis job cache — status, keys, namespacing, warmup state, flushing. Use when debugging caching behavior, checking freshness, or managing cached scrape results.
---

# Cache Ops

## Quick inspection

```bash
# Human-readable overview (keys with fetched_ago and job_count)
curl -s localhost:8000/cache/status | python3 -m json.tool

# Raw Redis
docker compose exec redis redis-cli
> KEYS jobs:*              # VN keys are jobs:{kw}:{loc}, global are jobs:{country}:{kw}:{loc}
> TYPE jobs:python:hanoi   # always hashes
> HLEN jobs:python:hanoi   # number of cached jobs
> HGETALL jobs:python:hanoi
```

## Key semantics

- Format: `jobs:{kw}:{loc}` (VN default) vs `jobs:{country}:{kw}:{loc}` (global). Never collide.
- No key TTL for VN; individual hash fields (jobs) older than 8 days (`RECENT_DAYS`) pruned by `_cleanup_old_jobs()` in `src/cache.py`.
- Global keys ARE excluded from the VN cleanup job and from `/recent-jobs` / `/stats` — check `is_vn_cache_key()` in `src/cache.py` before assuming a key participates in any view.
- Writes go through merge helpers — dedup by `link`, never blindly overwrite.

## Warmup state

```bash
curl -s localhost:8000/warmup/keywords | python3 -m json.tool
```

Warmup runs every 2h (`SCRAPE_INTERVAL=7200`), check cycle 10 min; a scrape fires only when `now - fetched_ts >= 7200`. LinkedIn fetches incrementally (`f_TPR=r7200`), others refetch the whole 8-day window. Global keys are never warmed.

## Force actions

```bash
# Trigger a live scrape into cache for one keyword/location
curl -s "localhost:8000/cache/scrape?keyword=python&location=Ho+Chi+Minh+City"

# Add a permanent warmup pair
curl -s -X POST localhost:8000/warmup/keywords \
  -H 'Content-Type: application/json' \
  -d '{"keyword":"data engineer","location":"Da Nang"}'
```

## Danger zone

`FLUSHDB` wipes everything including vector indexes — after flushing, expect cold searches (full scrapes, slow first responses) until warmup repopulates. Prefer deleting specific `jobs:*` keys instead.
