import json
from difflib import SequenceMatcher

from redis.asyncio import Redis

from .constants import REDIS_URL

_redis: Redis | None = None


def get_redis() -> Redis:
    """Return the shared Redis client, creating it on first call."""
    global _redis
    if _redis is None:
        _redis = Redis.from_url(REDIS_URL, decode_responses=True)
    return _redis


def _key(keyword: str, location: str) -> str:
    """Build the canonical Redis key for a keyword+location pair."""
    return f"jobs:{keyword.lower().strip()}:{location.lower().strip()}"


async def cache_get(keyword: str, location: str) -> tuple[list[dict], float] | None:
    """Return (jobs, fetched_ts) from cache, or None on miss."""
    try:
        raw = await get_redis().get(_key(keyword, location))
        if not raw:
            return None
        data = json.loads(raw)
        return data["jobs"], float(data["fetched_ts"])
    except Exception as e:
        print(f"[cache] get error: {e}")
        return None


async def cache_set(keyword: str, location: str, jobs: list[dict], fetched_ts: float) -> None:
    """Store jobs in cache permanently (no TTL)."""
    try:
        payload = json.dumps({"jobs": jobs, "fetched_ts": fetched_ts}, ensure_ascii=False)
        await get_redis().set(_key(keyword, location), payload)
        print(f"[cache] stored {len(jobs)} jobs for {keyword!r}")
    except Exception as e:
        print(f"[cache] set error: {e}")


async def cache_fuzzy_get(keyword: str, location: str, threshold: float = 0.6) -> tuple[list[dict], float] | None:
    """Find the closest cached keyword by similarity and return its jobs.

    Scans all keys matching jobs:*:<location> and picks the one whose keyword
    has the highest SequenceMatcher ratio against the query keyword, provided it
    meets threshold. Returns (jobs, fetched_ts) or None.
    """
    try:
        loc = location.lower().strip()
        pattern = f"jobs:*:{loc}"
        keys = [k async for k in get_redis().scan_iter(pattern)]
        if not keys:
            return None

        kw_lower = keyword.lower().strip()
        best_key: str | None = None
        best_score = 0.0

        for key in keys:
            cached_kw = key[len("jobs:") : -len(f":{loc}")] if loc else key[len("jobs:"):]
            score = SequenceMatcher(None, kw_lower, cached_kw).ratio()
            if score > best_score:
                best_score = score
                best_key = key

        if best_score < threshold or best_key is None:
            return None

        raw = await get_redis().get(best_key)
        if not raw:
            return None
        data = json.loads(raw)
        jobs = data["jobs"]
        if not jobs:
            return None
        print(f"[cache] fuzzy hit — {keyword!r} ~ {best_key!r} (score={best_score:.2f}, {len(jobs)} jobs)")
        return jobs, float(data["fetched_ts"])
    except Exception as e:
        print(f"[cache] fuzzy_get error: {e}")
        return None


async def cache_ttl(keyword: str, location: str) -> int:
    """Return remaining TTL in seconds for the cache key, or -2 if the key does not exist."""
    try:
        return await get_redis().ttl(_key(keyword, location))
    except Exception as e:
        print(f"[cache] ttl error: {e}")
        return -2


async def cache_merge(keyword: str, location: str, new_jobs: list[dict], fetched_ts: float) -> None:
    """Merge new_jobs into existing cache, deduplicate by link, update fetched_ts."""
    try:
        existing = await cache_get(keyword, location)
        all_jobs = (existing[0] if existing else []) + new_jobs
        seen: set[str] = set()
        deduped: list[dict] = []
        for j in all_jobs:
            if j["link"] not in seen:
                seen.add(j["link"])
                deduped.append(j)
        await cache_set(keyword, location, deduped, fetched_ts)
        print(f"[cache] merged {len(new_jobs)} new + {len(existing[0]) if existing else 0} cached = {len(deduped)} total")
    except Exception as e:
        print(f"[cache] merge error: {e}")
