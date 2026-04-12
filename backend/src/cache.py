import json
import time
from difflib import SequenceMatcher

from redis.asyncio import Redis

from .constants import REDIS_URL, RECENT_DAYS
from .logger import log_app
from .matching import strip_level

_redis: Redis | None = None


def get_redis() -> Redis:
    """Return the shared Redis client, creating it on first call."""
    global _redis
    if _redis is None:
        _redis = Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5.0,  # 5 second connection timeout
            socket_timeout=5.0,  # 5 second socket timeout
            retry_on_timeout=False,  # Don't retry on timeout - fail fast
            health_check_interval=30,  # Health check every 30 seconds
        )
    return _redis


def _key(keyword: str, location: str) -> str:
    """Build the canonical Redis key for a keyword+location pair."""
    return f"jobs:{keyword.lower().strip()}:{location.lower().strip()}"


def _access_key(keyword: str, location: str) -> str:
    """Build the last-access timestamp key for non-warmup keywords."""
    return f"jobs-access:{keyword.lower().strip()}:{location.lower().strip()}"


async def cache_get(keyword: str, location: str) -> tuple[list[dict], float] | None:
    """Return (jobs, fetched_ts) from cache, or None on miss."""
    try:
        raw = await get_redis().get(_key(keyword, location))
        if not raw:
            return None
        data = json.loads(raw)
        return data["jobs"], float(data["fetched_ts"])
    except Exception as e:
        log_app(f"cache get error: {e}", "ERROR")
        return None


async def cache_set(
    keyword: str, location: str, jobs: list[dict], fetched_ts: float, ttl_days: int = RECENT_DAYS
) -> None:
    """Store jobs in cache with a TTL (default RECENT_DAYS) to prevent unbounded Redis growth."""
    try:
        payload = json.dumps(
            {"jobs": jobs, "fetched_ts": fetched_ts}, ensure_ascii=False
        )
        await get_redis().set(_key(keyword, location), payload, ex=ttl_days * 86400)
        log_app(f"cache stored {len(jobs)} jobs for {keyword!r}")
    except Exception as e:
        log_app(f"cache set error: {e}", "ERROR")


async def cache_fuzzy_get(
    keyword: str, location: str, threshold: float = 0.85
) -> tuple[list[dict], float, str] | None:
    """Find the closest cached keyword by similarity and return its jobs.

    Scans all keys matching jobs:*:<location> and picks the one whose keyword
    has the highest SequenceMatcher ratio against the query keyword, provided it
    meets threshold. Returns (jobs, fetched_ts, matched_keyword) or None.
    """
    try:
        loc = location.lower().strip()
        pattern = f"jobs:*:{loc}"
        keys = [k async for k in get_redis().scan_iter(pattern)]
        if not keys:
            return None

        kw_lower = keyword.lower().strip()
        kw_core = strip_level(kw_lower)
        kw_core_words = set(kw_core.split())
        best_key: str | None = None
        best_score = 0.0

        for key in keys:
            cached_kw = (
                key[len("jobs:") : -len(f":{loc}")] if loc else key[len("jobs:") :]
            )
            cached_core = strip_level(cached_kw)
            cached_core_words = list(cached_core.split())

            # Each word in the shorter phrase must fuzzy-match a word in the longer phrase
            shorter_words = (
                kw_core_words
                if len(kw_core_words) <= len(cached_core_words)
                else set(cached_core_words)
            )
            longer_words = (
                cached_core_words
                if len(kw_core_words) <= len(cached_core_words)
                else list(kw_core_words)
            )
            if not all(
                any(SequenceMatcher(None, sw, lw).ratio() >= 0.8 for lw in longer_words)
                for sw in shorter_words
            ):
                continue

            score = SequenceMatcher(None, kw_core, cached_core).ratio()
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
        matched_kw = (
            best_key[len("jobs:") : -len(f":{loc}")]
            if loc
            else best_key[len("jobs:") :]
        )
        log_app(
            f"cache fuzzy hit — {keyword!r} ~ {best_key!r} (score={best_score:.2f}, {len(jobs)} jobs)"
        )
        return jobs, float(data["fetched_ts"]), matched_kw
    except Exception as e:
        log_app(f"cache fuzzy_get error: {e}", "ERROR")
        return None


async def cache_ttl(keyword: str, location: str) -> int:
    """Return remaining TTL in seconds for the cache key, or -2 if the key does not exist."""
    try:
        return await get_redis().ttl(_key(keyword, location))
    except Exception as e:
        log_app(f"cache ttl error: {e}", "ERROR")
        return -2


async def cache_preserve_posted_dates(
    keyword: str, location: str, new_jobs: list[dict]
) -> None:
    """Preserve posted_ts/posted_date/posted from existing cache for already-seen jobs.

    Some job boards (JobsGo, Glints, ViecOi, VietnamWorks, CareerViet) expose only an
    application deadline rather than a real posting date. Their scrapers derive
    posted_ts from relative text like "Hôm nay" → days_ago=0, so every re-scrape
    makes the job look freshly posted and it floats to the top.

    By locking in the original posted_ts from the first time a job was cached, the
    job's apparent age stays stable for up to RECENT_DAYS. After that the cache TTL
    expires and the next scrape naturally assigns a new date.
    """
    try:
        existing = await cache_get(keyword, location)
        if not existing:
            return
        existing_by_link: dict[str, dict] = {j["link"]: j for j in existing[0] if j.get("link")}
        for job in new_jobs:
            link = job.get("link")
            if not link or link not in existing_by_link:
                continue
            prev = existing_by_link[link]
            if prev.get("posted_ts") is not None:
                job["posted_ts"] = prev["posted_ts"]
            if prev.get("posted_date"):
                job["posted_date"] = prev["posted_date"]
            if prev.get("posted"):
                job["posted"] = prev["posted"]
    except Exception as e:
        log_app(f"cache_preserve_posted_dates error: {e}", "ERROR")


async def cache_merge(
    keyword: str, location: str, new_jobs: list[dict], fetched_ts: float
) -> None:
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
        log_app(
            f"cache merged {len(new_jobs)} new + {len(existing[0]) if existing else 0} cached = {len(deduped)} total"
        )
    except Exception as e:
        log_app(f"cache merge error: {e}", "ERROR")


async def cache_touch(keyword: str, location: str) -> None:
    """Record the current Unix timestamp as the last user-access time for this cache key.
    Only called on cache hits from user searches — never by warmup."""
    try:
        await get_redis().set(_access_key(keyword, location), str(time.time()))
    except Exception as e:
        log_app(f"cache touch error: {e}", "ERROR")


async def cache_access_ts(keyword: str, location: str) -> float:
    """Return the last user-access timestamp for this key, or 0.0 if never accessed."""
    try:
        raw = await get_redis().get(_access_key(keyword, location))
        return float(raw) if raw else 0.0
    except Exception as e:
        log_app(f"cache access_ts error: {e}", "ERROR")
        return 0.0


_EMBEDDED_LINKS_KEY = "vector:embedded_links"


async def embedded_links_add(links: list[str]) -> None:
    """Mark job links as having been embedded into Vectorize."""
    if not links:
        return
    try:
        await get_redis().sadd(_EMBEDDED_LINKS_KEY, *links)
    except Exception as e:
        log_app(f"embedded_links_add error: {e}", "ERROR")


async def embedded_links_filter(jobs: list[dict]) -> list[dict]:
    """Return only jobs whose links have NOT already been embedded into Vectorize."""
    if not jobs:
        return []
    try:
        pipe = get_redis().pipeline()
        for j in jobs:
            pipe.sismember(_EMBEDDED_LINKS_KEY, j.get("link", ""))
        results = await pipe.execute()
        return [j for j, seen in zip(jobs, results) if not seen]
    except Exception as e:
        log_app(f"embedded_links_filter error: {e}", "ERROR")
        return jobs


async def embedded_links_all() -> set[str]:
    """Return all job links that have been marked as embedded in Vectorize."""
    try:
        members = await get_redis().smembers(_EMBEDDED_LINKS_KEY)
        return {m.decode() if isinstance(m, bytes) else m for m in members}
    except Exception as e:
        log_app(f"embedded_links_all error: {e}", "ERROR")
        return set()


async def embedded_links_count() -> int:
    """Return the number of job links tracked as embedded."""
    try:
        return await get_redis().scard(_EMBEDDED_LINKS_KEY)
    except Exception as e:
        log_app(f"embedded_links_count error: {e}", "ERROR")
        return 0


_VECMARK_WARMUP_KEY = "vector:warmup_last_seen"
_VECMARK_NONWARMUP_KEY = "vector:nonwarmup_last_seen"


async def vector_mark_warmup_seen(links: list[str], ts: float) -> None:
    """Record warmup last-seen timestamps and remove from non-warmup tracking."""
    if not links:
        return
    try:
        redis = get_redis()
        pipe = redis.pipeline()
        for link in links:
            if link:
                pipe.zadd(_VECMARK_WARMUP_KEY, {link: ts})
                pipe.zrem(_VECMARK_NONWARMUP_KEY, link)
        await pipe.execute()
    except Exception as e:
        log_app(f"vector_mark_warmup_seen error: {e}", "ERROR")


async def vector_mark_nonwarmup_seen(links: list[str], ts: float) -> None:
    """Record non-warmup last-seen timestamps."""
    if not links:
        return
    try:
        redis = get_redis()
        pipe = redis.pipeline()
        for link in links:
            if link:
                pipe.zadd(_VECMARK_NONWARMUP_KEY, {link: ts})
        await pipe.execute()
    except Exception as e:
        log_app(f"vector_mark_nonwarmup_seen error: {e}", "ERROR")


async def vector_get_expired_nonwarmup(cutoff: float, limit: int = 500) -> list[str]:
    """Return non-warmup links whose last-seen score is <= cutoff."""
    try:
        return await get_redis().zrangebyscore(
            _VECMARK_NONWARMUP_KEY, "-inf", cutoff, start=0, num=limit
        )
    except Exception as e:
        log_app(f"vector_get_expired_nonwarmup error: {e}", "ERROR")
        return []


async def vector_get_warmup_scores(links: list[str]) -> dict[str, float | None]:
    """Return a dict of link -> warmup score (None if not present)."""
    result: dict[str, float | None] = {link: None for link in links}
    if not links:
        return result
    try:
        redis = get_redis()
        pipe = redis.pipeline()
        for link in links:
            if link:
                pipe.zscore(_VECMARK_WARMUP_KEY, link)
        scores = await pipe.execute()
        for link, score in zip(links, scores):
            if link:
                result[link] = score
    except Exception as e:
        log_app(f"vector_get_warmup_scores error: {e}", "ERROR")
    return result


async def vector_trim_warmup(cutoff: float) -> int:
    """Remove warmup entries older than cutoff. Returns number removed."""
    try:
        return await get_redis().zremrangebyscore(_VECMARK_WARMUP_KEY, "-inf", cutoff)
    except Exception as e:
        log_app(f"vector_trim_warmup error: {e}", "ERROR")
        return 0
