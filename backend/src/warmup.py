import asyncio
import time
from datetime import datetime, timezone, timedelta

from src.cache import cache_get, cache_set, get_redis, _key
from src.constants import RECENT_DAYS
from src.logger import log_app
from src.matching import title_matches, extract_skills, posted_ts
from src.ratelimit import _KEYWORD_ALIAS_VARIANTS
from src.scrapers import scrape_linkedin_detail_one, scrape_topcv_detail_one

_WARMUP_KEYWORDS = [
    "AI Engineer",
    "Product Manager",
    "Business Analyst",
    "Backend Engineer",
    "Frontend Engineer",
    "Fullstack Engineer",
    "Data Engineer",
    "DevOps Engineer",
    "Flutter Developer",
    "QA Engineer",
]
_WARMUP_LOCATIONS = ["Ho Chi Minh City", "Ha Noi", "Da Nang"]

_WARMUP_KEYWORDS_KEY = "warmup:keywords"


async def get_warmup_keywords() -> list[str]:
    """Return current warmup keywords from Redis, seeding defaults on first call."""
    redis = get_redis()
    members = await redis.smembers(_WARMUP_KEYWORDS_KEY)
    if not members:
        await redis.sadd(_WARMUP_KEYWORDS_KEY, *_WARMUP_KEYWORDS)
        return list(_WARMUP_KEYWORDS)
    return sorted(members)


async def add_warmup_keyword(keyword: str) -> bool:
    """Add a keyword to the warmup set. Returns True if it was new."""
    redis = get_redis()
    added = await redis.sadd(_WARMUP_KEYWORDS_KEY, keyword)
    return bool(added)


async def remove_warmup_keyword(keyword: str) -> bool:
    """Remove a keyword from the warmup set. Returns True if it existed."""
    redis = get_redis()
    removed = await redis.srem(_WARMUP_KEYWORDS_KEY, keyword)
    return bool(removed)

_TZ_ICT = timezone(timedelta(hours=7))
_QUIET_START = 20
_QUIET_END   = 8


def _seconds_until_active() -> float:
    """Return seconds to sleep until the quiet period ends (0 if currently active hours)."""
    now = datetime.now(_TZ_ICT)
    hour = now.hour
    if _QUIET_START <= hour or hour < _QUIET_END:
        wake = now.replace(hour=_QUIET_END, minute=0, second=0, microsecond=0)
        if hour >= _QUIET_START:
            wake = wake + timedelta(days=1)
        return (wake - now).total_seconds()
    return 0.0


async def _scrape_keyword(kw: str, loc: str, loop, executor, scrapers: dict, last_fetched_ts: float = 0.0) -> None:
    """Scrape all sites for one keyword+location, enrich descriptions, and merge into Redis.

    When last_fetched_ts > 0, LinkedIn uses f_TPR for incremental fetching.
    When 0, performs a full backfill. Sites are scraped sequentially to keep
    memory usage low on single-CPU servers.
    """
    from src.scrapers.linkedin import scrape_linkedin

    since_seconds = 43200 if last_fetched_ts > 0 else None

    def _timed(site: str, fn, kw: str, loc: str):
        t0 = time.perf_counter()
        if site == "LinkedIn":
            result = scrape_linkedin(kw, loc, since_seconds=since_seconds)
        else:
            result = fn(kw, loc)
        log_app(f"[warmup][{kw}][{loc}][{site}] {len(result)} jobs in {time.perf_counter()-t0:.1f}s")
        return result

    t0 = time.perf_counter()
    jobs: list[dict] = []
    linkedin_jobs: list[dict] = []
    topcv_jobs: list[dict] = []
    seen_links: set[str] = set()

    for scrape_kw in [kw] + _KEYWORD_ALIAS_VARIANTS.get(kw, []):
        for site, fn in scrapers.items():
            try:
                result = await loop.run_in_executor(executor, _timed, site, fn, scrape_kw, loc)
            except Exception as e:
                log_app(f"[warmup][{scrape_kw}][{loc}][{site}] error: {e}")
                result = []
            for j in result:
                if title_matches(j.get("title", ""), scrape_kw) and j.get("link") not in seen_links:
                    seen_links.add(j["link"])
                    j["posted_ts"] = posted_ts(j)
                    jobs.append(j)
                    if j.get("source") == "LinkedIn":
                        linkedin_jobs.append(j)
                    elif j.get("source") == "TopCV":
                        topcv_jobs.append(j)

    if linkedin_jobs:
        log_app(f"[warmup][{kw}][{loc}] enriching {len(linkedin_jobs)} LinkedIn jobs...")
        await asyncio.sleep(10.0)
        linkedin_jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
        for i, job in enumerate(linkedin_jobs[:30]):
            cooldown = 3.0 if i > 0 else 0.0
            try:
                await loop.run_in_executor(executor, scrape_linkedin_detail_one, job, cooldown)
            except Exception as e:
                log_app(f"[warmup][{kw}][{loc}] linkedin detail error: {e}")

    if topcv_jobs:
        log_app(f"[warmup][{kw}][{loc}] enriching {len(topcv_jobs)} TopCV jobs...")
        topcv_jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
        for i, job in enumerate(topcv_jobs):
            cooldown = 2.0 if i > 0 else 0.0
            try:
                await loop.run_in_executor(executor, scrape_topcv_detail_one, job, cooldown)
            except Exception as e:
                log_app(f"[warmup][{kw}][{loc}] topcv detail error: {e}")

    for j in jobs:
        j["skills"] = extract_skills(j.get("title", ""), j.get("description", ""))

    cutoff_ts = time.time() - RECENT_DAYS * 86400
    existing = await cache_get(kw, loc)
    existing_jobs = existing[0] if existing else []
    cached_fetched_ts = existing[1] if existing else 0.0

    new_links = {j["link"] for j in jobs}
    kept_cached = [
        j for j in existing_jobs
        if j["link"] not in new_links and j.get("posted_ts", 0.0) > cutoff_ts
    ]
    new_jobs_recent = [
        j for j in jobs
        if j.get("posted_ts", 0.0) > cutoff_ts
    ]

    merged = kept_cached + new_jobs_recent
    merged.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)

    await cache_set(kw, loc, merged, time.time())
    log_app(f"[warmup] {kw!r}/{loc!r} done — {len(new_jobs_recent)} new + {len(kept_cached)} kept = {len(merged)} total ({time.perf_counter()-t0:.1f}s)")


async def _cleanup_stale_keys() -> None:
    """Delete Redis keys for warmup locations that do not match any canonical warmup keyword.

    Removes leftover keys from renamed keywords or typo searches.
    """
    try:
        redis = get_redis()
        canonical = {kw.lower().strip() for kw in await get_warmup_keywords()}
        deleted = 0
        for loc in _WARMUP_LOCATIONS:
            loc_key = loc.lower().strip()
            pattern = f"jobs:*:{loc_key}"
            async for key in redis.scan_iter(pattern):
                kw_part = key[len("jobs:"):-len(f":{loc_key}")]
                if kw_part not in canonical:
                    await redis.delete(key)
                    log_app(f"[warmup] deleted stale key: {key!r}")
                    deleted += 1
        log_app(f"[warmup] cleanup done — {deleted} stale key(s) removed")
    except Exception as e:
        log_app(f"[warmup] cleanup error: {e}")


async def _cleanup_old_jobs() -> None:
    """Drop jobs older than RECENT_DAYS from every warmup key. Runs once daily."""
    cutoff_ts = time.time() - RECENT_DAYS * 86400
    cleaned = 0
    for kw in await get_warmup_keywords():
        for loc in _WARMUP_LOCATIONS:
            try:
                existing = await cache_get(kw, loc)
                if not existing:
                    continue
                jobs, fetched_ts = existing
                fresh = [j for j in jobs if j.get("posted_ts", 0.0) > cutoff_ts]
                if len(fresh) < len(jobs):
                    await cache_set(kw, loc, fresh, fetched_ts)
                    log_app(f"[warmup] cleanup: dropped {len(jobs)-len(fresh)} old jobs from {_key(kw, loc)!r}")
                    cleaned += len(jobs) - len(fresh)
            except Exception as e:
                log_app(f"[warmup] cleanup error for {kw!r}/{loc!r}: {e}")
    log_app(f"[warmup] daily cleanup done — {cleaned} old job(s) removed")


async def warmup(get_sem, executor, scrapers: dict) -> None:
    """Background loop that keeps all warmup keys fresh.

    Every CYCLE_INTERVAL seconds, scrapes any keyword×location whose fetched_ts
    is stale. Sleeps during quiet hours (20–8 ICT). Keys are stored permanently.
    """
    loop = asyncio.get_event_loop()

    CYCLE_INTERVAL = 600
    SCRAPE_INTERVAL = 7200

    await asyncio.sleep(5.0)
    await _cleanup_stale_keys()

    log_app(f"[warmup] startup pass — checking for missing keys...")
    warmup_kws = await get_warmup_keywords()
    startup_tasks = [
        (kw, loc)
        for kw in warmup_kws
        for loc in _WARMUP_LOCATIONS
        if await cache_get(kw, loc) is None
    ]

    if startup_tasks:
        log_app(f"[warmup] startup pass: scraping {len(startup_tasks)} missing entries...")

        async def _startup_scrape(kw: str, loc: str) -> None:
            try:
                async with get_sem():
                    await _scrape_keyword(kw, loc, loop, executor, scrapers)
            except Exception as e:
                log_app(f"[warmup] startup error for {kw!r}/{loc!r}: {e}")

        await asyncio.gather(*[_startup_scrape(kw, loc) for kw, loc in startup_tasks])
        log_app(f"[warmup] startup pass done")
    else:
        log_app(f"[warmup] startup pass: all keys present, skipping")

    _last_cleanup_ts = 0.0
    _CLEANUP_INTERVAL = 86400

    while True:
        sleep_secs = _seconds_until_active()
        if sleep_secs > 0:
            wake = datetime.now(_TZ_ICT) + timedelta(seconds=sleep_secs)
            log_app(f"[warmup] quiet hours ��� sleeping until {wake.strftime('%H:%M')} ICT ({sleep_secs/3600:.1f}h)")
            while True:
                remaining = _seconds_until_active()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(CYCLE_INTERVAL, remaining))
            log_app(f"[warmup] quiet hours over — resuming")

        now = time.time()
        tasks = []
        for kw in await get_warmup_keywords():
            for loc in _WARMUP_LOCATIONS:
                existing = await cache_get(kw, loc)
                if existing is None:
                    log_app(f"[warmup] URGENT: {_key(kw, loc)!r} missing — scraping immediately")
                    tasks.append((kw, loc, 0.0))
                else:
                    _, fetched_ts = existing
                    if now - fetched_ts >= SCRAPE_INTERVAL:
                        tasks.append((kw, loc, fetched_ts))

        if tasks:
            missing_count = sum(1 for _, _, ft in tasks if ft == 0.0)
            log_app(f"[warmup] cycle: {len(tasks)} keys need refresh ({missing_count} missing)")

            async def _scrape_one(kw: str, loc: str, fetched_ts: float) -> None:
                age = "missing" if fetched_ts == 0.0 else f"age={int(now - fetched_ts)}s"
                try:
                    log_app(f"[warmup] scraping {kw!r}/{loc!r} ({age})...")
                    async with get_sem():
                        await _scrape_keyword(kw, loc, loop, executor, scrapers, last_fetched_ts=fetched_ts)
                except Exception as e:
                    log_app(f"[warmup] error for {kw!r}/{loc!r}: {e}")

            await asyncio.gather(*[_scrape_one(kw, loc, ft) for kw, loc, ft in tasks])
        else:
            log_app(f"[warmup] cycle: all keys fresh, nothing to scrape")

        if time.time() - _last_cleanup_ts >= _CLEANUP_INTERVAL:
            await _cleanup_old_jobs()
            _last_cleanup_ts = time.time()

        await asyncio.sleep(CYCLE_INTERVAL)
