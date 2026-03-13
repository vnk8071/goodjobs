import asyncio
import os
import random
import time
from datetime import datetime, timezone, timedelta

from src.cache import cache_get, cache_set, get_redis, _key, cache_access_ts
from src.constants import RECENT_DAYS
from src.logger import log_app
from src.matching import title_matches, extract_skills, posted_ts

from src.scrapers import scrape_linkedin_detail_one, scrape_topcv_detail_one, scrape_itviec_detail_one, scrape_vietnamworks_detail_one, scrape_careerviet_detail_one

_WARMUP_KEYWORDS_DEFAULT = [
    "AI Engineer",
    "Product Manager",
    "Business Analyst",
    "Backend Engineer",
    "Frontend Engineer",
    "Fullstack Engineer",
    "Data Engineer",
    "DevOps Engineer",
    "Mobile Developer",
    "QA Engineer",
    "Data Scientist",
    "UI/UX Designer",
    "Marketing Executive",
    "Cloud Engineer",
]

def _load_warmup_keywords() -> list[str]:
    raw = os.environ.get("WARMUP_KEYWORDS", "").strip()
    if raw:
        return [kw.strip() for kw in raw.split(",") if kw.strip()]
    return _WARMUP_KEYWORDS_DEFAULT

_WARMUP_KEYWORDS = _load_warmup_keywords()
_WARMUP_LOCATIONS = ["Ho Chi Minh City", "Ha Noi"]

_WARMUP_KEYWORDS_KEY = "warmup:keywords"


async def get_warmup_keywords() -> list[str]:
    """Return current warmup keywords from Redis.

    Always ensures the hardcoded _WARMUP_KEYWORDS defaults are present —
    so new keywords added to the list are picked up on the next server restart
    even if the Redis set already existed.
    """
    redis = get_redis()
    # sadd is a no-op for members that already exist, so this is safe to call always
    await redis.sadd(_WARMUP_KEYWORDS_KEY, *_WARMUP_KEYWORDS)
    members = await redis.smembers(_WARMUP_KEYWORDS_KEY)
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
_QUIET_START = 22
_QUIET_END   = 10


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


async def _scrape_keyword(kw: str, loc: str, loop, executor, scrapers: dict, last_fetched_ts: float = 0.0, blocked_sites: set | None = None, enrich_limit: int | None = None) -> None:
    """Scrape all sites for one keyword+location, enrich descriptions, and merge into Redis.

    When last_fetched_ts > 0, LinkedIn uses f_TPR for incremental fetching.
    When 0, performs a full backfill. Sites are scraped sequentially with a
    per-site inter-request delay to avoid IP blocks.
    """
    from src.scrapers.linkedin import scrape_linkedin

    since_seconds = 86400 if last_fetched_ts > 0 else None

    # Minimum delay between consecutive site requests.
    # A random jitter of 0–50% is added on top to avoid predictable intervals.
    _SITE_DELAY: dict[str, float] = {
        "linkedin":     3.0,
        "itviec":       3.0,
        "topcv":        3.0,
        "vietnamworks": 3.0,
        "careerviet":   3.0,
    }

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
    itviec_jobs: list[dict] = []
    vietnamworks_jobs: list[dict] = []
    careerviet_jobs: list[dict] = []
    seen_links: set[str] = set()
    site_succeeded: set[str] = set()
    site_timeouts: set[str] = set()

    for i, (site, fn) in enumerate(scrapers.items()):
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(executor, _timed, site, fn, kw, loc),
                timeout=45.0,
            )
            site_succeeded.add(site)
        except asyncio.TimeoutError:
            log_app(f"[warmup][{kw}][{loc}][{site}] scrape timeout")
            if site not in site_succeeded:
                site_timeouts.add(site)
            result = []
        except Exception as e:
            log_app(f"[warmup][{kw}][{loc}][{site}] error: {e}")
            result = []
        for j in result:
            if title_matches(j.get("title", ""), kw) and j.get("link") not in seen_links:
                seen_links.add(j["link"])
                j["posted_ts"] = posted_ts(j)
                jobs.append(j)
                if j.get("source") == "LinkedIn":
                    linkedin_jobs.append(j)
                elif j.get("source") == "TopCV":
                    topcv_jobs.append(j)
                elif j.get("source") == "ITViec":
                    itviec_jobs.append(j)
                elif j.get("source") == "VietnamWorks":
                    vietnamworks_jobs.append(j)
                elif j.get("source") == "CareerViet":
                    careerviet_jobs.append(j)
        # Inter-site delay with jitter — only between sites, not after the last one.
        if i < len(scrapers) - 1:
            base = _SITE_DELAY.get(site, 4.0)
            jitter = random.uniform(0, base * 0.5)
            await asyncio.sleep(base + jitter)

    all_sites = set(scrapers.keys())
    if not site_succeeded and site_timeouts >= all_sites:
        log_app(f"[warmup] {kw!r}/{loc!r} all sources timed out — skipping cache update to allow retry")
        return

    cutoff_ts = time.time() - RECENT_DAYS * 86400
    existing = await cache_get(kw, loc)
    existing_jobs = existing[0] if existing else []

    cached_desc_by_link = {j["link"]: j["description"] for j in existing_jobs if j.get("description")}
    cached_with_desc = set(cached_desc_by_link)

    new_links = {j["link"] for j in jobs}
    kept_cached = [
        j for j in existing_jobs
        if j["link"] not in new_links and j.get("posted_ts", 0.0) > cutoff_ts
    ]
    new_jobs_recent = [j for j in jobs if j.get("posted_ts", 0.0) > cutoff_ts]
    for j in new_jobs_recent:
        if not j.get("description") and j["link"] in cached_desc_by_link:
            j["description"] = cached_desc_by_link[j["link"]]
    for j in new_jobs_recent:
        j["skills"] = extract_skills(j.get("title", ""), j.get("description", ""))

    merged = kept_cached + new_jobs_recent
    merged.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
    await cache_set(kw, loc, merged, time.time())
    log_app(f"[warmup] {kw!r}/{loc!r} cached — {len(new_jobs_recent)} new + {len(kept_cached)} kept = {len(merged)} total (pre-enrich)")

    linkedin_batch = [j for j in linkedin_jobs if j.get("link") not in cached_with_desc]
    linkedin_batch.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
    linkedin_batch = linkedin_batch[:enrich_limit if enrich_limit is not None else 30]
    if linkedin_batch:
        log_app(f"[warmup][{kw}][{loc}] enriching {len(linkedin_batch)} LinkedIn jobs without description...")
        await asyncio.sleep(1.0)
        enriched_linkedin = 0
        for i, job in enumerate(linkedin_batch):
            cooldown = 1.0 if i > 0 else 0.0
            try:
                ok = await asyncio.wait_for(
                    loop.run_in_executor(executor, scrape_linkedin_detail_one, job, cooldown),
                    timeout=60.0,
                )
                if not ok:
                    break
                enriched_linkedin += 1
            except asyncio.TimeoutError:
                log_app(f"[warmup][{kw}][{loc}] linkedin detail timeout: {job.get('link')}")
            except Exception as e:
                log_app(f"[warmup][{kw}][{loc}] linkedin detail error: {e}")
        log_app(f"[warmup][{kw}][{loc}] LinkedIn enrich done — {enriched_linkedin}/{len(linkedin_batch)} jobs enriched")

    topcv_batch = [j for j in topcv_jobs if j.get("link") not in cached_with_desc]
    topcv_batch.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
    topcv_batch = topcv_batch[:enrich_limit if enrich_limit is not None else 10]
    if topcv_batch:
        log_app(f"[warmup][{kw}][{loc}] enriching {len(topcv_batch)} TopCV jobs without description...")
        enriched_topcv = 0
        for i, job in enumerate(topcv_batch):
            cooldown = 1.0 if i > 0 else 0.0
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(executor, scrape_topcv_detail_one, job, cooldown),
                    timeout=60.0,
                )
                enriched_topcv += 1
            except asyncio.TimeoutError:
                log_app(f"[warmup][{kw}][{loc}] topcv detail timeout: {job.get('link')}")
            except Exception as e:
                log_app(f"[warmup][{kw}][{loc}] topcv detail error: {e}")
        log_app(f"[warmup][{kw}][{loc}] TopCV enrich done — {enriched_topcv}/{len(topcv_batch)} jobs enriched")

    itviec_batch = [j for j in itviec_jobs if j.get("link") not in cached_with_desc]
    itviec_batch.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
    itviec_batch = itviec_batch[:enrich_limit if enrich_limit is not None else 10]
    if itviec_batch:
        log_app(f"[warmup][{kw}][{loc}] enriching {len(itviec_batch)} ITViec jobs without description...")
        enriched_itviec = 0
        for i, job in enumerate(itviec_batch):
            cooldown = 1.0 if i > 0 else 0.0
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(executor, scrape_itviec_detail_one, job, cooldown),
                    timeout=60.0,
                )
                enriched_itviec += 1
            except asyncio.TimeoutError:
                log_app(f"[warmup][{kw}][{loc}] itviec detail timeout: {job.get('link')}")
            except Exception as e:
                log_app(f"[warmup][{kw}][{loc}] itviec detail error: {e}")
        log_app(f"[warmup][{kw}][{loc}] ITViec enrich done — {enriched_itviec}/{len(itviec_batch)} jobs enriched")

    vw_to_enrich = [j for j in vietnamworks_jobs if not j.get("description") and j.get("link") not in cached_with_desc]
    if vw_to_enrich:
        vw_to_enrich.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
        batch = vw_to_enrich[:enrich_limit if enrich_limit is not None else 10]
        log_app(f"[warmup][{kw}][{loc}] enriching {len(batch)} VietnamWorks jobs...")
        enriched_vw = 0
        for i, job in enumerate(batch):
            cooldown = 1.0 if i > 0 else 0.0
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(executor, scrape_vietnamworks_detail_one, job, cooldown),
                    timeout=60.0,
                )
                enriched_vw += 1
            except asyncio.TimeoutError:
                log_app(f"[warmup][{kw}][{loc}] vietnamworks detail timeout: {job.get('link')}")
            except Exception as e:
                log_app(f"[warmup][{kw}][{loc}] vietnamworks detail error: {e}")
        log_app(f"[warmup][{kw}][{loc}] VietnamWorks enrich done — {enriched_vw}/{len(batch)} jobs enriched")

    cv_to_enrich = [j for j in careerviet_jobs if not j.get("description") and j.get("link") not in cached_with_desc]
    if cv_to_enrich:
        cv_to_enrich.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
        batch = cv_to_enrich[:enrich_limit if enrich_limit is not None else 10]
        log_app(f"[warmup][{kw}][{loc}] enriching {len(batch)} CareerViet jobs...")
        enriched_cv = 0
        for i, job in enumerate(batch):
            cooldown = 1.0 if i > 0 else 0.0
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(executor, scrape_careerviet_detail_one, job, cooldown),
                    timeout=60.0,
                )
                enriched_cv += 1
            except asyncio.TimeoutError:
                log_app(f"[warmup][{kw}][{loc}] careerviet detail timeout: {job.get('link')}")
            except Exception as e:
                log_app(f"[warmup][{kw}][{loc}] careerviet detail error: {e}")
        log_app(f"[warmup][{kw}][{loc}] CareerViet enrich done — {enriched_cv}/{len(batch)} jobs enriched")

    if linkedin_batch or topcv_batch or itviec_batch or vw_to_enrich or cv_to_enrich:
        for j in new_jobs_recent:
            j["skills"] = extract_skills(j.get("title", ""), j.get("description", ""))
        merged.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
        await cache_set(kw, loc, merged, time.time())

    log_app(f"[warmup] {kw!r}/{loc!r} done — {len(new_jobs_recent)} new + {len(kept_cached)} kept = {len(merged)} total ({time.perf_counter()-t0:.1f}s)")


async def _cleanup_stale_keys() -> None:
    """Remove keywords from warmup:keywords set that are no longer in _WARMUP_KEYWORDS.

    Does NOT delete non-warmup job cache keys — those are handled by _cleanup_nonwarmup_stale_keys().
    Uses _WARMUP_KEYWORDS (hardcoded) as the source of truth, not the Redis set,
    so removed keywords are pruned even if they still exist in the set.
    """
    try:
        redis = get_redis()
        canonical = {kw.lower().strip() for kw in _WARMUP_KEYWORDS}

        members = await redis.smembers(_WARMUP_KEYWORDS_KEY)
        for member in members:
            if member.lower().strip() not in canonical:
                await redis.srem(_WARMUP_KEYWORDS_KEY, member)
                log_app(f"[warmup] removed stale keyword from set: {member!r}")

        log_app(f"[warmup] stale keyword set cleanup done")
    except Exception as e:
        log_app(f"[warmup] cleanup error: {e}")


async def _cleanup_nonwarmup_stale_keys() -> None:
    """Delete non-warmup job cache keys (and their access keys) that have not been
    accessed by a user in more than RECENT_DAYS days.

    Warmup keys are never touched by this function.
    """
    try:
        redis = get_redis()
        canonical_lower = {kw.lower().strip() for kw in _WARMUP_KEYWORDS}
        cutoff = time.time() - RECENT_DAYS * 86400
        deleted = 0

        async for key in redis.scan_iter("jobs:*"):
            # Extract keyword part from key: "jobs:{kw}:{loc}"
            # Key format guarantees at least one colon after "jobs:"
            remainder = key[len("jobs:"):]
            # Find the last colon — location part never contains a colon
            last_colon = remainder.rfind(":")
            if last_colon == -1:
                continue
            kw_part = remainder[:last_colon]
            loc_part = remainder[last_colon + 1:]

            if kw_part in canonical_lower:
                continue  # warmup key — never delete

            # Non-warmup key: check last-access timestamp
            access_ts = await cache_access_ts(kw_part, loc_part)
            if access_ts == 0.0 or access_ts < cutoff:
                await redis.delete(key)
                access_key = f"jobs-access:{kw_part}:{loc_part}"
                await redis.delete(access_key)
                log_app(f"[warmup] deleted stale non-warmup key: {key!r}")
                deleted += 1

        log_app(f"[warmup] non-warmup cleanup done — {deleted} key(s) removed")
    except Exception as e:
        log_app(f"[warmup] non-warmup cleanup error: {e}")


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


async def warmup(executor, scrapers: dict) -> None:
    """Background loop that keeps all warmup keys fresh.

    Every CYCLE_INTERVAL seconds, scrapes any keyword×location whose fetched_ts
    is stale. Sleeps during quiet hours (20–8 ICT). Keys are stored permanently.
    """
    loop = asyncio.get_event_loop()

    CYCLE_INTERVAL = 3600
    SCRAPE_INTERVAL = 25200

    await asyncio.sleep(5.0)
    await _cleanup_stale_keys()

    log_app(f"[warmup] startup pass — checking for missing or stale keys...")
    warmup_kws = await get_warmup_keywords()
    now = time.time()
    startup_tasks = []
    for kw in warmup_kws:
        for loc in _WARMUP_LOCATIONS:
            existing = await cache_get(kw, loc)
            if existing is None or now - existing[1] >= SCRAPE_INTERVAL:
                startup_tasks.append((kw, loc, existing[1] if existing else 0.0))

    if startup_tasks:
        log_app(f"[warmup] startup pass: scraping {len(startup_tasks)} missing/stale entries...")

        async def _startup_scrape(kw: str, loc: str, fetched_ts: float) -> None:
            sleep_secs = _seconds_until_active()
            if sleep_secs > 0:
                wake = datetime.now(_TZ_ICT) + timedelta(seconds=sleep_secs)
                log_app(f"[warmup] startup: quiet hours — waiting until {wake.strftime('%H:%M')} ICT")
                while True:
                    remaining = _seconds_until_active()
                    if remaining <= 0:
                        break
                    await asyncio.sleep(min(CYCLE_INTERVAL, remaining))
                log_app(f"[warmup] startup: quiet hours over — resuming")
            try:
                await _scrape_keyword(kw, loc, loop, executor, scrapers, last_fetched_ts=fetched_ts)
            except Exception as e:
                log_app(f"[warmup] startup error for {kw!r}/{loc!r}: {e}")

        for kw, loc, ft in startup_tasks:
            await _startup_scrape(kw, loc, ft)
        log_app(f"[warmup] startup pass done")
    else:
        log_app(f"[warmup] startup pass: all keys present, skipping")

    _last_cleanup_ts = 0.0
    _CLEANUP_INTERVAL = 86400

    while True:
        try:
            sleep_secs = _seconds_until_active()
            if sleep_secs > 0:
                wake = datetime.now(_TZ_ICT) + timedelta(seconds=sleep_secs)
                log_app(f"[warmup] quiet hours — sleeping until {wake.strftime('%H:%M')} ICT ({sleep_secs/3600:.1f}h)")
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
                        await _scrape_keyword(kw, loc, loop, executor, scrapers, last_fetched_ts=fetched_ts)
                    except Exception as e:
                        log_app(f"[warmup] error for {kw!r}/{loc!r}: {e}")

                for i, (kw, loc, ft) in enumerate(tasks):
                    await _scrape_one(kw, loc, ft)
                    # Between-keyword cooldown: 15–30 s so back-to-back keyword
                    # cycles don't look like a bot burst to LinkedIn/ITViec.
                    if i < len(tasks) - 1:
                        await asyncio.sleep(random.uniform(15, 30))
            else:
                log_app(f"[warmup] cycle: all keys fresh, nothing to scrape")

            if time.time() - _last_cleanup_ts >= _CLEANUP_INTERVAL:
                await _cleanup_old_jobs()
                await _cleanup_nonwarmup_stale_keys()
                _last_cleanup_ts = time.time()

            await asyncio.sleep(CYCLE_INTERVAL)

        except Exception as e:
            log_app(f"[warmup] cycle crashed: {e}")
