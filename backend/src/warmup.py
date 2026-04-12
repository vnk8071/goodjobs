import asyncio
import hashlib
import os
import random
import time
from datetime import datetime, timezone, timedelta

from src.cache import (
    cache_get,
    cache_set,
    get_redis,
    _key,
    cache_access_ts,
    vector_mark_warmup_seen,
    vector_get_expired_nonwarmup,
    vector_get_warmup_scores,
    vector_trim_warmup,
    embedded_links_add,
    embedded_links_filter,
)
from src.vector import upsert_jobs, delete_by_ids
from src.constants import RECENT_DAYS, VECTOR_RETENTION_DAYS
from src.logger import log_app
from src.matching import title_matches, extract_skills, posted_ts

from src.scrapers import (
    scrape_linkedin_detail_one,
    scrape_topcv_detail_one,
    scrape_itviec_detail_one,
    scrape_vietnamworks_detail_one,
    scrape_careerviet_detail_one,
    scrape_jobsgo_detail_one,
    scrape_careerlink_detail_one,
    scrape_glints_detail_one,
    scrape_viecoi_detail_one,
)
from src.background_summarizer import run_background_summarization

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
    try:
        redis = get_redis()
        # sadd is a no-op for members that already exist, so this is safe to call always
        await redis.sadd(_WARMUP_KEYWORDS_KEY, *_WARMUP_KEYWORDS)
        members = await redis.smembers(_WARMUP_KEYWORDS_KEY)
        return sorted(members)
    except Exception:
        # Fallback to hardcoded keywords if Redis is unavailable
        return sorted(_WARMUP_KEYWORDS)


async def add_warmup_keyword(keyword: str) -> bool:
    """Add a keyword to the warmup set. Returns True if it was new."""
    try:
        redis = get_redis()
        added = await redis.sadd(_WARMUP_KEYWORDS_KEY, keyword)
        return bool(added)
    except Exception:
        # If Redis is unavailable, we can't add the keyword
        return False


async def remove_warmup_keyword(keyword: str) -> bool:
    """Remove a keyword from the warmup set. Returns True if it existed."""
    try:
        redis = get_redis()
        removed = await redis.srem(_WARMUP_KEYWORDS_KEY, keyword)
        return bool(removed)
    except Exception:
        # If Redis is unavailable, we can't remove the keyword
        return False


_TZ_ICT = timezone(timedelta(hours=7))
_SCRAPE_HOURS = (10, 17)  # full scrape: 10:00 and 17:00 ICT
_ENRICH_HOURS = (3, 14)  # description enrich + summarize + embed: 03:00 and 14:00 ICT

_ALL_SCHEDULED_HOURS = sorted(set(_SCRAPE_HOURS) | set(_ENRICH_HOURS))


def _seconds_until_next_scheduled() -> tuple[float, int]:
    """Return (seconds_to_wait, hour) for the next scheduled run across all events."""
    now = datetime.now(_TZ_ICT)
    candidates = []
    for hour in _ALL_SCHEDULED_HOURS:
        t = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if t <= now:
            t += timedelta(days=1)
        candidates.append((t, hour))
    next_t, next_hour = min(candidates, key=lambda x: x[0])
    return (next_t - now).total_seconds(), next_hour


async def _scrape_keyword(
    kw: str,
    loc: str,
    loop,
    executor,
    scrapers: dict,
    last_fetched_ts: float = 0.0,
    blocked_sites: set | None = None,
    enrich_limit: int | None = None,
) -> None:
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
        "linkedin": 3.0,
        "itviec": 3.0,
        "topcv": 3.0,
        "vietnamworks": 3.0,
        "careerviet": 3.0,
        "jobsgo": 3.0,
        "careerlink": 3.0,
        "glints": 3.0,
        "viecoi": 3.0,
    }

    def _timed(site: str, fn, kw: str, loc: str):
        t0 = time.perf_counter()
        if site == "LinkedIn":
            result = scrape_linkedin(kw, loc, since_seconds=since_seconds)
        else:
            result = fn(kw, loc)
        log_app(
            f"[warmup][{kw}][{loc}][{site}] {len(result)} jobs in {time.perf_counter() - t0:.1f}s"
        )
        return result

    t0 = time.perf_counter()
    jobs: list[dict] = []
    linkedin_jobs: list[dict] = []
    topcv_jobs: list[dict] = []
    itviec_jobs: list[dict] = []
    vietnamworks_jobs: list[dict] = []
    careerviet_jobs: list[dict] = []
    jobsgo_jobs: list[dict] = []
    careerlink_jobs: list[dict] = []
    glints_jobs: list[dict] = []
    viecoi_jobs: list[dict] = []
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
            if (
                title_matches(j.get("title", ""), kw)
                and j.get("link") not in seen_links
            ):
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
                elif j.get("source") == "JobsGo":
                    jobsgo_jobs.append(j)
                elif j.get("source") == "CareerLink":
                    careerlink_jobs.append(j)
                elif j.get("source") == "Glints":
                    glints_jobs.append(j)
                elif j.get("source") == "ViecOi":
                    viecoi_jobs.append(j)
        # Inter-site delay with jitter — only between sites, not after the last one.
        if i < len(scrapers) - 1:
            base = _SITE_DELAY.get(site, 4.0)
            jitter = random.uniform(0, base * 0.5)
            await asyncio.sleep(base + jitter)

    all_sites = set(scrapers.keys())
    if not site_succeeded and site_timeouts >= all_sites:
        log_app(
            f"[warmup] {kw!r}/{loc!r} all sources timed out — skipping cache update to allow retry"
        )
        return

    cutoff_ts = time.time() - RECENT_DAYS * 86400
    existing = await cache_get(kw, loc)
    existing_jobs = existing[0] if existing else []

    cached_desc_by_link = {
        j["link"]: j["description"] for j in existing_jobs if j.get("description")
    }
    cached_with_desc = set(cached_desc_by_link)

    new_links = {j["link"] for j in jobs}
    kept_cached = [
        j
        for j in existing_jobs
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
    log_app(
        f"[warmup] {kw!r}/{loc!r} cached — {len(new_jobs_recent)} new + {len(kept_cached)} kept = {len(merged)} total (pre-enrich)"
    )

    await vector_mark_warmup_seen(
        [j["link"] for j in merged if j.get("link")], time.time()
    )

    # Per-site enrich config: (display_name, source_jobs, detail_fn, default_limit, is_linkedin)
    _site_enrich_cfg = [
        ("LinkedIn", linkedin_jobs, scrape_linkedin_detail_one, 30, True),
        ("TopCV", topcv_jobs, scrape_topcv_detail_one, 10, False),
        ("ITViec", itviec_jobs, scrape_itviec_detail_one, 10, False),
        ("VietnamWorks", vietnamworks_jobs, scrape_vietnamworks_detail_one, 10, False),
        ("CareerViet", careerviet_jobs, scrape_careerviet_detail_one, 10, False),
        ("JobsGo", jobsgo_jobs, scrape_jobsgo_detail_one, 10, False),
        ("CareerLink", careerlink_jobs, scrape_careerlink_detail_one, 10, False),
        ("Glints", glints_jobs, scrape_glints_detail_one, 10, False),
        ("ViecOi", viecoi_jobs, scrape_viecoi_detail_one, 10, False),
    ]

    any_enriched = False
    for display, site_jobs, detail_fn, default_limit, is_linkedin in _site_enrich_cfg:
        batch = [j for j in site_jobs if j.get("link") not in cached_with_desc]
        batch.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
        batch = batch[: enrich_limit if enrich_limit is not None else default_limit]
        if not batch:
            continue
        log_app(
            f"[warmup][{kw}][{loc}] enriching {len(batch)} {display} jobs without description..."
        )
        if is_linkedin:
            await asyncio.sleep(1.0)
        enriched = 0
        for i, job in enumerate(batch):
            cooldown = 1.0 if i > 0 else 0.0
            try:
                ok = await asyncio.wait_for(
                    loop.run_in_executor(executor, detail_fn, job, cooldown),
                    timeout=60.0,
                )
                if is_linkedin and not ok:
                    break
                enriched += 1
            except asyncio.TimeoutError:
                log_app(
                    f"[warmup][{kw}][{loc}] {display} detail timeout: {job.get('link')}"
                )
            except Exception as e:
                log_app(f"[warmup][{kw}][{loc}] {display} detail error: {e}")
        log_app(
            f"[warmup][{kw}][{loc}] {display} enrich done — {enriched}/{len(batch)} jobs enriched"
        )
        any_enriched = True

    if any_enriched:
        for j in new_jobs_recent:
            j["skills"] = extract_skills(j.get("title", ""), j.get("description", ""))
        merged.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
        await cache_set(kw, loc, merged, time.time())

    await vector_mark_warmup_seen(
        [j["link"] for j in merged if j.get("link")], time.time()
    )

    log_app(
        f"[warmup] {kw!r}/{loc!r} done — {len(new_jobs_recent)} new + {len(kept_cached)} kept = {len(merged)} total ({time.perf_counter() - t0:.1f}s)"
    )


async def _embed_cached_jobs(executor) -> None:
    """Embed all cached warmup jobs that have descriptions but haven't been embedded yet.

    Should be called after summarization so jobs have their summaries before embedding.
    """
    loop = asyncio.get_event_loop()
    redis = get_redis()
    try:
        import json

        all_jobs: list[dict] = []
        async for key in redis.scan_iter("jobs:*"):
            raw = await redis.get(key)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            for job in data.get("jobs", []):
                if job.get("description", "").strip():
                    all_jobs.append(job)

        unembedded = await embedded_links_filter(all_jobs)
        if not unembedded:
            log_app("[warmup][embed] all cached jobs already embedded — skipping")
            return

        log_app(
            f"[warmup][embed] embedding {len(unembedded)} cached jobs into Vectorize..."
        )
        ok = await asyncio.wait_for(
            loop.run_in_executor(executor, upsert_jobs, unembedded),
            timeout=180.0,
        )
        if ok:
            await embedded_links_add([j["link"] for j in unembedded if j.get("link")])
            log_app(f"[warmup][embed] embedded {len(unembedded)} jobs")
        else:
            log_app(
                "[warmup][embed] vectorize upsert returned False — partial failure",
                "ERROR",
            )
    except asyncio.TimeoutError:
        log_app("[warmup][embed] vectorize upsert timed out after 180s", "ERROR")
    except Exception as e:
        log_app(f"[warmup][embed] error: {e}", "ERROR")


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
            remainder = key[len("jobs:") :]
            # Find the last colon — location part never contains a colon
            last_colon = remainder.rfind(":")
            if last_colon == -1:
                continue
            kw_part = remainder[:last_colon]
            loc_part = remainder[last_colon + 1 :]

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


async def _cleanup_nonwarmup_vectors(executor) -> None:
    """Delete non-warmup vectors from Vectorize that have not been seen in 8 days.

    Links protected by warmup (present in vector:warmup_last_seen with a recent score)
    are never deleted.
    """
    try:
        cutoff = time.time() - VECTOR_RETENTION_DAYS * 86400
        BATCH = 200
        total_deleted = 0
        total_skipped = 0
        loop = asyncio.get_event_loop()

        while True:
            candidates = await vector_get_expired_nonwarmup(cutoff, limit=BATCH)
            if not candidates:
                break

            warmup_scores = await vector_get_warmup_scores(candidates)
            to_delete: list[str] = []
            for link in candidates:
                ws = warmup_scores.get(link)
                if ws is None or ws < cutoff:
                    to_delete.append(hashlib.sha1(link.encode()).hexdigest())
                else:
                    total_skipped += 1

            if not to_delete:
                break

            ok = await loop.run_in_executor(executor, delete_by_ids, to_delete)
            if ok:
                redis = get_redis()
                pipe = redis.pipeline()
                for link in candidates:
                    ws = warmup_scores.get(link)
                    if ws is None or ws < cutoff:
                        pipe.zrem("vector:nonwarmup_last_seen", link)
                        pipe.srem("vector:embedded_links", link)
                await pipe.execute()
                total_deleted += len(to_delete)
            else:
                log_app(
                    f"[warmup][vector-cleanup] delete_by_ids failed for {len(to_delete)} ids",
                    "ERROR",
                )
                break

        trimmed = await vector_trim_warmup(cutoff)
        log_app(
            f"[warmup][vector-cleanup] done — deleted {total_deleted} vectors, skipped {total_skipped}, trimmed {trimmed} warmup entries"
        )
    except Exception as e:
        log_app(f"[warmup][vector-cleanup] error: {e}", "ERROR")


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
                    log_app(
                        f"[warmup] cleanup: dropped {len(jobs) - len(fresh)} old jobs from {_key(kw, loc)!r}"
                    )
                    cleaned += len(jobs) - len(fresh)
            except Exception as e:
                log_app(f"[warmup] cleanup error for {kw!r}/{loc!r}: {e}")
    log_app(f"[warmup] daily cleanup done — {cleaned} old job(s) removed")


async def _enrich_cycle(executor, loop) -> None:
    """For every cached warmup job missing a description, fetch it, then summarize and embed."""
    import json

    redis = get_redis()
    _DETAIL_FN = {
        "LinkedIn": scrape_linkedin_detail_one,
        "TopCV": scrape_topcv_detail_one,
        "ITViec": scrape_itviec_detail_one,
        "VietnamWorks": scrape_vietnamworks_detail_one,
        "CareerViet": scrape_careerviet_detail_one,
        "JobsGo": scrape_jobsgo_detail_one,
        "CareerLink": scrape_careerlink_detail_one,
    }

    log_app("[warmup][enrich] starting description enrich pass over cached jobs...")
    total_enriched = 0

    for kw in await get_warmup_keywords():
        for loc in _WARMUP_LOCATIONS:
            key = _key(kw, loc)
            raw = await redis.get(key)
            if not raw:
                continue
            data = json.loads(raw)
            jobs = data.get("jobs", [])
            needs_desc = [j for j in jobs if not j.get("description", "").strip()]
            if not needs_desc:
                continue

            log_app(
                f"[warmup][enrich] {kw!r}/{loc!r}: {len(needs_desc)} jobs need description"
            )
            changed = False
            for i, job in enumerate(needs_desc):
                fn = _DETAIL_FN.get(job.get("source", ""))
                if not fn:
                    continue
                cooldown = 1.5 if i > 0 else 0.0
                try:
                    await asyncio.wait_for(
                        loop.run_in_executor(executor, fn, job, cooldown),
                        timeout=60.0,
                    )
                    if job.get("description", "").strip():
                        job["skills"] = extract_skills(
                            job.get("title", ""), job["description"]
                        )
                        changed = True
                        total_enriched += 1
                except asyncio.TimeoutError:
                    log_app(f"[warmup][enrich] timeout: {job.get('link')}")
                except Exception as e:
                    log_app(f"[warmup][enrich] error: {e}")

            if changed:
                payload = json.dumps(
                    {"jobs": jobs, "fetched_ts": data["fetched_ts"]}, ensure_ascii=False
                )
                await redis.set(key, payload)

    log_app(f"[warmup][enrich] done — {total_enriched} jobs enriched")

    log_app("[warmup][enrich] triggering summarization...")
    try:
        stats = await run_background_summarization()
        log_app(f"[warmup][enrich] summarization complete: {stats}")
    except Exception as e:
        log_app(f"[warmup][enrich] summarization error: {e}", "ERROR")

    log_app("[warmup][enrich] embedding cached jobs...")
    try:
        await _embed_cached_jobs(executor)
    except Exception as e:
        log_app(f"[warmup][enrich] embed error: {e}", "ERROR")


async def _run_scrape_cycle(
    executor, scrapers: dict, loop, last_fetched_ts: float = 0.0
) -> None:
    """Scrape all warmup keyword×location pairs, then summarize and embed."""
    warmup_kws = await get_warmup_keywords()
    pairs = [(kw, loc) for kw in warmup_kws for loc in _WARMUP_LOCATIONS]
    log_app(f"[warmup] cycle: scraping {len(pairs)} keyword×location pairs")

    for i, (kw, loc) in enumerate(pairs):
        try:
            log_app(f"[warmup] scraping {kw!r}/{loc!r}...")
            await _scrape_keyword(
                kw, loc, loop, executor, scrapers, last_fetched_ts=last_fetched_ts
            )
        except Exception as e:
            log_app(f"[warmup] error for {kw!r}/{loc!r}: {e}")
        if i < len(pairs) - 1:
            await asyncio.sleep(random.uniform(15, 30))

    log_app("[warmup] cycle done — triggering background summarization")
    try:
        stats = await run_background_summarization()
        log_app(f"[warmup] background summarization complete: {stats}")
    except Exception as e:
        log_app(f"[warmup] background summarization error: {e}", "ERROR")

    log_app("[warmup] embedding cached jobs after summarization")
    try:
        await _embed_cached_jobs(executor)
    except Exception as e:
        log_app(f"[warmup] embed error: {e}", "ERROR")


async def warmup(executor, scrapers: dict) -> None:
    """Background loop that scrapes all warmup keys at 10:00 and 17:00 ICT.

    On startup, scrapes any keyword×location pairs with no cache at all.
    Then sleeps until the next scheduled run time.
    """
    # Skip warmup if Redis is not available
    try:
        from src.cache import get_redis

        redis = get_redis()
        # Try to ping Redis to check if it's available
        await redis.ping()
    except Exception as e:
        log_app(f"[warmup] Redis not available, skipping warmup: {e}")
        return

    loop = asyncio.get_event_loop()

    _CLEANUP_INTERVAL = 86400
    _last_cleanup_ts = 0.0

    await asyncio.sleep(5.0)
    await _cleanup_stale_keys()

    # Startup: scrape pairs that are missing or stale (not refreshed since last scheduled run).
    # Missing pairs get a full backfill (last_fetched_ts=0); stale pairs get an incremental
    # scrape using the cache's own fetched_ts so we only fetch new jobs since last run.
    log_app("[warmup] startup — checking for missing or stale cache entries...")
    warmup_kws = await get_warmup_keywords()

    # Consider a cache stale if it hasn't been refreshed within the expected scrape interval.
    # _SCRAPE_HOURS runs twice daily, so anything older than ~13 hours needs a top-up.
    _STALE_THRESHOLD = 13 * 3600
    now = time.time()

    needs_scrape: list[tuple[str, str, float]] = []  # (kw, loc, last_fetched_ts)
    for kw in warmup_kws:
        for loc in _WARMUP_LOCATIONS:
            cached = await cache_get(kw, loc)
            if cached is None:
                needs_scrape.append((kw, loc, 0.0))  # full backfill
            else:
                _, fetched_ts = cached
                if now - fetched_ts > _STALE_THRESHOLD:
                    needs_scrape.append((kw, loc, fetched_ts))  # incremental

    if needs_scrape:
        missing_count = sum(1 for _, _, ts in needs_scrape if ts == 0.0)
        stale_count = len(needs_scrape) - missing_count
        log_app(
            f"[warmup] startup: {missing_count} missing + {stale_count} stale entries — scraping..."
        )
        for i, (kw, loc, last_fetched_ts) in enumerate(needs_scrape):
            try:
                await _scrape_keyword(
                    kw, loc, loop, executor, scrapers, last_fetched_ts=last_fetched_ts
                )
            except Exception as e:
                log_app(f"[warmup] startup error for {kw!r}/{loc!r}: {e}")
            if i < len(needs_scrape) - 1:
                await asyncio.sleep(random.uniform(15, 30))

        log_app("[warmup] startup scrape done — summarizing and embedding")
        try:
            stats = await run_background_summarization()
            log_app(f"[warmup] startup summarization complete: {stats}")
        except Exception as e:
            log_app(f"[warmup] startup summarization error: {e}", "ERROR")
        try:
            await _embed_cached_jobs(executor)
        except Exception as e:
            log_app(f"[warmup] startup embed error: {e}", "ERROR")
    else:
        log_app("[warmup] startup: all cache entries are fresh, skipping initial scrape")

    while True:
        try:
            secs, next_hour = _seconds_until_next_scheduled()
            next_run = datetime.now(_TZ_ICT) + timedelta(seconds=secs)
            log_app(
                f"[warmup] sleeping until {next_run.strftime('%H:%M')} ICT ({secs / 3600:.1f}h)"
            )
            await asyncio.sleep(secs)

            if next_hour in _SCRAPE_HOURS:
                await _run_scrape_cycle(
                    executor, scrapers, loop, last_fetched_ts=time.time() - 86400
                )
            else:
                await _enrich_cycle(executor, loop)

            if time.time() - _last_cleanup_ts >= _CLEANUP_INTERVAL:
                await _cleanup_old_jobs()
                await _cleanup_nonwarmup_stale_keys()
                await _cleanup_nonwarmup_vectors(executor)
                _last_cleanup_ts = time.time()

        except Exception as e:
            log_app(f"[warmup] cycle crashed: {e}")
