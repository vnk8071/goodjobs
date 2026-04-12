import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import strawberry
from strawberry.fastapi import GraphQLRouter
from src.graphql_schema import schema

from src.cache import (
    cache_get,
    cache_set,
    cache_fuzzy_get,
    cache_preserve_posted_dates,
    cache_touch,
    get_redis,
    vector_mark_nonwarmup_seen,
    embedded_links_filter,
    embedded_links_add,
)
from src.constants import MAX_CONCURRENT, ADMIN_SECRET
from src.logger import log_search, log_app
from src.utils import timed_scrape
from src.matching import (
    title_matches,
    title_matches_loose,
    extract_skills,
    posted_ts,
    posted_relative,
    strip_level,
    strip_generic_role,
    correct_keyword_typos,
    normalize_keyword,
    _LEVEL_WORDS,
    LEVEL_SYNONYMS,
)
from src.models import Job, ScrapeRequest
from src.intent import suggest_query, record_search, classify_and_extract
from src.ratelimit import check_rate_limit, ip_active_inc, ip_active_dec
from src.scrapers import *
from src.vector import (
    ensure_index,
    search as vector_search,
    upsert_jobs,
    score_jobs_by_embedding,
)
from src.warmup import (
    warmup,
    _WARMUP_LOCATIONS,
    _WARMUP_KEYWORDS,
    _scrape_keyword,
    get_warmup_keywords,
    add_warmup_keyword,
    remove_warmup_keyword,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the background warmup task on application startup."""
    log_app("Application starting...")
    try:
        asyncio.create_task(warmup(_executor, _SCRAPERS))
        log_app("Warmup task scheduled")
    except Exception as e:
        log_app(f"Failed to start warmup task: {e}")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, ensure_index)
    yield
    log_app("Application shutting down")


app = FastAPI(title="Job Scraper API", lifespan=lifespan, docs_url=None, redoc_url=None)

# Add GraphQL endpoint
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")

_ALLOWED_ORIGINS = [
    "http://localhost",
    "http://localhost:80",
    "http://localhost:5173",
    "https://goodjobs.io.vn",
    "https://www.goodjobs.io.vn",
    "https://vnk8071.github.io",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

_executor = ThreadPoolExecutor(max_workers=6)

_MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_SCRAPES", str(MAX_CONCURRENT)))
_scrape_sem: asyncio.Semaphore | None = None
_warmup_sem: asyncio.Semaphore | None = None
_queue_count = 0


def _get_sem() -> asyncio.Semaphore:
    """Return the global scrape semaphore, creating it on first call."""
    global _scrape_sem
    if _scrape_sem is None:
        _scrape_sem = asyncio.Semaphore(_MAX_CONCURRENT)
    return _scrape_sem


def _get_warmup_sem() -> asyncio.Semaphore:
    """Return the warmup semaphore, capped at MAX_CONCURRENT-1 to reserve 1 slot for user requests."""
    global _warmup_sem
    if _warmup_sem is None:
        _warmup_sem = asyncio.Semaphore(max(1, _MAX_CONCURRENT - 1))
    return _warmup_sem


def _refresh_posted_times(jobs: list[dict]) -> None:
    """Recalculate the 'posted' relative time string for all jobs from their posted_ts."""
    for j in jobs:
        if "posted_ts" in j:
            j["posted"] = posted_relative(j["posted_ts"])


def _get_related_keywords(cache_keyword: str) -> list[str]:
    """Return the canonical cache key. All alias jobs are stored in the canonical cache."""
    return [cache_keyword.lower()]


async def _fetch_vector_supplement(
    query: str,
    seen_links: set[str],
    location: str,
    warmup_keywords: list[str],
    top_k: int = 20,
) -> list[dict]:
    """Search Vectorize and hydrate full job dicts from all cached keys at the user's location.

    Searches warmup keys first, then scans all jobs:{kw}:{location} keys for remaining matches.
    Excludes links already in seen_links. No cross-location fallback.
    """
    loop = asyncio.get_event_loop()
    try:
        matches = await loop.run_in_executor(_executor, vector_search, query, top_k)
    except Exception as e:
        log_app(f"[vector] supplement search error: {e}", "ERROR")
        return []
    if not matches:
        return []

    target = {
        m["link"]: m["score"]
        for m in matches
        if m.get("link") and m["link"] not in seen_links
    }
    if not target:
        return []

    try:
        redis = get_redis()
    except Exception as e:
        log_app(f"[vector] Redis unavailable for hydration: {e}", "ERROR")
        return []

    hydrated: list[dict] = []

    async def _hydrate_from_key(key: str) -> None:
        if not target:
            return
        raw = await redis.get(key)
        if not raw:
            return
        try:
            data = json.loads(raw)
            age_cutoff = time.time() - 8 * 86400
            for job in data.get("jobs", []):
                link = job.get("link", "")
                if link in target and job.get("posted_ts", 0) >= age_cutoff:
                    job["_vector_score"] = target.pop(link)
                    job["_from_vector"] = True
                    hydrated.append(job)
        except Exception:
            pass

    for kw in warmup_keywords:
        await _hydrate_from_key(f"jobs:{kw.lower()}:{location.lower()}")

    if target:
        loc_pattern = f"jobs:*:{location.lower()}"
        warmup_keys = {
            f"jobs:{kw.lower()}:{location.lower()}" for kw in warmup_keywords
        }
        async for key in redis.scan_iter(loc_pattern):
            key_str = key.decode() if isinstance(key, bytes) else key
            if key_str not in warmup_keys:
                await _hydrate_from_key(key_str)
            if not target:
                break

    hydrated.sort(key=lambda j: j["_vector_score"], reverse=True)
    return hydrated


_SCRAPERS = {
    "linkedin": scrape_linkedin,
    "itviec": scrape_itviec,
    "topcv": scrape_topcv,
    "vietnamworks": scrape_vietnamworks,
    "topdev": scrape_topdev,
    # "indeed":       scrape_indeed,
    "careerviet": scrape_careerviet,
    "jobsgo": scrape_jobsgo,
    "careerlink": scrape_careerlink,
    "glints": scrape_glints,
    "viecoi": scrape_viecoi,
}

NON_WARMUP_ENRICH_LIMIT = 10
_active_bg_rescrapes: set[str] = set()
_active_bg_rescrapes_lock = asyncio.Lock()


def _is_warmup_keyword(keyword: str) -> bool:
    """Return True if keyword (case-insensitive) is in the hardcoded warmup list."""
    return keyword.lower().strip() in {kw.lower().strip() for kw in _WARMUP_KEYWORDS}


async def _background_rescrape(
    keyword: str, location: str, last_fetched_ts: float
) -> None:
    """Background re-scrape for a non-warmup cache hit.

    Runs the full scrape+enrich cycle using the existing _scrape_keyword() from warmup,
    but caps description enrichment at NON_WARMUP_ENRICH_LIMIT per site.
    Uses the warmup semaphore to avoid overloading the executor.
    """
    key = f"{keyword.lower()}:{location.lower()}"
    async with _active_bg_rescrapes_lock:
        if key in _active_bg_rescrapes:
            log_app(
                f"[bg-rescrape] already in progress for {keyword!r}/{location!r}, skipping"
            )
            return
        _active_bg_rescrapes.add(key)
    sem = _get_warmup_sem()
    async with sem:
        loop = asyncio.get_event_loop()
        try:
            log_app(f"[bg-rescrape] starting for {keyword!r}/{location!r}")
            await _scrape_keyword(
                keyword,
                location,
                loop,
                _executor,
                _SCRAPERS,
                last_fetched_ts=last_fetched_ts,
                enrich_limit=NON_WARMUP_ENRICH_LIMIT,
            )
        except Exception as e:
            log_app(f"[bg-rescrape] error for {keyword!r}/{location!r}: {e}", "ERROR")
        finally:
            async with _active_bg_rescrapes_lock:
                _active_bg_rescrapes.discard(key)


@app.get("/")
@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "good jobs"}


async def _cache_status_data() -> dict:
    now = time.time()
    STALE_THRESHOLD = 7200
    keys = []
    stale_count = 0
    for kw in await get_warmup_keywords():
        for loc in _WARMUP_LOCATIONS:
            existing = await cache_get(kw, loc)
            is_missing = existing is None
            fetched_ts = existing[1] if existing else 0.0
            job_count = len(existing[0]) if existing else 0
            if fetched_ts:
                age = int(now - fetched_ts)
                if age < 3600:
                    fetched_ago = f"{age // 60}m ago"
                elif age < 86400:
                    fetched_ago = f"{age // 3600}h {(age % 3600) // 60}m ago"
                else:
                    fetched_ago = f"{age // 86400}d ago"
            else:
                fetched_ago = "never"
            is_stale = is_missing or (now - fetched_ts >= STALE_THRESHOLD)
            if is_stale:
                stale_count += 1
            keys.append(
                {
                    "keyword": kw,
                    "location": loc,
                    "missing": is_missing,
                    "stale": is_stale,
                    "fetched_ago": fetched_ago,
                    "job_count": job_count,
                }
            )
    return {
        "total": len(keys),
        "missing": sum(1 for k in keys if k["missing"]),
        "stale": stale_count,
        "keys": keys,
    }


@app.get("/cache/status")
async def cache_status():
    """Report status of all cache keys."""
    return await _cache_status_data()


@app.get("/cache/overview")
async def cache_overview(secret: str = ""):
    """Dashboard data: all cached keys with job counts + total embedded vector count."""
    if ADMIN_SECRET and secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    now = time.time()
    STALE_THRESHOLD = 7200
    age_cutoff = now - 8 * 86400

    redis = get_redis()
    # Pass 1 — collect all cache keys and their fresh job links
    keys_data: list[dict] = []
    # Map from cache key → list of fresh job links (for embedding check)
    key_links: dict[str, list[str]] = {}
    all_links: list[str] = []  # ordered, for pipeline batch check
    warmup_kws = set(kw.lower().strip() for kw in await get_warmup_keywords())

    try:
        async for key in redis.scan_iter("jobs:*"):
            raw = await redis.get(key)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            jobs = data.get("jobs", [])
            fetched_ts = float(data.get("fetched_ts", 0))

            # Parse keyword/location from key "jobs:<kw>:<loc>"
            parts = key.split(":")
            kw_part = parts[1] if len(parts) >= 3 else ""
            loc_part = ":".join(parts[2:]) if len(parts) >= 3 else ""

            fresh_jobs = [j for j in jobs if j.get("posted_ts", 0) >= age_cutoff]
            links = [j["link"] for j in fresh_jobs if j.get("link")]
            key_links[key] = links
            all_links.extend(links)

            age = int(now - fetched_ts) if fetched_ts else None
            if age is not None:
                if age < 3600:
                    fetched_ago = f"{age // 60}m ago"
                elif age < 86400:
                    fetched_ago = f"{age // 3600}h {(age % 3600) // 60}m ago"
                else:
                    fetched_ago = f"{age // 86400}d ago"
            else:
                fetched_ago = "never"

            is_warmup = kw_part.lower().strip() in warmup_kws
            is_stale = fetched_ts == 0 or (now - fetched_ts >= STALE_THRESHOLD)

            keys_data.append(
                {
                    "key": key,
                    "keyword": kw_part,
                    "location": loc_part,
                    "job_count": len(fresh_jobs),
                    "fetched_ts": fetched_ts,
                    "fetched_ago": fetched_ago,
                    "stale": is_stale,
                    "warmup": is_warmup,
                    "embedded_count": 0,  # filled in pass 2
                }
            )
    except Exception as e:
        log_app(f"[cache/overview] scan error: {e}", "ERROR")

    # Pass 2 — batch-check all links against vector:embedded_links in one pipeline
    embedded_set: set[str] = set()
    try:
        if all_links:
            pipe = redis.pipeline()
            for link in all_links:
                pipe.sismember("vector:embedded_links", link)
            results = await pipe.execute()
            embedded_set = {link for link, is_emb in zip(all_links, results) if is_emb}
    except Exception as e:
        log_app(f"[cache/overview] embedding check error: {e}", "ERROR")

    # Fill per-key embedded counts
    total_jobs = 0
    seen_links: set[str] = set()
    for entry in keys_data:
        links = key_links.get(entry["key"], [])
        entry["embedded_count"] = sum(1 for l in links if l in embedded_set)
        total_jobs += entry["job_count"]
        seen_links.update(links)

    # Sort: warmup first, then by job_count desc
    keys_data.sort(key=lambda k: (not k["warmup"], -k["job_count"]))

    embedded_count = len(embedded_set)

    return {
        "total_keys": len(keys_data),
        "total_jobs": total_jobs,
        "total_unique_links": len(seen_links),
        "embedded_count": embedded_count,
        "keys": keys_data,
    }


@app.get("/admin/embedded-jobs")
async def admin_embedded_jobs(secret: str = ""):
    """List all cached jobs with their embedding status."""
    if ADMIN_SECRET and secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    redis = get_redis()
    jobs_out: list[dict] = []
    seen_links: set[str] = set()

    async for key in redis.scan_iter("jobs:*"):
        raw = await redis.get(key)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for job in data.get("jobs", []):
            link = job.get("link", "")
            if not link or link in seen_links:
                continue
            seen_links.add(link)
            jobs_out.append(
                {
                    "title": job.get("title", ""),
                    "company": job.get("company", ""),
                    "location": job.get("location", ""),
                    "source": job.get("source", ""),
                    "posted": job.get("posted", ""),
                    "posted_ts": job.get("posted_ts", 0),
                    "link": link,
                    "description": job.get("description", "").strip(),
                    "has_description": bool(job.get("description", "").strip()),
                }
            )

    if not jobs_out:
        return {"count": 0, "jobs": []}

    # Batch-check which links are embedded
    pipe = redis.pipeline()
    for job in jobs_out:
        pipe.sismember("vector:embedded_links", job["link"])
    embedded_flags = await pipe.execute()

    for job, is_embedded in zip(jobs_out, embedded_flags):
        job["embedded"] = bool(is_embedded)

    jobs_out.sort(key=lambda j: j.get("posted_ts", 0), reverse=True)
    return {"count": len(jobs_out), "jobs": jobs_out}


@app.post("/admin/embed-test")
async def admin_embed_test(secret: str = "", n: int = 3):
    """Embed a small sample of cached jobs to verify the Cloudflare Vectorize pipeline.

    Picks up to `n` unembedded jobs from the cache, embeds them, upserts to Vectorize,
    and marks them in Redis. Returns per-job results so you can see exactly what happened.
    """
    if ADMIN_SECRET and secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    n = max(1, min(n, 20))  # clamp 1–20

    # Find up to n unembedded fresh jobs from the cache
    redis = get_redis()
    age_cutoff = time.time() - 8 * 86400
    candidates: list[dict] = []
    try:
        async for key in redis.scan_iter("jobs:*"):
            if len(candidates) >= n * 5:  # oversample to account for already-embedded
                break
            raw = await redis.get(key)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            for job in data.get("jobs", []):
                if (
                    job.get("posted_ts", 0) >= age_cutoff
                    and job.get("link")
                    and job.get("title")
                ):
                    candidates.append(job)
                    if len(candidates) >= n * 5:
                        break
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis scan failed: {e}")

    if not candidates:
        return {"ok": False, "error": "No fresh jobs found in cache", "results": []}

    # Filter out already-embedded jobs
    loop = asyncio.get_event_loop()
    unembedded = await embedded_links_filter(candidates)
    sample = (
        unembedded[:n] or candidates[:n]
    )  # fallback to re-embed if all already done

    # Run upsert in executor (blocking HTTP calls)
    t0 = time.perf_counter()
    ok = await loop.run_in_executor(_executor, upsert_jobs, sample)
    elapsed = round(time.perf_counter() - t0, 2)

    if ok:
        await embedded_links_add([j["link"] for j in sample])

    results = [
        {
            "title": j.get("title", ""),
            "company": j.get("company", ""),
            "source": j.get("source", ""),
            "link": j.get("link", ""),
            "already_embedded": j not in unembedded,
        }
        for j in sample
    ]

    return {
        "ok": ok,
        "elapsed_s": elapsed,
        "attempted": len(sample),
        "results": results,
    }


@app.get("/admin/analytics")
async def admin_analytics(secret: str = ""):
    """Aggregate search.log into request statistics and write analytics.json backup."""
    if ADMIN_SECRET and secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    import glob as _glob
    from collections import Counter

    log_dir = os.getenv("LOG_DIR", os.path.join(os.path.dirname(__file__), "logs"))
    backup_path = os.path.join(log_dir, "analytics.json")

    # Collect all rotated log files: search.log, search.log.1, ..., search.log.5
    # Filter to only numeric suffixes to avoid crashing on e.g. search.log.gz
    import re as _re
    _log_re = _re.compile(r"search\.log(\.\d+)?$")
    log_files = sorted(
        [p for p in _glob.glob(os.path.join(log_dir, "search.log*")) if _log_re.search(p)],
        key=lambda p: (0 if p.endswith("search.log") else int(p.rsplit(".", 1)[-1])),
    )

    entries: list[dict] = []
    for path in log_files:
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entries.append(json.loads(line))
                    except Exception:
                        continue
        except OSError:
            continue

    # Fall back to backup if no entries parsed
    if not entries:
        try:
            with open(backup_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {
                "total_requests": 0,
                "today_requests": 0,
                "unique_ips": 0,
                "top_keywords": [],
                "requests_by_hour": {str(h): 0 for h in range(24)},
                "requests_by_week": [],
                "intent_breakdown": {"job_title": 0, "cv_or_skills": 0, "not_job": 0},
                "recent_searches": [],
            }

    _TZ_ICT = timezone(timedelta(hours=7))
    today_str = datetime.now(_TZ_ICT).strftime("%Y-%m-%d")

    keyword_counter: Counter = Counter()
    hour_counter_today: Counter = Counter()
    day_counter: Counter = Counter()
    intent_counter: Counter = Counter()
    unique_ips: set = set()
    today_count = 0

    # Build last-7-days date strings for requests_by_week
    week_dates = [
        (datetime.now(_TZ_ICT) - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(6, -1, -1)
    ]
    week_dates_set = set(week_dates)

    for e in entries:
        kw = e.get("keyword", "").strip()
        ip = e.get("ip", "")
        ts = e.get("ts", "")

        if kw:
            keyword_counter[kw.lower()] += 1
        if ip:
            unique_ips.add(ip)
        intent = e.get("intent", "")
        if intent:
            intent_counter[intent] += 1
        if ts:
            try:
                dt = datetime.fromisoformat(ts)
                date_str = dt.strftime("%Y-%m-%d")
                if date_str == today_str:
                    hour_counter_today[dt.hour] += 1
                    today_count += 1
                if date_str in week_dates_set:
                    day_counter[date_str] += 1
            except Exception:
                pass

    top_keywords = [
        {"keyword": kw, "count": cnt}
        for kw, cnt in keyword_counter.most_common(10)
    ]

    requests_by_hour = {str(h): hour_counter_today.get(h, 0) for h in range(24)}
    requests_by_week = [
        {"date": d, "count": day_counter.get(d, 0)} for d in week_dates
    ]

    recent_searches = [
        {
            "ts": e.get("ts", ""),
            "ip": e.get("ip", ""),
            "keyword": e.get("keyword", ""),
            "location": e.get("location", ""),
            "intent": e.get("intent", ""),
        }
        for e in reversed(entries[-50:])
    ]

    intent_breakdown = {
        "job_title": intent_counter.get("job_title", 0),
        "cv_or_skills": intent_counter.get("cv_or_skills", 0),
        "not_job": intent_counter.get("not_job", 0),
    }

    result = {
        "total_requests": len(entries),
        "today_requests": today_count,
        "unique_ips": len(unique_ips),
        "top_keywords": top_keywords,
        "requests_by_hour": requests_by_hour,
        "requests_by_week": requests_by_week,
        "intent_breakdown": intent_breakdown,
        "recent_searches": recent_searches,
    }

    # Write backup
    try:
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
    except OSError as e:
        log_app(f"[analytics] failed to write backup: {e}", "WARNING")

    return result


@app.get("/stats")
async def stats():
    """Return total unique jobs posted in the last 7 days across all cached keys."""
    cutoff = time.time() - 8 * 86400
    seen_links: set[str] = set()
    count = 0
    try:
        redis = get_redis()
        async for key in redis.scan_iter("jobs:*"):
            raw = await redis.get(key)
            if not raw:
                continue
            try:
                data = json.loads(raw)
                for job in data.get("jobs", []):
                    link = job.get("link", "")
                    ts = job.get("posted_ts", 0.0)
                    if ts >= cutoff and link and link not in seen_links:
                        seen_links.add(link)
                        count += 1
            except Exception:
                continue
    except Exception as e:
        log_app(f"[stats] error: {e}", "ERROR")
    return {"jobs_this_week": count}


@app.get("/cache/scrape")
async def cache_scrape(
    secret: str = "",
    keyword: str | None = None,
    location: str | None = None,
    warmup_only: bool = False,
):
    """Trigger a background scrape. Optionally filter by keyword and/or location.

    Examples:
      /cache/scrape
      /cache/scrape?keyword=AI Engineer
      /cache/scrape?keyword=AI Engineer&location=Ho Chi Minh City
      /cache/scrape?warmup_only=true
    """
    if ADMIN_SECRET and secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    loop = asyncio.get_event_loop()
    status = await _cache_status_data()

    warmup_kws = set(kw.lower().strip() for kw in await get_warmup_keywords())

    keys_to_scrape = []
    for k in status["keys"]:
        if keyword is not None and k["keyword"].lower() != keyword.lower():
            continue
        if location is not None and k["location"].lower() != location.lower():
            continue
        is_warmup = k["keyword"].lower().strip() in warmup_kws
        if warmup_only and not is_warmup:
            continue
        keys_to_scrape.append(k)

    if not keys_to_scrape:
        return {"triggered": 0, "message": "no matching keys found"}

    async def _run() -> None:
        for k in keys_to_scrape:
            kw, loc = k["keyword"], k["location"]
            existing = await cache_get(kw, loc)
            fetched_ts = existing[1] if existing else 0.0
            try:
                await _scrape_keyword(
                    kw, loc, loop, _executor, _SCRAPERS, last_fetched_ts=fetched_ts
                )
            except Exception as e:
                log_app(f"[cache/scrape] error for {kw!r}/{loc!r}: {e}", "ERROR")

    asyncio.create_task(_run())
    triggered = [
        {"keyword": k["keyword"], "location": k["location"]} for k in keys_to_scrape
    ]
    return {"triggered": len(triggered), "keys": triggered}


@app.get("/warmup/keywords")
async def list_warmup_keywords():
    """List all current warmup keywords."""
    keywords = await get_warmup_keywords()
    return {"keywords": keywords, "count": len(keywords)}


@app.post("/suggest-query")
async def suggest_query_endpoint(req: ScrapeRequest, request: Request):
    """Pre-verify user query with Cloudflare AI + IP search history.

    Returns spelling correction and best matching cached keyword.
    Response:
      {
        "corrected": str,
        "changed": bool,
        "suggested_cache_keyword": str | None,
        "reasoning": str,
      }
    """
    keyword = req.keyword.strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="keyword is required")

    ip = (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )

    warmup_kws = await get_warmup_keywords()
    # Scan Redis for all cached keyword keys to give AI more coverage
    cached_kws: list[str] = list(warmup_kws)
    try:
        loc = req.location.lower().strip() if req.location else ""
        pattern = f"jobs:*:{loc}" if loc else "jobs:*"
        async for key in get_redis().scan_iter(pattern):
            # Extract keyword portion from "jobs:<kw>:<loc>"
            parts = key.split(":")
            if len(parts) >= 3:
                kw_part = ":".join(parts[1:-1]) if loc else ":".join(parts[1:])
                if kw_part and kw_part not in cached_kws:
                    cached_kws.append(kw_part)
    except Exception:
        pass

    result = await suggest_query(keyword, ip, cached_kws)
    return result


@app.post("/classify-input")
async def classify_input_endpoint(req: ScrapeRequest):
    """Classify user input as job_title or cv_or_skills, and extract a clean keyword.

    Response:
      {
        "input_type": "job_title" | "cv_or_skills",
        "keyword": str,
        "reasoning": str,
        "is_job_title": bool,
      }
    """
    raw = req.keyword.strip()
    if not raw:
        raise HTTPException(status_code=400, detail="keyword is required")
    return await classify_and_extract(raw)


@app.post("/scrape", response_model=list[Job])
async def scrape(req: ScrapeRequest, request: Request):
    """Scrape jobs for a keyword and location, returning all results as a JSON array."""
    keyword = req.keyword.strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="keyword is required")
    log_search(request, keyword, req.location, req.intent)

    # Rely on AI suggestion flow for typo handling; avoid hardcoded corrections.
    keyword_normalized = " ".join(
        re.sub(r"\d+", " ", normalize_keyword(keyword)).split()
    )
    cache_keyword = strip_level(keyword_normalized)

    kw_words_lower_s = set(keyword_normalized.lower().split())
    requested_levels_s = kw_words_lower_s & _LEVEL_WORDS
    has_level_s = bool(requested_levels_s)
    match_keyword_s = cache_keyword if has_level_s else keyword
    match_fn_s = title_matches if has_level_s else title_matches_loose

    all_cached_jobs: list[dict] = []
    related_keywords = _get_related_keywords(cache_keyword)
    for related_kw in related_keywords:
        cached = await cache_get(related_kw, req.location)
        if cached:
            cached_jobs, _ = cached
            all_cached_jobs.extend(cached_jobs)

    if all_cached_jobs:
        log_app(
            f"cache hit — {len(all_cached_jobs)} jobs from {len(related_keywords)} related keywords (/scrape)"
        )
        all_cached_jobs_by_link = {j.get("link"): j for j in all_cached_jobs}
        unique_jobs = list(all_cached_jobs_by_link.values())
        unique_jobs = [
            j for j in unique_jobs if match_fn_s(j.get("title", ""), match_keyword_s)
        ]
        for j in unique_jobs:
            if has_level_s:
                title_lower = j.get("title", "").lower()
                j["level_match"] = any(lvl in title_lower for lvl in requested_levels_s)
        log_app(
            f"level filter: {keyword!r} → {cache_keyword!r}, {len(unique_jobs)} jobs after filtering"
        )
        _refresh_posted_times(unique_jobs)
        return unique_jobs

    loop = asyncio.get_event_loop()

    tasks = [
        loop.run_in_executor(
            _executor, timed_scrape, site, fn, cache_keyword, req.location
        )
        for site, fn in _SCRAPERS.items()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    jobs: list[dict] = []
    linkedin_jobs: list[dict] = []
    for result in results:
        if isinstance(result, Exception):
            log_app(f"Scraper error: {result}", "ERROR")
        elif isinstance(result, list):
            for j in result:
                if title_matches(j.get("title", ""), keyword):
                    j["posted_ts"] = posted_ts(j)
                    jobs.append(j)
                    if j.get("source") == "LinkedIn":
                        linkedin_jobs.append(j)

    if linkedin_jobs:
        await loop.run_in_executor(_executor, scrape_linkedin_details, linkedin_jobs)

    for j in jobs:
        j["skills"] = extract_skills(j.get("title", ""), j.get("description", ""))

    await cache_preserve_posted_dates(cache_keyword, req.location, jobs)
    await cache_set(cache_keyword, req.location, jobs, time.time())
    _refresh_posted_times(jobs)
    return jobs


@app.get("/search-semantic")
async def search_semantic(q: str, top_k: int = 20):
    """Semantic job search using Cloudflare Vectorize + embeddinggemma-300m embeddings.

    Embeds the query string and returns the top-k most similar indexed jobs by cosine similarity.
    """
    if not q.strip():
        raise HTTPException(status_code=400, detail="q is required")
    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(_executor, vector_search, q.strip(), top_k)
    return {"query": q.strip(), "count": len(results), "results": results}


@app.post("/scrape-stream")
async def scrape_stream(req: ScrapeRequest, request: Request):
    """Stream job results via SSE as each scraper finishes.

    On cache hit: emits cached jobs immediately then closes.
    On cache miss: runs all scrapers in parallel, streams results per site.
    LinkedIn and TopCV descriptions are enriched in Phase 2 and streamed job-by-job.
    """
    keyword = req.keyword.strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="keyword is required")

    ip = (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )
    rate_err, rate_headers = check_rate_limit(ip)
    if rate_err:
        raise HTTPException(status_code=429, detail=rate_err, headers=rate_headers)

    log_search(request, keyword, req.location, req.intent)
    # Record this search in IP history for future intent suggestions (fire-and-forget)
    asyncio.ensure_future(record_search(ip, keyword, req.location))

    warmup_kws = await get_warmup_keywords()

    # Rely on AI suggestion flow for typo handling; avoid hardcoded corrections.
    keyword_normalized = " ".join(
        re.sub(r"\d+", " ", normalize_keyword(keyword)).split()
    )
    cache_keyword = strip_level(keyword_normalized)

    # Detect level words in the requested keyword (e.g. "junior" in "Junior Software Engineer").
    # When present, broaden the search to the level-stripped keyword so we get enough results,
    # then tag each job whose title matches the level for frontend highlighting.
    kw_words_lower = set(keyword_normalized.lower().split())
    requested_levels = kw_words_lower & _LEVEL_WORDS
    has_level = bool(requested_levels)
    # Use for title matching: if a level was requested, match against the stripped keyword
    # so that "Software Engineer" jobs are included alongside "Junior Software Engineer".
    match_keyword = cache_keyword if has_level else keyword

    # For CV input with no explicit level word, use AI-inferred level from classify step.
    # Map each inferred level to the title words we look for in job listings.
    inferred_levels: set[str] = set()
    if not has_level and req.estimated_level in LEVEL_SYNONYMS:
        inferred_levels = LEVEL_SYNONYMS[req.estimated_level]

    loop = asyncio.get_event_loop()
    _is_warmup_kw = any(
        cache_keyword.lower().strip() == kw.lower().strip() for kw in warmup_kws
    )
    _is_warmup_loc = any(
        req.location.strip().lower() == loc.strip().lower() for loc in _WARMUP_LOCATIONS
    )
    is_warmup = _is_warmup_kw and _is_warmup_loc

    # For warmup, keep matching strict to reduce cache noise; for user searches, be looser.
    match_fn = title_matches if is_warmup else title_matches_loose

    def _tag_level(j: dict) -> dict:
        """Mark job as level_match if its title contains any of the requested/inferred level words."""
        title_lower = j.get("title", "").lower()
        if has_level:
            j["level_match"] = any(lvl in title_lower for lvl in requested_levels)
        elif inferred_levels:
            j["level_match"] = any(lvl in title_lower for lvl in inferred_levels)
        return j

    def _process(jobs: list[dict]) -> list[dict]:
        filtered = []
        for j in jobs:
            if is_warmup and not title_matches(j.get("title", ""), match_keyword):
                continue
            if not is_warmup and not title_matches_loose(
                j.get("title", ""), match_keyword
            ):
                continue
            j["posted_ts"] = posted_ts(j)
            j["posted"] = posted_relative(j["posted_ts"])
            j["skills"] = extract_skills(j.get("title", ""), j.get("description", ""))
            _tag_level(j)
            filtered.append(j)
        return filtered

    async def event_generator() -> AsyncGenerator[str, None]:
        global _queue_count
        loop = asyncio.get_event_loop()
        sem = _get_sem()
        ip_active_inc(ip)
        try:
            # CV/skills mode: combine semantic (embedding) matches with keyword cache results.
            # Shows all cached results immediately, then scrapes fresh ones.
            raw_vector_query = (req.raw_input or "").strip()
            vector_seen_links: set[str] = set()

            cached_prefill_jobs: list[dict] = []
            cached_prefill_latest_ts = 0.0
            fuzzy_matched_kw = None

            all_cached_jobs: list[dict] = []
            cache_fetched_ts_list = []
            related_keywords = _get_related_keywords(cache_keyword)
            for related_kw in related_keywords:
                cached = await cache_get(related_kw, req.location)
                if cached:
                    cached_jobs, cache_fetched_ts = cached
                    all_cached_jobs.extend(cached_jobs)
                    cache_fetched_ts_list.append(cache_fetched_ts)

            if raw_vector_query:
                # Fetch embedding results and keyword cache in parallel, combine into one batch.
                vector_primary = await _fetch_vector_supplement(
                    raw_vector_query,
                    seen_links=set(),
                    location=req.location,
                    warmup_keywords=warmup_kws,
                    top_k=50,
                )
                # Only keep high-confidence semantic matches (score >= 0.6) to avoid noise, especially for warmup keywords where we want to maintain cache quality.
                vector_primary = [
                    j for j in vector_primary if j.get("_vector_score", 0.0) >= 0.6
                ]

                # Build combined map: start with keyword-cache jobs, then overlay vector matches
                # (vector overrides to attach the score).
                age_cutoff_cv = time.time() - 8 * 86400
                combined_by_link: dict[str, dict] = {}
                if all_cached_jobs:
                    log_app(
                        f"cv cache — {len(all_cached_jobs)} jobs from {len(related_keywords)} related keywords"
                    )
                    unique_cache = list(
                        {j.get("link"): j for j in all_cached_jobs}.values()
                    )
                    unique_cache = [
                        j
                        for j in unique_cache
                        if match_fn(j.get("title", ""), match_keyword)
                        and j.get("posted_ts", 0) >= age_cutoff_cv
                    ]

                    # Also compute semantic scores for cached jobs so "Điểm" is populated
                    # even when the job wasn't embedded into Vectorize.
                    # Best-effort: if embedding is unavailable, jobs will simply have no score.
                    try:
                        await loop.run_in_executor(
                            _executor,
                            score_jobs_by_embedding,
                            unique_cache,
                            raw_vector_query,
                            250,
                        )
                    except Exception as e:
                        log_app(f"[cv] local embedding score error: {e}", "ERROR")

                    for j in unique_cache:
                        _tag_level(j)
                    _refresh_posted_times(unique_cache)
                    for j in unique_cache:
                        combined_by_link[str(j.get("link", ""))] = j

                for j in vector_primary:
                    link = str(j.get("link", ""))
                    j["posted_ts"] = posted_ts(j)
                    j["posted"] = posted_relative(j["posted_ts"])
                    j["skills"] = extract_skills(
                        j.get("title", ""), j.get("description", "")
                    )
                    _tag_level(j)
                    combined_by_link[link] = j  # vector overrides with score

                combined = list(combined_by_link.values())
                combined.sort(
                    key=lambda j: (
                        j.get("_vector_score", 0.0),
                        j.get("posted_ts", 0.0),
                    ),
                    reverse=True,
                )

                if combined:
                    now_ts = (
                        max(cache_fetched_ts_list)
                        if cache_fetched_ts_list
                        else time.time()
                    )
                    yield (
                        "event: cached\n"
                        f"data: {json.dumps({'jobs': combined, 'fetched_ts': now_ts, 'fuzzy': True}, ensure_ascii=False)}\n\n"
                    )
                    for j in combined:
                        if j.get("link"):
                            vector_seen_links.add(str(j["link"]))
                    cached_prefill_jobs = combined
                    cached_prefill_latest_ts = now_ts

                # Skip the regular cache emission below — already handled above.
                all_cached_jobs = []

            if all_cached_jobs:
                log_app(
                    f"cache hit — {len(all_cached_jobs)} jobs from {len(related_keywords)} related keywords"
                )
                all_cached_jobs_by_link = {j.get("link"): j for j in all_cached_jobs}
                unique_jobs = list(all_cached_jobs_by_link.values())
                unique_jobs = [
                    j
                    for j in unique_jobs
                    if match_fn(j.get("title", ""), match_keyword)
                ]
                # Fallback: if no jobs matched, retry with generic role words stripped
                # so "LLM Specialist" matches cached "LLM Engineer" jobs.
                if not unique_jobs:
                    fallback_kw = strip_generic_role(match_keyword)
                    if fallback_kw != match_keyword.lower().strip():
                        unique_jobs = [
                            j
                            for j in all_cached_jobs_by_link.values()
                            if match_fn(j.get("title", ""), fallback_kw)
                        ]
                for j in unique_jobs:
                    _tag_level(j)
                age_cutoff = time.time() - 8 * 86400
                unique_jobs = [
                    j for j in unique_jobs if j.get("posted_ts", 0) >= age_cutoff
                ]
                log_app(
                    f"level filter: {keyword!r} → {cache_keyword!r}, {len(unique_jobs)} jobs after filtering"
                )
                _refresh_posted_times(unique_jobs)
                latest_ts = max(cache_fetched_ts_list) if cache_fetched_ts_list else 0
                yield f"event: cached\ndata: {json.dumps({'jobs': unique_jobs, 'fetched_ts': latest_ts, 'fuzzy': is_warmup}, ensure_ascii=False)}\n\n"

                if is_warmup and unique_jobs:
                    yield "event: done\ndata: {}\n\n"
                    seen_links = {str(j["link"]) for j in unique_jobs if j.get("link")}
                    vector_query = req.raw_input.strip() or keyword
                    vector_supplement = await _fetch_vector_supplement(
                        vector_query, seen_links, req.location, warmup_kws
                    )
                    if vector_supplement:
                        _refresh_posted_times(vector_supplement)
                        yield f"event: vector-results\ndata: {json.dumps({'jobs': vector_supplement, 'count': len(vector_supplement)}, ensure_ascii=False)}\n\n"
                    return

                # Cache hit but jobs empty or non-warmup: show cached jobs then scrape fresh inline.
                try:
                    await cache_touch(cache_keyword, req.location)
                    await vector_mark_nonwarmup_seen(
                        [str(j["link"]) for j in unique_jobs if j.get("link")],
                        time.time(),
                    )
                except Exception as e:
                    log_app(
                        f"[scrape-stream] Redis error updating cache touch: {e}",
                        "ERROR",
                    )
                cached_prefill_jobs = unique_jobs
                cached_prefill_latest_ts = latest_ts

            fuzzy = await cache_fuzzy_get(cache_keyword, req.location)
            if fuzzy:
                fuzzy_jobs, fuzzy_fetched_ts, fuzzy_matched_kw = fuzzy
                age_cutoff_fuzzy = time.time() - 8 * 86400
                refiltered = [
                    j
                    for j in fuzzy_jobs
                    if match_fn(j.get("title", ""), match_keyword)
                    and j.get("posted_ts", 0) >= age_cutoff_fuzzy
                ]
                if not refiltered:
                    fallback_kw = strip_generic_role(match_keyword)
                    if fallback_kw != match_keyword.lower().strip():
                        refiltered = [
                            j
                            for j in fuzzy_jobs
                            if match_fn(j.get("title", ""), fallback_kw)
                            and j.get("posted_ts", 0) >= age_cutoff_fuzzy
                        ]
                for j in refiltered:
                    _tag_level(j)
                if refiltered:
                    log_app(
                        f"cache fuzzy — streaming {len(refiltered)} jobs for {keyword!r}, done"
                    )
                    _refresh_posted_times(refiltered)
                    yield f"event: cached\ndata: {json.dumps({'jobs': refiltered, 'fetched_ts': fuzzy_fetched_ts, 'fuzzy': True}, ensure_ascii=False)}\n\n"
                    if is_warmup:
                        yield "event: done\ndata: {}\n\n"
                        return
                    asyncio.create_task(
                        cache_set(
                            cache_keyword, req.location, refiltered, fuzzy_fetched_ts
                        )
                    )
                    cached_prefill_jobs = cached_prefill_jobs or refiltered
                    cached_prefill_latest_ts = (
                        cached_prefill_latest_ts or fuzzy_fetched_ts
                    )
                else:
                    log_app(
                        f"cache fuzzy — 0 jobs matched {keyword!r} after re-filter, skipping fuzzy"
                    )

            if sem._value == 0:  # noqa: SLF001
                _queue_count += 1
                pos = _queue_count
                yield f"event: queued\ndata: {json.dumps({'position': pos})}\n\n"

            async with sem:
                if _queue_count > 0:
                    _queue_count -= 1
                yield "event: started\ndata: {}\n\n"

                fetch_ts = time.time()

                scrape_kw = fuzzy_matched_kw if fuzzy_matched_kw else cache_keyword
                linkedin_fut = loop.run_in_executor(
                    _executor,
                    timed_scrape,
                    "linkedin",
                    scrape_linkedin,
                    scrape_kw,
                    req.location,
                )
                other_scrapers = {k: v for k, v in _SCRAPERS.items() if k != "linkedin"}
                futures: dict = {linkedin_fut: "linkedin"}
                for site, fn in other_scrapers.items():
                    futures[
                        loop.run_in_executor(
                            _executor, timed_scrape, site, fn, scrape_kw, req.location
                        )
                    ] = site

                all_jobs: list[dict] = (
                    list(cached_prefill_jobs) if cached_prefill_jobs else []
                )
                pending = set(futures.keys())
                deadline = loop.time() + 120.0

                enrich_limit = NON_WARMUP_ENRICH_LIMIT if not is_warmup else 30

                # Per-site enrich config: (display_name, detail_fn, cooldown, initial_sleep)
                _ENRICH_CFG = [
                    ("linkedin", scrape_linkedin_detail_one, 3.0, 10.0),
                    ("topcv", scrape_topcv_detail_one, 2.0, 0.0),
                    ("itviec", scrape_itviec_detail_one, 2.0, 0.0),
                    ("topdev", scrape_topdev_detail_one, 2.0, 0.0),
                    ("jobsgo", scrape_jobsgo_detail_one, 2.0, 0.0),
                    ("careerlink", scrape_careerlink_detail_one, 2.0, 0.0),
                    ("glints", scrape_glints_detail_one, 2.0, 0.0),
                    ("viecoi", scrape_viecoi_detail_one, 2.0, 0.0),
                ]
                # queue and task keyed by site name
                enrich_queues: dict[str, asyncio.Queue] = {
                    cfg[0]: asyncio.Queue() for cfg in _ENRICH_CFG
                }
                enrich_tasks: dict[str, asyncio.Task | None] = {
                    cfg[0]: None for cfg in _ENRICH_CFG
                }

                async def _site_phase2(
                    site: str,
                    detail_fn,
                    cooldown: float,
                    initial_sleep: float,
                    jobs: list[dict],
                ) -> None:
                    queue = enrich_queues[site]
                    jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
                    log_app(
                        f"{site}: fetching details for {len(jobs)} jobs (streaming, newest first)..."
                    )
                    if initial_sleep:
                        await asyncio.sleep(initial_sleep)
                    for i, job in enumerate(jobs[:enrich_limit]):
                        cd = cooldown if i > 0 else 0.0
                        try:
                            ok = await loop.run_in_executor(
                                _executor, detail_fn, job, cd
                            )
                        except Exception as e:
                            log_app(f"{site} detail error job {i}: {e}", "ERROR")
                            ok = True
                        job["skills"] = extract_skills(
                            job.get("title", ""), job.get("description", "")
                        )
                        await queue.put(job)
                        if site == "linkedin" and not ok:
                            log_app("linkedin: rate-limited — stopping detail fetch")
                            for remaining in jobs[i + 1 : enrich_limit]:
                                remaining["skills"] = extract_skills(
                                    remaining.get("title", ""),
                                    remaining.get("description", ""),
                                )
                                await queue.put(remaining)
                            break
                    await queue.put(None)
                    log_app(f"{site}: finished streaming details")

                while pending:
                    time_left = max(0.1, deadline - loop.time())
                    done, pending = await asyncio.wait(
                        pending,
                        return_when=asyncio.FIRST_COMPLETED,
                        timeout=min(15.0, time_left),
                    )
                    if not done:
                        if loop.time() >= deadline:
                            for fut in pending:
                                fut.cancel()
                            log_app(
                                f"stream timeout — cancelling {len(pending)} scraper(s)"
                            )
                            break
                        yield ": keepalive\n\n"
                        continue

                    for fut in done:
                        site = futures[fut]
                        try:
                            result = fut.result()
                        except Exception as e:
                            log_app(f"{site} scraper error: {e}", "ERROR")
                            result = []
                        filtered = _process(result)
                        # Deduplicate against jobs already sent via vector prefill
                        if vector_seen_links:
                            filtered = [
                                j
                                for j in filtered
                                if str(j.get("link", "")) not in vector_seen_links
                            ]
                        all_jobs.extend(filtered)
                        cfg = next((c for c in _ENRICH_CFG if c[0] == site), None)
                        if cfg is not None:
                            yield f"event: {site}-enriching\ndata: {json.dumps({'count': len(filtered)})}\n\n"
                            enrich_tasks[site] = asyncio.create_task(
                                _site_phase2(site, cfg[1], cfg[2], cfg[3], filtered)
                            )
                        if filtered:
                            yield f"data: {json.dumps(filtered, ensure_ascii=False)}\n\n"

                    # Drain any ready enriched items while waiting for more scrapers
                    for site_name, queue in enrich_queues.items():
                        while not queue.empty():
                            item = queue.get_nowait()
                            if item is None:
                                enrich_tasks[site_name] = None
                            else:
                                yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                            if enrich_tasks[site_name] is None:
                                break

                yield "event: done\ndata: {}\n\n"

                # Drain remaining enriched items after all scrapers have finished
                for site_name, task in list(enrich_tasks.items()):
                    if task is None:
                        continue
                    queue = enrich_queues[site_name]
                    enriched_count = 0
                    while True:
                        item = await queue.get()
                        if item is None:
                            break
                        enriched_count += 1
                        yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                    yield f"event: {site_name}-done\ndata: {json.dumps({'count': enriched_count})}\n\n"

                # Deduplicate before persisting/upserting (cache prefill + live scrape may overlap).
                # For jobsgo/vietnamworks, posted_date is derived from relative text ("3 days ago")
                # and resets on every scrape. Preserve the oldest known posted_ts for these sources.
                _RELATIVE_DATE_SOURCES = {
                    "JobsGo",
                    "VietnamWorks",
                    "TopDev",
                    "CareerViet",
                    "CareerLink",
                }
                by_link: dict[str, dict] = {}
                for job in all_jobs:
                    link = job.get("link")
                    if not link:
                        continue
                    if link in by_link and job.get("source") in _RELATIVE_DATE_SOURCES:
                        prev_ts = by_link[link].get("posted_ts", 0.0)
                        new_ts = job.get("posted_ts", 0.0)
                        if prev_ts > 0 and (new_ts == 0 or prev_ts < new_ts):
                            job = {
                                **job,
                                "posted_ts": prev_ts,
                                "posted_date": by_link[link].get(
                                    "posted_date", job.get("posted_date", "")
                                ),
                            }
                    by_link[link] = job
                all_jobs = list(by_link.values())
                all_jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)

                if not is_warmup:
                    seen_links_for_supplement = {
                        str(j["link"]) for j in all_jobs if j.get("link")
                    }
                    vector_query = req.raw_input.strip() or keyword
                    vector_supplement = await _fetch_vector_supplement(
                        vector_query,
                        seen_links_for_supplement,
                        req.location,
                        warmup_kws,
                    )
                    if vector_supplement:
                        _refresh_posted_times(vector_supplement)
                        yield f"event: vector-results\ndata: {json.dumps({'jobs': vector_supplement, 'count': len(vector_supplement)}, ensure_ascii=False)}\n\n"
                        all_jobs = all_jobs + vector_supplement
                        log_app(
                            f"[vector] appended {len(vector_supplement)} related jobs for {keyword!r}"
                        )
                await cache_preserve_posted_dates(cache_keyword, req.location, all_jobs)
                await cache_set(cache_keyword, req.location, all_jobs, fetch_ts)
                if not is_warmup:
                    try:
                        await cache_touch(cache_keyword, req.location)
                        await vector_mark_nonwarmup_seen(
                            [str(j["link"]) for j in all_jobs if j.get("link")],
                            time.time(),
                        )
                    except Exception as e:
                        log_app(
                            f"[scrape-stream] Redis error updating cache touch: {e}",
                            "ERROR",
                        )
        finally:
            ip_active_dec(ip)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
