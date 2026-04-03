import asyncio
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from src.cache import cache_get, cache_set, cache_fuzzy_get, cache_touch, embedded_links_add, embedded_links_filter, get_redis, vector_mark_nonwarmup_seen
from src.constants import MAX_CONCURRENT
from src.logger import log_search, log_app
from src.matching import title_matches, title_matches_loose, extract_skills, posted_ts, posted_relative, strip_level, correct_keyword_typos, normalize_keyword
from src.models import Job, ScrapeRequest
from src.ratelimit import check_rate_limit, ip_active_inc, ip_active_dec
from src.scrapers import *
from src.vector import ensure_index, upsert_jobs, search as vector_search, rerank_jobs_by_vector
from src.warmup import warmup, _WARMUP_LOCATIONS, _WARMUP_KEYWORDS, _scrape_keyword, get_warmup_keywords, add_warmup_keyword, remove_warmup_keyword


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the background warmup task on application startup."""
    log_app("Application starting...")
    asyncio.create_task(warmup(_executor, _SCRAPERS))
    log_app("Warmup task scheduled")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, ensure_index)
    yield
    log_app("Application shutting down")


app = FastAPI(title="Job Scraper API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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


async def _upsert_and_track(jobs: list[dict]) -> None:
    """Embed and upsert only jobs not already in Vectorize, then mark them as embedded."""
    loop = asyncio.get_event_loop()
    unembedded = await embedded_links_filter(jobs)
    if not unembedded:
        return
    ok = await loop.run_in_executor(_executor, upsert_jobs, unembedded)
    if ok:
        await embedded_links_add([j["link"] for j in unembedded if j.get("link")])
        log_app(f"[vector] tracked {len(unembedded)} newly embedded links")


async def _fetch_vector_supplement(query: str, seen_links: set[str], location: str, warmup_keywords: list[str], top_k: int = 20) -> list[dict]:
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

    target = {m["link"]: m["score"] for m in matches if m.get("link") and m["link"] not in seen_links}
    if not target:
        return []

    redis = get_redis()
    hydrated: list[dict] = []

    async def _hydrate_from_key(key: str) -> None:
        if not target:
            return
        raw = await redis.get(key)
        if not raw:
            return
        try:
            data = json.loads(raw)
            for job in data.get("jobs", []):
                link = job.get("link", "")
                if link in target:
                    job["_vector_score"] = target.pop(link)
                    job["_from_vector"] = True
                    hydrated.append(job)
        except Exception:
            pass

    for kw in warmup_keywords:
        await _hydrate_from_key(f"jobs:{kw.lower()}:{location.lower()}")

    if target:
        loc_pattern = f"jobs:*:{location.lower()}"
        warmup_keys = {f"jobs:{kw.lower()}:{location.lower()}" for kw in warmup_keywords}
        async for key in redis.scan_iter(loc_pattern):
            key_str = key.decode() if isinstance(key, bytes) else key
            if key_str not in warmup_keys:
                await _hydrate_from_key(key_str)
            if not target:
                break

    hydrated.sort(key=lambda j: j["_vector_score"], reverse=True)
    return hydrated


_SCRAPERS = {
    "linkedin":     scrape_linkedin,
    "itviec":       scrape_itviec,
    "topcv":        scrape_topcv,
    "vietnamworks": scrape_vietnamworks,
    "topdev":       scrape_topdev,
    # "indeed":       scrape_indeed,
    "careerviet":   scrape_careerviet,
    "jobsgo":       scrape_jobsgo,
    "careerlink":   scrape_careerlink,
}

NON_WARMUP_ENRICH_LIMIT = 5
_active_bg_rescrapes: set[str] = set()


def _is_warmup_keyword(keyword: str) -> bool:
    """Return True if keyword (case-insensitive) is in the hardcoded warmup list."""
    return keyword.lower().strip() in {kw.lower().strip() for kw in _WARMUP_KEYWORDS}


async def _background_rescrape(keyword: str, location: str, last_fetched_ts: float) -> None:
    """Background re-scrape for a non-warmup cache hit.

    Runs the full scrape+enrich cycle using the existing _scrape_keyword() from warmup,
    but caps description enrichment at NON_WARMUP_ENRICH_LIMIT per site.
    Uses the warmup semaphore to avoid overloading the executor.
    """
    key = f"{keyword.lower()}:{location.lower()}"
    if key in _active_bg_rescrapes:
        log_app(f"[bg-rescrape] already in progress for {keyword!r}/{location!r}, skipping")
        return
    _active_bg_rescrapes.add(key)
    sem = _get_warmup_sem()
    async with sem:
        loop = asyncio.get_event_loop()
        try:
            log_app(f"[bg-rescrape] starting for {keyword!r}/{location!r}")
            await _scrape_keyword(
                keyword, location, loop, _executor, _SCRAPERS,
                last_fetched_ts=last_fetched_ts,
                enrich_limit=NON_WARMUP_ENRICH_LIMIT,
            )
        except Exception as e:
            log_app(f"[bg-rescrape] error for {keyword!r}/{location!r}: {e}", "ERROR")
        finally:
            _active_bg_rescrapes.discard(key)


@app.get("/")
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
            keys.append({"keyword": kw, "location": loc, "missing": is_missing,
                         "stale": is_stale, "fetched_ago": fetched_ago, "job_count": job_count})
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


@app.get("/stats")
async def stats():
    """Return total unique jobs posted in the last 7 days across all cached keys."""
    cutoff = time.time() - 7 * 86400
    seen_links: set[str] = set()
    count = 0
    try:
        async for key in get_redis().scan_iter("jobs:*"):
            raw = await get_redis().get(key)
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
async def cache_scrape(keyword: str | None = None, location: str | None = None):
    """Trigger a background scrape. Optionally filter by keyword and/or location.

    Examples:
      /cache/scrape
      /cache/scrape?keyword=AI Engineer
      /cache/scrape?keyword=AI Engineer&location=Ho Chi Minh City
    """
    loop = asyncio.get_event_loop()
    status = await _cache_status_data()

    keys_to_scrape = [
        k for k in status["keys"]
        if (keyword is None or k["keyword"].lower() == keyword.lower())
        and (location is None or k["location"].lower() == location.lower())
    ]

    if not keys_to_scrape:
        return {"triggered": 0, "message": "no matching keys found"}

    async def _run() -> None:
        for k in keys_to_scrape:
            kw, loc = k["keyword"], k["location"]
            existing = await cache_get(kw, loc)
            fetched_ts = existing[1] if existing else 0.0
            try:
                await _scrape_keyword(kw, loc, loop, _executor, _SCRAPERS, last_fetched_ts=fetched_ts)
            except Exception as e:
                log_app(f"[cache/scrape] error for {kw!r}/{loc!r}: {e}", "ERROR")

    asyncio.create_task(_run())
    triggered = [{"keyword": k["keyword"], "location": k["location"]} for k in keys_to_scrape]
    return {"triggered": len(triggered), "keys": triggered}


@app.get("/warmup/keywords")
async def list_warmup_keywords():
    """List all current warmup keywords."""
    keywords = await get_warmup_keywords()
    return {"keywords": keywords, "count": len(keywords)}


@app.post("/scrape", response_model=list[Job])
async def scrape(req: ScrapeRequest, request: Request):
    """Scrape jobs for a keyword and location, returning all results as a JSON array."""
    keyword = req.keyword.strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="keyword is required")
    log_search(request, keyword, req.location)

    warmup_kws = await get_warmup_keywords()
    keyword_corrected = correct_keyword_typos(keyword, warmup_kws)
    if keyword_corrected != keyword.lower():
        log_app(f"typo correction: {keyword!r} → {keyword_corrected!r}")

    keyword_normalized = " ".join(re.sub(r"\d+", " ", normalize_keyword(keyword_corrected)).split())
    cache_keyword = strip_level(keyword_normalized)

    all_cached_jobs: list[dict] = []
    related_keywords = _get_related_keywords(cache_keyword)
    for related_kw in related_keywords:
        cached = await cache_get(related_kw, req.location)
        if cached:
            cached_jobs, _ = cached
            all_cached_jobs.extend(cached_jobs)

    if all_cached_jobs:
        log_app(f"cache hit — {len(all_cached_jobs)} jobs from {len(related_keywords)} related keywords (/scrape)")
        all_cached_jobs_by_link = {j.get("link"): j for j in all_cached_jobs}
        unique_jobs = list(all_cached_jobs_by_link.values())
        unique_jobs = [j for j in unique_jobs if title_matches(j.get("title", ""), keyword)]
        log_app(f"level filter: {keyword!r} → {cache_keyword!r}, {len(unique_jobs)} jobs after filtering")
        _refresh_posted_times(unique_jobs)
        return unique_jobs

    loop = asyncio.get_event_loop()

    def _timed(site: str, fn, kw: str, loc: str):
        log_app(f"{site} scraper starting")
        t0 = time.perf_counter()
        result = fn(kw, loc)
        elapsed = time.perf_counter() - t0
        log_app(f"{site} scraper done in {elapsed:.1f}s — {len(result)} jobs")
        return result

    tasks = [
        loop.run_in_executor(_executor, _timed, site, fn, cache_keyword, req.location)
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

    await cache_set(cache_keyword, req.location, jobs, time.time())
    asyncio.create_task(_upsert_and_track(jobs))
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
    rate_err = check_rate_limit(ip)
    if rate_err:
        raise HTTPException(status_code=429, detail=rate_err)

    log_search(request, keyword, req.location)

    warmup_kws = await get_warmup_keywords()
    keyword_corrected = correct_keyword_typos(keyword, warmup_kws)
    if keyword_corrected != keyword.lower():
        log_app(f"typo correction: {keyword!r} → {keyword_corrected!r}")

    keyword_normalized = " ".join(re.sub(r"\d+", " ", normalize_keyword(keyword_corrected)).split())
    cache_keyword = strip_level(keyword_normalized)

    loop = asyncio.get_event_loop()
    is_warmup = any(cache_keyword.lower().strip() == kw.lower().strip() for kw in warmup_kws)

    def _process(jobs: list[dict]) -> list[dict]:
        filtered = []
        for j in jobs:
            if is_warmup and not title_matches(j.get("title", ""), keyword):
                continue
            if not is_warmup and not title_matches_loose(j.get("title", ""), keyword):
                continue
            j["posted_ts"] = posted_ts(j)
            j["posted"] = posted_relative(j["posted_ts"])
            j["skills"] = extract_skills(j.get("title", ""), j.get("description", ""))
            filtered.append(j)
        return filtered

    def _timed(site: str, fn, kw: str, loc: str):
        log_app(f"{site} scraper starting")
        t0 = time.perf_counter()
        result = fn(kw, loc)
        elapsed = time.perf_counter() - t0
        log_app(f"{site} scraper done in {elapsed:.1f}s — {len(result)} jobs")
        return result

    async def event_generator() -> AsyncGenerator[str, None]:
        global _queue_count
        sem = _get_sem()
        ip_active_inc(ip)
        try:
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

            if all_cached_jobs:
                log_app(f"cache hit — {len(all_cached_jobs)} jobs from {len(related_keywords)} related keywords")
                all_cached_jobs_by_link = {j.get("link"): j for j in all_cached_jobs}
                unique_jobs = list(all_cached_jobs_by_link.values())
                unique_jobs = [j for j in unique_jobs if title_matches(j.get("title", ""), keyword)]
                age_cutoff = time.time() - 8 * 86400
                unique_jobs = [j for j in unique_jobs if j.get("posted_ts", 0) >= age_cutoff]
                log_app(f"level filter: {keyword!r} → {cache_keyword!r}, {len(unique_jobs)} jobs after filtering")
                _refresh_posted_times(unique_jobs)
                latest_ts = max(cache_fetched_ts_list) if cache_fetched_ts_list else 0
                yield f"event: cached\ndata: {json.dumps({'jobs': unique_jobs, 'fetched_ts': latest_ts, 'fuzzy': False}, ensure_ascii=False)}\n\n"

                if is_warmup:
                    yield "event: done\ndata: {}\n\n"
                    seen_links = {str(j["link"]) for j in unique_jobs if j.get("link")}
                    vector_supplement = await _fetch_vector_supplement(keyword, seen_links, req.location, warmup_kws)
                    if vector_supplement:
                        yield f"event: vector-results\ndata: {json.dumps({'jobs': vector_supplement, 'count': len(vector_supplement)}, ensure_ascii=False)}\n\n"
                    return

                # Non-warmup cache hit: show cached jobs immediately, then scrape fresh inline.
                await cache_touch(cache_keyword, req.location)
                await vector_mark_nonwarmup_seen(
                    [str(j["link"]) for j in unique_jobs if j.get("link")], time.time()
                )
                cached_prefill_jobs = unique_jobs
                cached_prefill_latest_ts = latest_ts

            fuzzy = await cache_fuzzy_get(cache_keyword, req.location)
            if fuzzy:
                fuzzy_jobs, fuzzy_fetched_ts, fuzzy_matched_kw = fuzzy
                age_cutoff_fuzzy = time.time() - 8 * 86400
                refiltered = [j for j in fuzzy_jobs if title_matches(j.get("title", ""), keyword) and j.get("posted_ts", 0) >= age_cutoff_fuzzy]
                if refiltered:
                    log_app(f"cache fuzzy — streaming {len(refiltered)} jobs for {keyword!r}, done")
                    _refresh_posted_times(refiltered)
                    yield f"event: cached\ndata: {json.dumps({'jobs': refiltered, 'fetched_ts': fuzzy_fetched_ts, 'fuzzy': True}, ensure_ascii=False)}\n\n"
                    if is_warmup:
                        yield "event: done\ndata: {}\n\n"
                        return
                    cached_prefill_jobs = cached_prefill_jobs or refiltered
                    cached_prefill_latest_ts = cached_prefill_latest_ts or fuzzy_fetched_ts
                else:
                    log_app(f"cache fuzzy — 0 jobs matched {keyword!r} after re-filter, skipping fuzzy")

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
                other_scrapers = {k: v for k, v in _SCRAPERS.items() if k != "linkedin"}
                futures: dict = {
                    loop.run_in_executor(_executor, _timed, site, fn, scrape_kw, req.location): site
                    for site, fn in other_scrapers.items()
                }
                linkedin_fut = loop.run_in_executor(
                    _executor, _timed, "linkedin", scrape_linkedin, scrape_kw, req.location
                )
                futures[linkedin_fut] = "linkedin"

                linkedin_jobs: list[dict] = []
                topcv_jobs: list[dict] = []
                itviec_jobs: list[dict] = []
                topdev_jobs: list[dict] = []
                jobsgo_jobs: list[dict] = []
                careerlink_jobs: list[dict] = []
                all_jobs: list[dict] = list(cached_prefill_jobs) if cached_prefill_jobs else []
                pending = set(futures.keys())
                deadline = loop.time() + 120.0

                enrich_queue: asyncio.Queue = asyncio.Queue()
                enrich_task: asyncio.Task | None = None
                topcv_enrich_queue: asyncio.Queue = asyncio.Queue()
                topcv_enrich_task: asyncio.Task | None = None
                itviec_enrich_queue: asyncio.Queue = asyncio.Queue()
                itviec_enrich_task: asyncio.Task | None = None
                topdev_enrich_queue: asyncio.Queue = asyncio.Queue()
                topdev_enrich_task: asyncio.Task | None = None
                jobsgo_enrich_queue: asyncio.Queue = asyncio.Queue()
                jobsgo_enrich_task: asyncio.Task | None = None
                careerlink_enrich_queue: asyncio.Queue = asyncio.Queue()
                careerlink_enrich_task: asyncio.Task | None = None

                enrich_limit = NON_WARMUP_ENRICH_LIMIT if not is_warmup else 30

                async def _phase2(jobs: list[dict]) -> None:
                    jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
                    count = len(jobs)
                    log_app(f"linkedin: fetching descriptions for {count} jobs (streaming, newest first)...")
                    await asyncio.sleep(10.0)
                    for i, job in enumerate(jobs[:enrich_limit]):
                        cooldown = 3.0 if i > 0 else 0.0
                        try:
                            ok = await loop.run_in_executor(
                                _executor, scrape_linkedin_detail_one, job, cooldown
                            )
                        except Exception as e:
                            log_app(f"linkedin detail error job {i}: {e}", "ERROR")
                            ok = True
                        job["skills"] = extract_skills(job.get("title", ""), job.get("description", ""))
                        await enrich_queue.put(job)
                        if not ok:
                            log_app("linkedin: rate-limited — stopping detail fetch")
                            for remaining in jobs[i + 1:enrich_limit]:
                                remaining["skills"] = extract_skills(remaining.get("title", ""), remaining.get("description", ""))
                                await enrich_queue.put(remaining)
                            break
                    await enrich_queue.put(None)
                    log_app("linkedin: finished streaming descriptions")

                async def _topcv_phase2(jobs: list[dict]) -> None:
                    jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
                    log_app(f"topcv: fetching details for {len(jobs)} jobs (streaming, newest first)...")
                    for i, job in enumerate(jobs[:enrich_limit]):
                        cooldown = 2.0 if i > 0 else 0.0
                        try:
                            await loop.run_in_executor(
                                _executor, scrape_topcv_detail_one, job, cooldown
                            )
                        except Exception as e:
                            log_app(f"topcv detail error job {i}: {e}", "ERROR")
                        job["skills"] = extract_skills(job.get("title", ""), job.get("description", ""))
                        await topcv_enrich_queue.put(job)
                    await topcv_enrich_queue.put(None)
                    log_app("topcv: finished streaming details")

                async def _itviec_phase2(jobs: list[dict]) -> None:
                    jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
                    log_app(f"itviec: fetching details for {len(jobs)} jobs (streaming, newest first)...")
                    for i, job in enumerate(jobs[:enrich_limit]):
                        cooldown = 2.0 if i > 0 else 0.0
                        try:
                            await loop.run_in_executor(
                                _executor, scrape_itviec_detail_one, job, cooldown
                            )
                        except Exception as e:
                            log_app(f"itviec detail error job {i}: {e}", "ERROR")
                        job["skills"] = extract_skills(job.get("title", ""), job.get("description", ""))
                        await itviec_enrich_queue.put(job)
                    await itviec_enrich_queue.put(None)
                    log_app("itviec: finished streaming details")

                async def _topdev_phase2(jobs: list[dict]) -> None:
                    jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
                    log_app(f"topdev: fetching details for {len(jobs)} jobs (streaming, newest first)...")
                    for i, job in enumerate(jobs[:enrich_limit]):
                        cooldown = 2.0 if i > 0 else 0.0
                        try:
                            await loop.run_in_executor(
                                _executor, scrape_topdev_detail_one, job, cooldown
                            )
                        except Exception as e:
                            log_app(f"topdev detail error job {i}: {e}", "ERROR")
                        job["skills"] = extract_skills(job.get("title", ""), job.get("description", ""))
                        await topdev_enrich_queue.put(job)
                    await topdev_enrich_queue.put(None)
                    log_app("topdev: finished streaming details")

                async def _careerlink_phase2(jobs: list[dict]) -> None:
                    jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
                    log_app(f"careerlink: fetching details for {len(jobs)} jobs (streaming, newest first)...")
                    for i, job in enumerate(jobs[:enrich_limit]):
                        cooldown = 2.0 if i > 0 else 0.0
                        try:
                            await loop.run_in_executor(
                                _executor, scrape_careerlink_detail_one, job, cooldown
                            )
                        except Exception as e:
                            log_app(f"careerlink detail error job {i}: {e}", "ERROR")
                        job["skills"] = extract_skills(job.get("title", ""), job.get("description", ""))
                        await careerlink_enrich_queue.put(job)
                    await careerlink_enrich_queue.put(None)
                    log_app("careerlink: finished streaming details")

                async def _jobsgo_phase2(jobs: list[dict]) -> None:
                    jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
                    log_app(f"jobsgo: fetching details for {len(jobs)} jobs (streaming, newest first)...")
                    for i, job in enumerate(jobs[:enrich_limit]):
                        cooldown = 2.0 if i > 0 else 0.0
                        try:
                            await loop.run_in_executor(
                                _executor, scrape_jobsgo_detail_one, job, cooldown
                            )
                        except Exception as e:
                            log_app(f"jobsgo detail error job {i}: {e}", "ERROR")
                        job["skills"] = extract_skills(job.get("title", ""), job.get("description", ""))
                        await jobsgo_enrich_queue.put(job)
                    await jobsgo_enrich_queue.put(None)
                    log_app("jobsgo: finished streaming details")

                while pending:
                    time_left = max(0.1, deadline - loop.time())
                    done, pending = await asyncio.wait(
                        pending, return_when=asyncio.FIRST_COMPLETED, timeout=min(15.0, time_left)
                    )
                    if not done:
                        if loop.time() >= deadline:
                            for fut in pending:
                                fut.cancel()
                            log_app(f"stream timeout — cancelling {len(pending)} scraper(s)")
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
                        all_jobs.extend(filtered)
                        if site == "linkedin":
                            linkedin_jobs = filtered
                            yield f"event: linkedin-enriching\ndata: {json.dumps({'count': len(linkedin_jobs)})}\n\n"
                            enrich_task = asyncio.create_task(_phase2(linkedin_jobs))
                        if site == "topcv":
                            topcv_jobs = filtered
                            yield f"event: topcv-enriching\ndata: {json.dumps({'count': len(topcv_jobs)})}\n\n"
                            topcv_enrich_task = asyncio.create_task(_topcv_phase2(topcv_jobs))
                        if site == "itviec":
                            itviec_jobs = filtered
                            yield f"event: itviec-enriching\ndata: {json.dumps({'count': len(itviec_jobs)})}\n\n"
                            itviec_enrich_task = asyncio.create_task(_itviec_phase2(itviec_jobs))
                        if site == "topdev":
                            topdev_jobs = filtered
                            yield f"event: topdev-enriching\ndata: {json.dumps({'count': len(topdev_jobs)})}\n\n"
                            topdev_enrich_task = asyncio.create_task(_topdev_phase2(topdev_jobs))
                        if site == "jobsgo":
                            jobsgo_jobs = filtered
                            yield f"event: jobsgo-enriching\ndata: {json.dumps({'count': len(jobsgo_jobs)})}\n\n"
                            jobsgo_enrich_task = asyncio.create_task(_jobsgo_phase2(jobsgo_jobs))
                        if site == "careerlink":
                            careerlink_jobs = filtered
                            yield f"event: careerlink-enriching\ndata: {json.dumps({'count': len(careerlink_jobs)})}\n\n"
                            careerlink_enrich_task = asyncio.create_task(_careerlink_phase2(careerlink_jobs))
                        if filtered:
                            yield f"data: {json.dumps(filtered, ensure_ascii=False)}\n\n"

                    while not enrich_queue.empty():
                        item = enrich_queue.get_nowait()
                        if item is None:
                            enrich_task = None
                        else:
                            yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                        if enrich_task is None:
                            break
                    while not topcv_enrich_queue.empty():
                        item = topcv_enrich_queue.get_nowait()
                        if item is None:
                            topcv_enrich_task = None
                        else:
                            yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                        if topcv_enrich_task is None:
                            break
                    while not itviec_enrich_queue.empty():
                        item = itviec_enrich_queue.get_nowait()
                        if item is None:
                            itviec_enrich_task = None
                        else:
                            yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                        if itviec_enrich_task is None:
                            break
                    while not topdev_enrich_queue.empty():
                        item = topdev_enrich_queue.get_nowait()
                        if item is None:
                            topdev_enrich_task = None
                        else:
                            yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                        if topdev_enrich_task is None:
                            break
                    while not jobsgo_enrich_queue.empty():
                        item = jobsgo_enrich_queue.get_nowait()
                        if item is None:
                            jobsgo_enrich_task = None
                        else:
                            yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                        if jobsgo_enrich_task is None:
                            break
                    while not careerlink_enrich_queue.empty():
                        item = careerlink_enrich_queue.get_nowait()
                        if item is None:
                            careerlink_enrich_task = None
                        else:
                            yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                        if careerlink_enrich_task is None:
                            break

                yield "event: done\ndata: {}\n\n"

                if enrich_task is not None:
                    enriched_count = 0
                    while True:
                        item = await enrich_queue.get()
                        if item is None:
                            break
                        enriched_count += 1
                        yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                    yield f"event: linkedin-done\ndata: {json.dumps({'count': enriched_count})}\n\n"

                if topcv_enrich_task is not None:
                    enriched_count = 0
                    while True:
                        item = await topcv_enrich_queue.get()
                        if item is None:
                            break
                        enriched_count += 1
                        yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                    yield f"event: topcv-done\ndata: {json.dumps({'count': enriched_count})}\n\n"

                if itviec_enrich_task is not None:
                    enriched_count = 0
                    while True:
                        item = await itviec_enrich_queue.get()
                        if item is None:
                            break
                        enriched_count += 1
                        yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                    yield f"event: itviec-done\ndata: {json.dumps({'count': enriched_count})}\n\n"

                if topdev_enrich_task is not None:
                    enriched_count = 0
                    while True:
                        item = await topdev_enrich_queue.get()
                        if item is None:
                            break
                        enriched_count += 1
                        yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                    yield f"event: topdev-done\ndata: {json.dumps({'count': enriched_count})}\n\n"

                if jobsgo_enrich_task is not None:
                    enriched_count = 0
                    while True:
                        item = await jobsgo_enrich_queue.get()
                        if item is None:
                            break
                        enriched_count += 1
                        yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                    yield f"event: jobsgo-done\ndata: {json.dumps({'count': enriched_count})}\n\n"

                if careerlink_enrich_task is not None:
                    enriched_count = 0
                    while True:
                        item = await careerlink_enrich_queue.get()
                        if item is None:
                            break
                        enriched_count += 1
                        yield f"data: {json.dumps([item], ensure_ascii=False)}\n\n"
                    yield f"event: careerlink-done\ndata: {json.dumps({'count': enriched_count})}\n\n"

                # Deduplicate before persisting/upserting (cache prefill + live scrape may overlap).
                by_link = {j.get("link"): j for j in all_jobs if j.get("link")}
                all_jobs = list(by_link.values())
                all_jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)

                asyncio.create_task(_upsert_and_track(all_jobs))

                if not is_warmup:
                    seen_links_for_supplement = {str(j["link"]) for j in all_jobs if j.get("link")}
                    vector_supplement = await _fetch_vector_supplement(keyword, seen_links_for_supplement, req.location, warmup_kws)
                    if vector_supplement:
                        yield f"event: vector-results\ndata: {json.dumps({'jobs': vector_supplement, 'count': len(vector_supplement)}, ensure_ascii=False)}\n\n"
                        all_jobs = all_jobs + vector_supplement
                        log_app(f"[vector] appended {len(vector_supplement)} related jobs for {keyword!r}")
                await cache_set(cache_keyword, req.location, all_jobs, fetch_ts)
                if not is_warmup:
                    await cache_touch(cache_keyword, req.location)
                    await vector_mark_nonwarmup_seen(
                        [str(j["link"]) for j in all_jobs if j.get("link")], time.time()
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
