import asyncio
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from src.cache import cache_get, cache_set, cache_fuzzy_get
from src.constants import MAX_CONCURRENT
from src.logger import log_search, log_app
from src.matching import title_matches, extract_skills, posted_ts, posted_relative, strip_level
from src.models import Job, ScrapeRequest
from src.ratelimit import _KEYWORD_ALIASES, check_rate_limit, ip_active_inc, ip_active_dec
from src.scrapers import *
from src.warmup import warmup, _WARMUP_LOCATIONS, _scrape_keyword, get_warmup_keywords, add_warmup_keyword, remove_warmup_keyword


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the background warmup task on application startup."""
    log_app("Application starting...")
    asyncio.create_task(warmup(_get_sem, _executor, _SCRAPERS))
    log_app("Warmup task scheduled")
    yield
    log_app("Application shutting down")


app = FastAPI(title="Job Scraper API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_executor = ThreadPoolExecutor(max_workers=4)

_MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_SCRAPES", str(MAX_CONCURRENT)))
_scrape_sem: asyncio.Semaphore | None = None
_queue_count = 0


def _get_sem() -> asyncio.Semaphore:
    """Return the global scrape semaphore, creating it on first call."""
    global _scrape_sem
    if _scrape_sem is None:
        _scrape_sem = asyncio.Semaphore(_MAX_CONCURRENT)
    return _scrape_sem


def _refresh_posted_times(jobs: list[dict]) -> None:
    """Recalculate the 'posted' relative time string for all jobs from their posted_ts."""
    for j in jobs:
        if "posted_ts" in j:
            j["posted"] = posted_relative(j["posted_ts"])


def _get_related_keywords(cache_keyword: str) -> list[str]:
    """Return the canonical cache key. All alias jobs are stored in the canonical cache."""
    return [cache_keyword.lower()]


_SCRAPERS = {
    "linkedin":     scrape_linkedin,
    "itviec":       scrape_itviec,
    "topcv":        scrape_topcv,
    "vietnamworks": scrape_vietnamworks,
    # TODO: re-enable these once we have error handling and monitoring in place to catch scraper breakages faster
    # "topdev":       scrape_topdev,
    # "indeed":       scrape_indeed,
    "careerviet":   scrape_careerviet,
}


@app.get("/")
def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "good jobs"}


@app.get("/cache/status")
async def cache_status():
    """Report status of all cache keys and trigger scraping for any that are missing or stale (>2h)."""
    loop = asyncio.get_event_loop()
    now = time.time()
    STALE_THRESHOLD = 7200
    keys = []
    needs_scrape: list[tuple[str, str, float]] = []
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
            keys.append({"keyword": kw, "location": loc, "missing": is_missing,
                         "stale": is_stale, "fetched_ago": fetched_ago, "job_count": job_count})
            if is_stale:
                needs_scrape.append((kw, loc, fetched_ts))

    if needs_scrape and _get_sem()._value > 0:  # noqa: SLF001
        async def _scrape_stale() -> None:
            for kw, loc, fetched_ts in needs_scrape:
                try:
                    async with _get_sem():
                        await _scrape_keyword(kw, loc, loop, _executor, _SCRAPERS, last_fetched_ts=fetched_ts)
                except Exception as e:
                    log_app(f"cache/status scrape error for {kw!r}/{loc!r}: {e}", "ERROR")
        asyncio.create_task(_scrape_stale())

    return {
        "total": len(keys),
        "missing": sum(1 for k in keys if k["missing"]),
        "stale": len(needs_scrape),
        "triggered_scrape": len(needs_scrape) > 0,
        "keys": keys,
    }


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

    cache_keyword = _KEYWORD_ALIASES.get(keyword.lower(), strip_level(keyword))

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
    _refresh_posted_times(jobs)
    return jobs


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

    cache_keyword = _KEYWORD_ALIASES.get(keyword.lower(), strip_level(keyword))

    loop = asyncio.get_event_loop()

    def _process(jobs: list[dict]) -> list[dict]:
        filtered = []
        for j in jobs:
            if title_matches(j.get("title", ""), keyword):
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
                log_app(f"level filter: {keyword!r} → {cache_keyword!r}, {len(unique_jobs)} jobs after filtering")
                _refresh_posted_times(unique_jobs)
                latest_ts = max(cache_fetched_ts_list) if cache_fetched_ts_list else 0
                yield f"event: cached\ndata: {json.dumps({'jobs': unique_jobs, 'fetched_ts': latest_ts, 'fuzzy': False}, ensure_ascii=False)}\n\n"
                yield "event: done\ndata: {}\n\n"
                return

            fuzzy = await cache_fuzzy_get(cache_keyword, req.location)
            if fuzzy:
                fuzzy_jobs, fuzzy_fetched_ts, fuzzy_matched_kw = fuzzy
                refiltered = [j for j in fuzzy_jobs if title_matches(j.get("title", ""), keyword)]
                if refiltered:
                    log_app(f"cache fuzzy — streaming {len(refiltered)} jobs for {keyword!r}, done")
                    _refresh_posted_times(refiltered)
                    yield f"event: cached\ndata: {json.dumps({'jobs': refiltered, 'fetched_ts': fuzzy_fetched_ts, 'fuzzy': True}, ensure_ascii=False)}\n\n"
                    yield "event: done\ndata: {}\n\n"
                    return
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

                scrape_kw = fuzzy_matched_kw if fuzzy else cache_keyword
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
                all_jobs: list[dict] = []
                pending = set(futures.keys())
                deadline = loop.time() + 120.0

                enrich_queue: asyncio.Queue = asyncio.Queue()
                enrich_task: asyncio.Task | None = None
                topcv_enrich_queue: asyncio.Queue = asyncio.Queue()
                topcv_enrich_task: asyncio.Task | None = None
                itviec_enrich_queue: asyncio.Queue = asyncio.Queue()
                itviec_enrich_task: asyncio.Task | None = None

                async def _phase2(jobs: list[dict]) -> None:
                    jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
                    count = len(jobs)
                    log_app(f"linkedin: fetching descriptions for {count} jobs (streaming, newest first)...")
                    await asyncio.sleep(10.0)
                    for i, job in enumerate(jobs[:30]):
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
                            for remaining in jobs[i + 1:30]:
                                remaining["skills"] = extract_skills(remaining.get("title", ""), remaining.get("description", ""))
                                await enrich_queue.put(remaining)
                            break
                    await enrich_queue.put(None)
                    log_app("linkedin: finished streaming descriptions")

                async def _topcv_phase2(jobs: list[dict]) -> None:
                    jobs.sort(key=lambda j: j.get("posted_ts", 0.0), reverse=True)
                    log_app(f"topcv: fetching details for {len(jobs)} jobs (streaming, newest first)...")
                    for i, job in enumerate(jobs):
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
                    for i, job in enumerate(jobs):
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

                await cache_set(cache_keyword, req.location, all_jobs, fetch_ts)
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
