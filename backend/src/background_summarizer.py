# src/background_summarizer.py
"""Background summarization task that runs after scraping completes."""

import asyncio

from src.cache import get_redis, _key
from src.logger import log_app
from src.summarizer import get_summarizer


async def get_jobs_without_summary(keyword_filter: str | None = None) -> list[dict]:
    """Get all jobs that don't have summaries yet.

    keyword_filter: If set, only scan jobs for this keyword.
    """
    from src.warmup import _WARMUP_KEYWORDS_KEY, _WARMUP_LOCATIONS

    redis = get_redis()
    jobs_to_summarize = []

    try:
        warmup_keywords = await redis.smembers(_WARMUP_KEYWORDS_KEY)

        for kw in warmup_keywords:
            if keyword_filter and kw.lower() != keyword_filter.lower():
                continue
            for loc in _WARMUP_LOCATIONS:
                key = _key(kw, loc)
                raw = await redis.get(key)
                if not raw:
                    continue

                import json
                data = json.loads(raw)
                jobs = data.get("jobs", [])

                for job in jobs:
                    if not job.get("summary_description"):
                        jobs_to_summarize.append({
                            "keyword": kw,
                            "location": loc,
                            "job_id": job.get("link"),
                            "job": job,
                        })

        log_app(f"[summarizer] found {len(jobs_to_summarize)} jobs without summaries")
        return jobs_to_summarize

    except Exception as e:
        log_app(f"[summarizer] error getting jobs without summary: {e}", "ERROR")
        return []


async def update_job_summary(keyword: str, location: str, job_id: str, summary: str) -> bool:
    """Update a single job with its summary."""
    redis = get_redis()
    try:
        key = _key(keyword, location)
        raw = await redis.get(key)
        if not raw:
            return False

        import json
        data = json.loads(raw)
        jobs = data.get("jobs", [])

        updated = False
        for job in jobs:
            if job.get("link") == job_id:
                job["summary_description"] = summary
                updated = True
                break

        if updated:
            payload = json.dumps({"jobs": jobs, "fetched_ts": data["fetched_ts"]}, ensure_ascii=False)
            await redis.set(key, payload)
            return True

        return False

    except Exception as e:
        log_app(f"[summarizer] error updating job summary: {e}", "ERROR")
        return False


async def summarize_pending_jobs(batch_size: int = 25, keyword_filter: str | None = None) -> dict[str, int]:
    """Summarize all jobs that don't have summaries yet.

    batch_size: Number of jobs to process per API call (default: 25)
    keyword_filter: If set, only summarize jobs for this keyword.
    """
    summarizer = get_summarizer()
    jobs_to_summarize = await get_jobs_without_summary(keyword_filter=keyword_filter)

    if not jobs_to_summarize:
        log_app("[summarizer] no jobs to summarize")
        return {"processed": 0, "success": 0, "failed": 0}

    stats = {"processed": 0, "success": 0, "skipped": 0, "failed": 0}

    for i in range(0, len(jobs_to_summarize), batch_size):
        batch = jobs_to_summarize[i:i + batch_size]
        descriptions = [item["job"].get("description", "") for item in batch]
        batch_num = i // batch_size + 1

        titles = [item["job"].get("title", "?") for item in batch]
        skipped = sum(1 for d in descriptions if not d or len(d) < 50)
        log_app(f"[summarizer] processing batch {batch_num}/{(len(jobs_to_summarize) + batch_size - 1) // batch_size}: {len(descriptions)} jobs ({skipped} empty) — {titles[:5]}{'...' if len(titles) > 5 else ''}")

        try:
            summaries = summarizer.batch_summarize(descriptions)

            for item, summary in zip(batch, summaries):
                stats["processed"] += 1

                # Skip empty summaries (API timeout/failure) — will retry next cycle
                if not summary:
                    stats["skipped"] += 1
                    continue

                success = await update_job_summary(
                    item["keyword"],
                    item["location"],
                    item["job_id"],
                    summary
                )

                if success:
                    stats["success"] += 1
                else:
                    stats["failed"] += 1

        except Exception as e:
            log_app(f"[summarizer] batch error: {e}", "ERROR")
            stats["failed"] += len(batch)

        if i + batch_size < len(jobs_to_summarize):
            await asyncio.sleep(2.0)

    log_app(f"[summarizer] batch complete: {stats}")
    return stats


async def run_background_summarization(keyword_filter: str | None = None) -> dict[str, int]:
    """Main entry point for background summarization."""
    if keyword_filter:
        log_app(f"[summarizer] starting background summarization (keyword={keyword_filter!r})...")
    else:
        log_app("[summarizer] starting background summarization...")

    stats = await summarize_pending_jobs(keyword_filter=keyword_filter)

    log_app(f"[summarizer] background summarization complete: {stats}")
    return stats
