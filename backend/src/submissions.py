"""Job-submissions store: headhunters POST a job, it waits in a pending
queue for manual review, and approved jobs are searchable like any scraper.

Storage is three Redis hashes (id -> JSON): submissions:pending,
submissions:approved, submissions:rejected. No database, no email service —
see docs/superpowers/specs/2026-09-05-job-submissions-design.md.
"""

import html
import json
import time
import unicodedata
import uuid
from datetime import datetime, timezone

from .cache import get_redis
from .constants import RECENT_DAYS, SUBMISSION_RATE_LIMIT, SUBMISSION_RATE_WINDOW_SECS
from .logger import log_app
from .matching import title_matches_loose

_PENDING_KEY = "submissions:pending"
_APPROVED_KEY = "submissions:approved"
_REJECTED_KEY = "submissions:rejected"
_RATE_KEY_PREFIX = "ratelimit:submit:"

_REQUIRED_FIELDS = ("title", "company", "location", "description")

_MAX_LENGTHS = {
    "title": 200,
    "company": 200,
    "location": 200,
    "description": 10000,
    "salary": 200,
}
_MAX_SKILLS = 20


def _validate(data: dict) -> None:
    for field in _REQUIRED_FIELDS:
        if not str(data.get(field, "")).strip():
            raise ValueError(f"{field} is required")
    for field, max_len in _MAX_LENGTHS.items():
        if len(str(data.get(field, ""))) > max_len:
            raise ValueError(f"{field} must be at most {max_len} characters")
    skills = data.get("skills", [])
    if isinstance(skills, list) and len(skills) > _MAX_SKILLS:
        raise ValueError(f"skills must have at most {_MAX_SKILLS} entries")
    apply_link = str(data.get("apply_link", "")).strip()
    if apply_link and not (apply_link.startswith("http://") or apply_link.startswith("https://")):
        raise ValueError("apply_link must start with http:// or https://")
    if not apply_link and not str(data.get("contact_email", "")).strip():
        raise ValueError("apply_link or contact_email is required")


def validate_submission(data: dict) -> None:
    """Public entry point for validating submission data without side
    effects — lets callers validate before other gates (e.g. rate limiting)
    run. Raises ValueError with the same messages as create_submission."""
    _validate(data)


async def create_submission(data: dict, submitter_ip: str = "") -> str:
    """Validate and store a new pending submission. Raises ValueError on invalid input."""
    _validate(data)
    try:
        sub_id = uuid.uuid4().hex
        raw_skills = data.get("skills", [])
        skills = [s.strip() for s in raw_skills if str(s).strip()] if isinstance(raw_skills, list) else []
        record = {
            "id": sub_id,
            "title": data["title"].strip(),
            "company": data["company"].strip(),
            "location": data["location"].strip(),
            "description": html.escape(data["description"].strip()).replace("\n", "<br>"),
            "apply_link": str(data.get("apply_link", "")).strip(),
            "contact_email": str(data.get("contact_email", "")).strip(),
            "skills": skills,
            "salary": str(data.get("salary", "")).strip(),
            "status": "pending",
            "submitted_ts": time.time(),
            "reviewed_ts": None,
            "submitter_ip": submitter_ip,
        }
        await get_redis().hset(_PENDING_KEY, sub_id, json.dumps(record, ensure_ascii=False))
        log_app(f"[submissions] new pending submission {sub_id!r}: {record['title']!r} @ {record['company']!r}")
        return sub_id
    except Exception as e:
        log_app(f"[submissions] create_submission error: {e}", "ERROR")
        raise


async def list_pending() -> list[dict]:
    """Return pending submissions, newest first."""
    try:
        raw = await get_redis().hgetall(_PENDING_KEY)
        records = [json.loads(v) for v in raw.values()]
        records.sort(key=lambda r: r.get("submitted_ts", 0.0), reverse=True)
        return records
    except Exception as e:
        log_app(f"[submissions] list_pending error: {e}", "ERROR")
        return []


async def _move(sub_id: str, from_key: str, to_key: str, new_status: str) -> bool:
    try:
        redis = get_redis()
        raw = await redis.hget(from_key, sub_id)
        if not raw:
            return False
        record = json.loads(raw)
        record["status"] = new_status
        record["reviewed_ts"] = time.time()
        await redis.hset(to_key, sub_id, json.dumps(record, ensure_ascii=False))
        await redis.hdel(from_key, sub_id)
        return True
    except Exception as e:
        log_app(f"[submissions] _move error: {e}", "ERROR")
        raise


async def approve(sub_id: str) -> bool:
    """Move a pending submission to approved. Returns False if id not found."""
    ok = await _move(sub_id, _PENDING_KEY, _APPROVED_KEY, "approved")
    if ok:
        log_app(f"[submissions] approved {sub_id!r}")
    return ok


async def reject(sub_id: str) -> bool:
    """Move a pending submission to rejected. Returns False if id not found."""
    ok = await _move(sub_id, _PENDING_KEY, _REJECTED_KEY, "rejected")
    if ok:
        log_app(f"[submissions] rejected {sub_id!r}")
    return ok


def _strip_diacritics(text: str) -> str:
    """Fold Vietnamese diacritics to plain ASCII (Hà Nội -> Ha Noi) so
    submitted locations match search input regardless of accent usage."""
    text = text.replace("đ", "d").replace("Đ", "D")
    return "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )


def _location_matches(job_location: str, requested_location: str) -> bool:
    """Loose free-text location match: substring either direction, and
    "remote" always matches. No VN-city dictionary — submitters type
    whatever city they mean, and volume is low enough that a simple
    heuristic plus manual review is enough for v1."""
    req = _strip_diacritics(requested_location.strip().lower())
    loc = _strip_diacritics(job_location.strip().lower())
    if not req or not loc:
        return True
    if "remote" in loc:
        return True
    return req in loc or loc in req


def _record_to_job(record: dict) -> dict:
    """Shape one approved submission record into the standard job-dict
    contract (source="Direct"). Downstream code (main.py's shared
    _process step) fills in posted/posted_ts from posted_date and
    re-extracts skills from title+description, exactly as it does for
    every other source — so this only needs to supply posted_date and a
    description that carries the submitter's (escaped) skill tags.

    Shared by search_submissions (keyword/location-filtered, used by live
    search) and list_approved_as_jobs (unfiltered, used by /recent-jobs)."""
    description = record.get("description", "")
    skills = record.get("skills", [])
    if skills:
        safe_skills = [html.escape(s) for s in skills]
        description = f"{description}\n\nSkills: {', '.join(safe_skills)}"

    apply_link = record.get("apply_link", "")
    contact_email = record.get("contact_email", "")
    link = apply_link or (f"mailto:{contact_email}" if contact_email else "")

    submitted_ts = record.get("submitted_ts", 0.0)
    posted_date = (
        datetime.fromtimestamp(submitted_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        if submitted_ts else ""
    )

    return {
        "title": record.get("title", ""),
        "company": record.get("company", ""),
        "location": record.get("location", ""),
        "posted_date": posted_date,
        "link": link,
        "description": description,
        "source": "Direct",
        "skills": skills,
    }


async def search_submissions(keyword: str, location: str) -> list[dict]:
    """Return approved submissions matching keyword + location, in the
    standard job-dict shape (see _record_to_job)."""
    try:
        raw = await get_redis().hgetall(_APPROVED_KEY)
    except Exception as e:
        log_app(f"[submissions] search error: {e}", "ERROR")
        return []

    jobs: list[dict] = []
    for value in raw.values():
        try:
            record = json.loads(value)
        except Exception:
            continue
        if not title_matches_loose(record.get("title", ""), keyword):
            continue
        if not _location_matches(record.get("location", ""), location):
            continue
        jobs.append(_record_to_job(record))
    return jobs


async def list_approved_as_jobs() -> list[dict]:
    """Return every approved submission in the standard job-dict shape, with
    no keyword/location filtering. Used by /recent-jobs, which aggregates
    across every cached keyword×location pair rather than searching one."""
    try:
        raw = await get_redis().hgetall(_APPROVED_KEY)
    except Exception as e:
        log_app(f"[submissions] list_approved_as_jobs error: {e}", "ERROR")
        return []

    jobs: list[dict] = []
    for value in raw.values():
        try:
            record = json.loads(value)
        except Exception:
            continue
        jobs.append(_record_to_job(record))
    return jobs


async def check_rate_limit(ip: str) -> bool:
    """Increment this IP's submission counter and return whether it's still
    within SUBMISSION_RATE_LIMIT for the current SUBMISSION_RATE_WINDOW_SECS
    window. Fails open (returns True) if Redis errors — don't let an
    infrastructure hiccup block legitimate submissions."""
    if not ip:
        return True
    try:
        redis = get_redis()
        key = f"{_RATE_KEY_PREFIX}{ip}"
        count = await redis.incr(key)
        if count == 1:
            await redis.expire(key, SUBMISSION_RATE_WINDOW_SECS)
        return count <= SUBMISSION_RATE_LIMIT
    except Exception as e:
        log_app(f"[submissions] rate limit check error: {e}", "ERROR")
        return True


_PENDING_MAX_AGE_DAYS = 30


async def prune_expired_approved() -> int:
    """Delete approved submissions older than RECENT_DAYS, and abandoned
    pending submissions nobody reviewed within _PENDING_MAX_AGE_DAYS.
    Called once daily from warmup.py's existing cleanup cycle.
    Returns total count removed."""
    approved_cutoff = time.time() - RECENT_DAYS * 86400
    pending_cutoff = time.time() - _PENDING_MAX_AGE_DAYS * 86400
    removed = 0
    try:
        redis = get_redis()
        for key, cutoff in ((_APPROVED_KEY, approved_cutoff), (_PENDING_KEY, pending_cutoff)):
            raw = await redis.hgetall(key)
            for sub_id, value in raw.items():
                try:
                    record = json.loads(value)
                except Exception:
                    continue
                if record.get("submitted_ts", 0.0) < cutoff:
                    await redis.hdel(key, sub_id)
                    removed += 1
        if removed:
            log_app(f"[submissions] pruned {removed} expired submission(s)")
        return removed
    except Exception as e:
        log_app(f"[submissions] prune error: {e}", "ERROR")
        return 0
