"""Job-aware AI chatbot for Good Jobs.

The chatbot answers questions about jobs using the same cached listings that
power the homepage. It retrieves relevant jobs with Cloudflare Vectorize when
available, falls back to keyword matching, and asks Cloudflare Workers AI to
summarise the results. Every answer is grounded in the retrieved listings; the
frontend also renders the job cards returned alongside the text.
"""

from __future__ import annotations

import asyncio
import collections
import json
import re
import time
import unicodedata
from typing import Any

from .cache import get_redis, is_vn_cache_key
from .intent import _call_cloudflare_ai_with_system
from .logger import log_app
from .utils import _strip_html
from .vector import search as vector_search

_CHAT_RATE_WINDOW = 60
_CHAT_RATE_MAX = 8
_MAX_MESSAGE_LEN = 600
_MAX_HISTORY = 6
_MAX_JOBS = 8
_JOB_CACHE_TTL = 60

# Per-IP in-memory rate limiter, separate from the scrape/search limiter.
_ip_timestamps: dict[str, collections.deque[float]] = collections.defaultdict(
    collections.deque
)
_jobs_cache: tuple[float, list[dict]] | None = None


def check_chat_rate_limit(ip: str) -> bool:
    """Return True if this IP may send another chat message."""
    now = time.time()
    dq = _ip_timestamps[ip]
    while dq and dq[0] < now - _CHAT_RATE_WINDOW:
        dq.popleft()
    if len(dq) >= _CHAT_RATE_MAX:
        return False
    dq.append(now)
    return True


# ─── Text helpers ────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    """Lowercase and strip Vietnamese diacritics for matching."""
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.replace("đ", "d").replace("Đ", "D").lower()


_STOPWORDS = {
    "the", "a", "an", "and", "or", "for", "in", "on", "at", "to", "of",
    "is", "are", "any", "find", "show", "me", "jobs", "job", "work",
    "works", "working", "viec", "lam", "tim", "co", "khong", "nao", "o",
    "tai", "va", "voi", "cho", "minh", "ban", "giup", "can", "muon",
    "nhung", "cac", "mot", "nhieu", "ve", "la", "gi", "how", "what",
    "which", "where", "when", "who", "why", "you", "i", "we", "my",
    "your", "please", "give", "list", "recommend", "suggest", "about",
    "with", "from", "that", "this", "these", "those", "available",
    "opening", "openings", "position", "positions", "role", "roles",
    "salary", "luong", "review", "reviews", "top", "best", "good", "hay",
    "nhat", "hien", "dang", "moi", "nay", "do", "thi", "se", "duoc",
    "skills", "skill",
}

_LOCATION_ALIASES = {
    "hcm": ["ho chi minh"],
    "hcmc": ["ho chi minh"],
    "saigon": ["ho chi minh"],
    "sai gon": ["ho chi minh"],
    "hn": ["ha noi"],
    "hanoi": ["ha noi"],
    "ha noi": ["ha noi"],
    "danang": ["da nang"],
    "da nang": ["da nang"],
    "remote": ["remote"],
}


def _tokens(message: str) -> list[str]:
    normalized = _normalize(message)
    words = re.findall(r"[a-z0-9+#.]+", normalized)
    tokens: list[str] = []
    for word in words:
        if len(word) < 2 or word in _STOPWORDS:
            continue
        tokens.append(word)
        for alias in _LOCATION_ALIASES.get(word, []):
            tokens.extend(alias.split())
    return list(dict.fromkeys(tokens))


def _job_text(job: dict) -> str:
    skills = " ".join(job.get("skills") or [])
    return _normalize(
        " ".join(
            [
                job.get("title") or "",
                job.get("company") or "",
                job.get("location") or "",
                skills,
                job.get("summary_description") or "",
                (job.get("description") or "")[:1200],
            ]
        )
    )


def _keyword_score(job: dict, tokens: list[str]) -> int:
    text = _job_text(job)
    score = 0
    for token in tokens:
        if re.search(r"(?<![a-z0-9])" + re.escape(token) + r"(?![a-z0-9])", text):
            score += 1
    return score


# ─── Retrieval ───────────────────────────────────────────────────────────────

async def _load_cached_jobs() -> list[dict]:
    """Load unique VN jobs from the Redis cache, memoised for a minute."""
    global _jobs_cache
    now = time.time()
    if _jobs_cache and now - _jobs_cache[0] < _JOB_CACHE_TTL:
        return _jobs_cache[1]

    jobs: list[dict] = []
    seen: set[str] = set()
    try:
        redis = get_redis()
        async for key in redis.scan_iter("jobs:*"):
            if not is_vn_cache_key(key):
                continue
            raw = await redis.get(key)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            for job in data.get("jobs", []):
                link = job.get("link") or ""
                if not link or link in seen:
                    continue
                if not (job.get("description") or job.get("summary_description")):
                    continue
                seen.add(link)
                jobs.append(job)
    except Exception as e:
        log_app(f"[chat] load cached jobs error: {e}", "ERROR")

    _jobs_cache = (now, jobs)
    return jobs


async def _retrieve_jobs(message: str, top_k: int = _MAX_JOBS) -> list[dict]:
    """Return the most relevant cached jobs for a natural-language question."""
    loop = asyncio.get_running_loop()

    matches: list[dict] = []
    try:
        matches = await loop.run_in_executor(None, vector_search, message, 20)
    except Exception as e:
        log_app(f"[chat] vector search error: {e}", "WARNING")

    cached = await _load_cached_jobs()
    by_link = {job.get("link"): job for job in cached if job.get("link")}
    scored: dict[str, dict] = {}

    for match in matches:
        link = match.get("link") or ""
        if not link:
            continue
        job = dict(by_link.get(link) or {})
        for key in ("title", "company", "location", "source", "link", "posted"):
            if not job.get(key) and match.get(key):
                job[key] = match[key]
        job["_score"] = float(match.get("score") or 0)
        scored[link] = job

    # Keyword fallback/supplement: keeps the chatbot useful even when
    # Vectorize is unavailable or the question is a simple role/location query.
    tokens = _tokens(message)
    if tokens:
        for job in cached:
            link = job.get("link") or ""
            if not link or link in scored:
                continue
            score = _keyword_score(job, tokens)
            if score > 0:
                job = dict(job)
                job["_score"] = 0.25 + min(score, 4) * 0.05
                scored[link] = job

    ranked = sorted(
        scored.values(),
        key=lambda job: (job.get("_score", 0.0), job.get("posted_ts", 0.0)),
        reverse=True,
    )
    return ranked[:top_k]


def _job_context(jobs: list[dict]) -> str:
    lines: list[str] = []
    for index, job in enumerate(jobs, 1):
        title = job.get("title") or "Untitled"
        company = job.get("company") or "Unknown company"
        location = job.get("location") or "Unknown location"
        source = job.get("source") or "Good Jobs"
        posted = job.get("posted") or ""
        skills = ", ".join((job.get("skills") or [])[:8])
        description = _strip_html(
            job.get("summary_description") or job.get("description") or ""
        )
        description = " ".join(description.split())[:320]
        link = job.get("link") or ""
        lines.append(
            f"{index}. {title} | {company} | {location} | {source} | {posted}\n"
            f"   Skills: {skills or 'n/a'}\n"
            f"   Summary: {description or 'n/a'}\n"
            f"   Link: {link}"
        )
    return "\n".join(lines)


def _system_prompt(lang: str) -> str:
    language = "Vietnamese" if lang == "vi" else "English"
    return f"""\
You are Good Jobs AI, the job-search assistant for Good Jobs (goodjobs.io.vn), \
a Vietnamese job aggregator that indexes listings from LinkedIn, ITviec, TopCV, \
VietnamWorks, Glints, JobsGo and other boards.

Answer in {language}.
Use ONLY the job listings in the user message for facts about specific jobs, \
companies, locations, skills, or salaries. Never invent a job, company, or link.
If the listings do not contain the answer, say so briefly and suggest a more \
specific search (role + location).
You may give general career advice, but clearly label it as general advice.
Be concise: at most 6 short sentences or 5 bullet points. When recommending \
jobs, mention the title, company, location, and source."""


def _history_text(history: list[dict]) -> str:
    lines: list[str] = []
    for message in history[-_MAX_HISTORY:]:
        content = (message.get("content") or "").strip()
        if not content:
            continue
        role = "User" if message.get("role") == "user" else "Assistant"
        lines.append(f"{role}: {content[:400]}")
    return "\n".join(lines)


def _fallback_answer(jobs: list[dict], lang: str) -> str:
    if not jobs:
        return (
            "Mình chưa tìm thấy việc làm phù hợp trong dữ liệu hiện có. "
            "Bạn thử hỏi cụ thể hơn, ví dụ: \"AI Engineer ở Hồ Chí Minh\"."
            if lang == "vi"
            else "I couldn't find matching jobs in the current cache. "
            "Try a more specific question, for example: \"AI Engineer in Ho Chi Minh City\"."
        )
    header = (
        f"Mình tìm thấy {len(jobs)} việc làm liên quan:"
        if lang == "vi"
        else f"I found {len(jobs)} related jobs:"
    )
    lines = [header]
    for job in jobs:
        lines.append(
            f"- {job.get('title', '')} — {job.get('company', '')} "
            f"({job.get('location', '')}) · {job.get('source', '')}"
        )
    return "\n".join(lines)


def _public_job(job: dict) -> dict:
    return {
        "title": job.get("title") or "",
        "company": job.get("company") or "",
        "location": job.get("location") or "",
        "source": job.get("source") or "",
        "link": job.get("link") or "",
        "posted": job.get("posted") or "",
        "skills": (job.get("skills") or [])[:8],
        "score": round(float(job.get("_score", 0.0)), 3),
    }


async def answer_job_question(
    message: str,
    history: list[dict[str, Any]] | None = None,
    lang: str = "en",
) -> dict:
    """Retrieve jobs and generate a grounded answer."""
    message = message.strip()[:_MAX_MESSAGE_LEN]
    jobs = await _retrieve_jobs(message)
    context = _job_context(jobs)
    history_text = _history_text(history or [])

    prompt = ""
    if history_text:
        prompt += f"Conversation so far:\n{history_text}\n\n"
    prompt += f"Question: {message}\n\n"
    prompt += f"Job listings from the Good Jobs cache:\n{context or '(no matching listings)'}"

    loop = asyncio.get_running_loop()
    answer = await loop.run_in_executor(
        None,
        _call_cloudflare_ai_with_system,
        _system_prompt(lang),
        prompt,
        600,
    )
    if not answer:
        answer = _fallback_answer(jobs, lang)

    return {
        "answer": answer,
        "jobs": [_public_job(job) for job in jobs],
        "count": len(jobs),
    }
