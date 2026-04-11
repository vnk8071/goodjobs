"""Intent pre-verification for user search queries.

Uses Cloudflare AI to:
1. Fix spelling/typos in the raw query (conservative — never drastic rewrites)
2. Predict search intent and map to the most relevant cached keywords

IP-based search history (last 5 searches) is stored in Redis and passed as
context so the AI can infer intent from the user's recent activity.
"""

import hashlib
import json
import re
import time

# PII patterns to scrub from CV text before sending to external AI
_PII_PATTERNS: list[tuple[str, str]] = [
    # Email addresses
    (r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", "[EMAIL]"),
    # Phone numbers (Vietnamese and international formats)
    (r"(?<!\d)(\+?84|0)[\s.\-]?\d{2,3}[\s.\-]?\d{3,4}[\s.\-]?\d{3,4}(?!\d)", "[PHONE]"),
    (r"(?<!\d)\+?[1-9]\d{0,2}[\s.\-]?\(?\d{1,4}\)?[\s.\-]?\d{3,4}[\s.\-]?\d{3,6}(?!\d)", "[PHONE]"),
    # URLs
    (r"https?://\S+|www\.\S+", "[URL]"),
    # Dates of birth (various formats)
    (r"\b(?:ngày sinh|date of birth|dob|born)[:\s]+[\d/\-\.]{6,}", "[DOB]"),
    # CMND / CCCD / Passport numbers
    (r"\b(?:CMND|CCCD|Passport|Hộ chiếu|ID)[:\s#]*[\dA-Z]{7,12}\b", "[ID_NUMBER]"),
    # Street addresses (common Vietnamese address patterns)
    (r"\b\d+[,\s]+(?:đường|phố|ngõ|hẻm|tổ|khu|quận|huyện|phường|xã|tỉnh|thành phố)[^\n]{0,60}", "[ADDRESS]"),
]

_PII_RE: list[tuple[re.Pattern, str]] = [
    (re.compile(pat, re.IGNORECASE), repl) for pat, repl in _PII_PATTERNS
]


def _scrub_pii(text: str) -> str:
    """Remove PII from CV text before sending to external LLM."""
    for pattern, replacement in _PII_RE:
        text = pattern.sub(replacement, text)
    return text

import requests

from .cache import get_redis
from .matching import _LEVEL_WORDS
from .constants import (
    CLOUDFLARE_ACCOUNT_ID,
    CLOUDFLARE_API_BASE,
    CLOUDFLARE_API_TOKEN,
    CLOUDFLARE_MODEL,
)
from .logger import log_app

# Redis key prefix for per-IP search history
_HISTORY_KEY_PREFIX = "search-history:"
# Keep last N searches per IP
_HISTORY_MAX = 5
# TTL for history keys (7 days)
_HISTORY_TTL = 7 * 86400

# Cloudflare AI generation params — deterministic output preferred
_TEMPERATURE = 0.1
_TOP_P = 0.3
_TOP_K = 10
_MAX_TOKENS = 200

_SYSTEM_PROMPT = """\
You are a Vietnamese job-search query assistant. Given a raw user query, \
recent search history, and a list of known cached keywords, return a JSON object with:

1. "corrected": the corrected query — fix ONLY spelling/typos and common \
abbreviations (e.g. "UX designer" → "UI/UX Designer"). \
NEVER change the core intent drastically. If nothing needs fixing, return the original query.
2. "suggested_cache_keyword": the single best matching keyword from the \
provided cached_keywords list that matches the corrected query intent, or null if none fit.
3. "reasoning": one short Vietnamese sentence explaining the correction (or "no change needed").

Rules:
- Only fix obvious spelling errors or expand well-known abbreviations.
- Do NOT add or remove seniority/level words (intern/junior/senior/lead) from the query.
- Do NOT change the job domain (e.g. UX to frontend engineering).
- For "suggested_cache_keyword": the domain (e.g. "AI", "Backend", "Marketing") MUST match \
the query. Do NOT suggest a cached keyword just because it shares a level word. \
Example: "ai intern" must NOT map to "AI Engineer" — return null instead.
- "corrected" must remain close in meaning to the original.
- The reasoning should be concise and in Vietnamese.
- Return ONLY the JSON object, no markdown, no explanation outside the JSON.
"""


def _hash_ip(ip: str) -> str:
    """Return a 16-char irreversible hash of the IP address."""
    return hashlib.sha256(ip.encode()).hexdigest()[:16]


def _history_key(ip: str) -> str:
    return f"{_HISTORY_KEY_PREFIX}{_hash_ip(ip)}"


async def record_search(ip: str, keyword: str, location: str) -> None:
    """Append a search to the IP's history, keeping only the last _HISTORY_MAX entries."""
    try:
        redis = get_redis()
        key = _history_key(ip)
        entry = json.dumps(
            {"keyword": keyword, "location": location, "ts": int(time.time())},
            ensure_ascii=False,
        )
        pipe = redis.pipeline()
        pipe.lpush(key, entry)
        pipe.ltrim(key, 0, _HISTORY_MAX - 1)
        pipe.expire(key, _HISTORY_TTL)
        await pipe.execute()
    except Exception as e:
        log_app(f"[intent] record_search error: {e}", "WARNING")


async def get_search_history(ip: str) -> list[dict]:
    """Return the last _HISTORY_MAX searches for an IP (most recent first)."""
    try:
        redis = get_redis()
        raw_entries = await redis.lrange(_history_key(ip), 0, _HISTORY_MAX - 1)
        result = []
        for raw in raw_entries:
            try:
                result.append(json.loads(raw))
            except Exception:
                pass
        return result
    except Exception as e:
        log_app(f"[intent] get_search_history error: {e}", "WARNING")
        return []


def _call_cloudflare_ai_with_system(
    system: str, prompt: str, max_tokens: int = _MAX_TOKENS
) -> str | None:
    """Cloudflare AI call with a given system prompt. Returns the assistant reply or None."""
    if not CLOUDFLARE_ACCOUNT_ID or not CLOUDFLARE_API_TOKEN:
        return None
    url = f"{CLOUDFLARE_API_BASE}/{CLOUDFLARE_ACCOUNT_ID}/ai/run/{CLOUDFLARE_MODEL}"
    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": _TEMPERATURE,
        "top_p": _TOP_P,
        "top_k": _TOP_K,
        "chat_template_kwargs": {"enable_thinking": False},
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        result = data.get("result", {})

        # Cloudflare's response schema can vary by model.
        # Observed formats:
        # 1) {"result": {"response": "..."}}
        # 2) OpenAI-ish: {"result": {"choices": [{"message": {"content": "..."}}]}}
        # Some models (e.g. qwen3*) may emit the actual text in message.reasoning_content
        # with message.content being null, even with enable_thinking=False.
        direct = (result.get("response") or "").strip()
        if direct:
            return direct

        choices = result.get("choices") or []
        if choices:
            msg = (choices[0] or {}).get("message") or {}
            content = (msg.get("content") or "").strip()
            if content:
                return content
            reasoning = (msg.get("reasoning_content") or "").strip()
            if reasoning:
                return reasoning

        return None
    except Exception as e:
        log_app(f"[intent] Cloudflare AI call failed: {e}", "WARNING")
        return None


def _call_cloudflare_ai(prompt: str) -> str | None:
    """Cloudflare AI call with the default suggest-query system prompt."""
    return _call_cloudflare_ai_with_system(_SYSTEM_PROMPT, prompt)


def _parse_ai_response(raw: str) -> dict | None:
    """Parse the AI JSON response, tolerating markdown code fences."""
    try:
        cleaned = raw.strip().strip("`").lstrip("json").strip()
        m = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if m:
            cleaned = m.group(0)
        return json.loads(cleaned)
    except Exception:
        pass
    return None


# ── Input classification ──────────────────────────────────────────────────────

_CV_SECTION_HEADERS = {
    "education",
    "skills",
    "experience",
    "summary",
    "objective",
    "projects",
    "certifications",
    "languages",
    "references",
    "awards",
    "publications",
    "interests",
    "hobbies",
    "contact",
    "profile",
}

_CLASSIFY_SYSTEM_PROMPT = """\
You are a Vietnamese job-search assistant. Given user input, do two things:
1. Classify whether it is a short job title, a CV / skill list, or completely unrelated to job search.
2. Extract the single best job title keyword to search for (unless unrelated).

Return ONLY a JSON object:
{
  "input_type": "job_title" | "cv_or_skills" | "not_job",
  "keyword": "<2-4 word English job title>",
  "alternatives": ["<related job title>", "<related job title>"] ,
  "reasoning": "<one short sentence>"
}

Classification rules:
- "job_title": the entire input is a short role name (≤6 words), e.g. "AI Engineer", "Data Scientist"
- "cv_or_skills": anything longer — a resume, skill list, bio, or sentences describing experience
- "not_job": input is a greeting, random text, question, or anything not related to job search (e.g. "Hi", "hello", "what is the weather?", "tell me a joke"). Set keyword to "" when using this type.

Keyword extraction rules:
- NEVER use CV section headers (EDUCATION, SKILLS, EXPERIENCE, SUMMARY, etc.) as the keyword
- For a CV: look at the EXPERIENCE section and pick the most recent or most senior job title
- For a skill list: infer the most fitting standard job title from the skills
- PRESERVE seniority/level words (Senior/Junior/Lead/Principal/Staff/Intern/Fresher) exactly as the user wrote them — do NOT strip or change them
- NEVER include dates, months (January..December, Jan..Dec), years, or "Present"/"Current"
- NEVER include company names, locations, or employment type (Full-time/Part-time/Contract)
- The keyword must be a clean English job title of 1–5 words that faithfully reflects what the user asked for (e.g. "AI Lead", "Junior Data Scientist", "Intern Software Engineer")
- alternatives: provide 3–6 closely related standard English job titles (2–5 words each). No duplicates.
- alternatives MUST be job titles (not skills). Preserve the same level/seniority as the user specified.
- Return ONLY the JSON, no markdown, no explanation
"""



def _extract_job_title_fallback(raw_input: str) -> str:
    """Return the first non-header line of the input, truncated to 60 chars.

    Used when Cloudflare AI is unavailable.
    """
    lines = [ln.strip() for ln in raw_input.splitlines() if ln.strip()]
    for ln in lines:
        if ln.lower().strip(":") not in _CV_SECTION_HEADERS:
            return ln[:60]
    return raw_input.strip()[:60]


async def classify_and_extract(raw_input: str) -> dict:
    """Classify user input as job_title or cv_or_skills, and extract a clean keyword.

    Returns:
      {
        "input_type": "job_title" | "cv_or_skills",
        "keyword": str,   # clean job title to search
        "reasoning": str,
        "is_job_title": bool,
      }
    """
    prompt = f'User input:\n"""\n{_scrub_pii(raw_input[:3000])}\n"""'
    raw_reply = _call_cloudflare_ai_with_system(
        _CLASSIFY_SYSTEM_PROMPT, prompt, max_tokens=150
    )

    first_line_raw = _extract_job_title_fallback(raw_input)
    first_line = first_line_raw.strip()
    is_short = len(raw_input.strip().split()) <= 6 and "\n" not in raw_input.strip()
    fallback = {
        "input_type": "job_title" if is_short else "cv_or_skills",
        "keyword": first_line,
        "alternatives": [],
        "reasoning": "AI unavailable",
        "is_job_title": is_short,
    }

    if not raw_reply:
        log_app(
            f"[intent] classify FALLBACK (no AI reply) input={raw_input[:60]!r} keyword={first_line!r}",
            "WARNING",
        )
        return fallback

    parsed = _parse_ai_response(raw_reply)
    if not parsed or "keyword" not in parsed:
        log_app(
            f"[intent] classify_and_extract unparseable: {raw_reply[:200]!r}", "WARNING"
        )
        return fallback

    keyword = (parsed.get("keyword") or first_line).strip()
    # Reject if AI returned a section header (e.g. "EDUCATION")
    if keyword.lower() in _CV_SECTION_HEADERS:
        log_app(
            f"[intent] AI returned section header as keyword, using fallback: {keyword!r}",
            "WARNING",
        )
        keyword = first_line
    input_type = parsed.get("input_type", "job_title")
    is_job_title = input_type == "job_title"

    # Optional related titles
    alternatives_raw = parsed.get("alternatives")
    alternatives: list[str] = []
    if isinstance(alternatives_raw, list):
        for a in alternatives_raw:
            if not isinstance(a, str):
                continue
            t = a.strip()
            if not t:
                continue
            # Keep short job-title-like strings.
            if len(t.split()) < 2 or len(t.split()) > 6:
                continue
            if t.lower() == keyword.lower():
                continue
            if t.lower() in {x.lower() for x in alternatives}:
                continue
            alternatives.append(t)
            if len(alternatives) >= 6:
                break

    log_app(
        f"[intent] classify input={raw_input[:60]!r} type={input_type!r} keyword={keyword!r}"
    )
    return {
        "input_type": input_type,
        "keyword": keyword,
        "alternatives": alternatives,
        "reasoning": (parsed.get("reasoning") or "").strip(),
        "is_job_title": is_job_title,
    }


async def suggest_query(
    raw_keyword: str,
    ip: str,
    cached_keywords: list[str],
) -> dict:
    """Pre-verify user input with Cloudflare AI + IP history context.

    Returns a dict:
      {
        "corrected": str,          # spelling-fixed query (or original if no change)
        "changed": bool,           # True when corrected != original (ignoring case/strip)
        "suggested_cache_keyword": str | None,  # best cache-key match, or None
        "reasoning": str,
      }

    Falls back to the original query if AI is unavailable or slow.
    """
    history = await get_search_history(ip)

    history_text = ""
    if history:
        history_lines = [
            f"  - {h['keyword']} ({h.get('location', '')})" for h in history
        ]
        history_text = "Recent searches by this user:\n" + "\n".join(history_lines)
    else:
        history_text = "No recent search history available."

    # Provide top-50 cached keywords as candidates (avoid huge prompts)
    kw_sample = cached_keywords[:50]
    cached_kw_text = ", ".join(f'"{k}"' for k in kw_sample) if kw_sample else "none"

    prompt = (
        f'User raw query: "{raw_keyword}"\n\n'
        f"{history_text}\n\n"
        f"Available cached_keywords (pick the best one or null): [{cached_kw_text}]"
    )

    raw_reply = _call_cloudflare_ai(prompt)

    fallback = {
        "corrected": raw_keyword,
        "changed": False,
        "suggested_cache_keyword": None,
        "reasoning": "AI unavailable — returning original query.",
    }

    if not raw_reply:
        return fallback

    parsed = _parse_ai_response(raw_reply)
    if not parsed:
        log_app(f"[intent] unparseable AI response: {raw_reply[:200]!r}", "WARNING")
        return fallback

    corrected = (parsed.get("corrected") or raw_keyword).strip()
    # Safety: reject if corrected is more than 2× the length of original (drastic rewrite)
    if len(corrected) > max(len(raw_keyword) * 2, len(raw_keyword) + 20):
        log_app(f"[intent] AI correction too long, rejecting: {corrected!r}", "WARNING")
        corrected = raw_keyword

    changed = corrected.lower().strip() != raw_keyword.lower().strip()
    suggested = parsed.get("suggested_cache_keyword") or None
    # Validate suggested keyword is actually in the list
    if suggested and suggested not in cached_keywords:
        # Case-insensitive lookup
        lower_map = {k.lower(): k for k in cached_keywords}
        suggested = lower_map.get(suggested.lower())
    # Guard: reject suggested cache keyword if its domain words don't overlap
    # with the non-level words in the corrected query.
    # e.g. "ai intern" must not map to "AI Engineer" — "intern" is a level word,
    # so only "ai" is a domain word; "AI Engineer" contains "ai" → allowed.
    # But "backend intern" must not map to "AI Engineer" — "backend" ≠ "ai".
    if suggested:
        query_words = set(corrected.lower().split())
        domain_words = query_words - _LEVEL_WORDS
        suggested_words = set(suggested.lower().split())
        if domain_words and not (domain_words & suggested_words):
            log_app(
                f"[intent] rejected suggested_cache_keyword {suggested!r}: "
                f"domain mismatch with query {corrected!r}",
                "WARNING",
            )
            suggested = None

    log_app(
        f"[intent] suggest_query ip={ip} raw={raw_keyword!r} corrected={corrected!r} "
        f"changed={changed} suggested={suggested!r}"
    )

    return {
        "corrected": corrected,
        "changed": changed,
        "suggested_cache_keyword": suggested,
        "reasoning": (parsed.get("reasoning") or "").strip(),
    }
