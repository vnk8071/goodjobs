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

import requests

from .cache import get_redis
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
abbreviations (e.g. "AI lead" → "Lead AI Engineer", "UX designer" → "UI/UX Designer"). \
NEVER change the core intent drastically. If nothing needs fixing, return the original query.
2. "suggested_cache_keyword": the single best matching keyword from the \
provided cached_keywords list that matches the corrected query intent, or null if none fit.
3. "reasoning": one short English sentence explaining the correction (or "no change needed").

Rules:
- Only fix obvious spelling errors or expand well-known abbreviations.
- Do NOT add seniority levels (junior/senior) if the user didn't specify one.
- Do NOT change the job domain (e.g. UX to frontend engineering).
- "corrected" must remain close in meaning to the original.
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
        entry = json.dumps({"keyword": keyword, "location": location, "ts": int(time.time())}, ensure_ascii=False)
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


def _call_cloudflare_ai(prompt: str) -> str | None:
    """Single (non-batch) Cloudflare AI call. Returns the assistant reply or None."""
    if not CLOUDFLARE_ACCOUNT_ID or not CLOUDFLARE_API_TOKEN:
        return None
    url = f"{CLOUDFLARE_API_BASE}/{CLOUDFLARE_ACCOUNT_ID}/ai/run/{CLOUDFLARE_MODEL}"
    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": _MAX_TOKENS,
        "temperature": _TEMPERATURE,
        "top_p": _TOP_P,
        "top_k": _TOP_K,
        "chat_template_kwargs": {"enable_thinking": False},
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("result", {}).get("response", "").strip()
    except Exception as e:
        log_app(f"[intent] Cloudflare AI call failed: {e}", "WARNING")
        return None


def _parse_ai_response(raw: str) -> dict | None:
    """Parse the AI JSON response, tolerating markdown code fences."""
    try:
        cleaned = raw.strip().strip("`").lstrip("json").strip()
        # Extract first JSON object if model prepended text
        m = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if m:
            cleaned = m.group(0)
        obj = json.loads(cleaned)
        if "corrected" in obj:
            return obj
    except Exception:
        pass
    return None


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
        history_lines = [f"  - {h['keyword']} ({h.get('location', '')})" for h in history]
        history_text = "Recent searches by this user:\n" + "\n".join(history_lines)
    else:
        history_text = "No recent search history available."

    # Provide top-50 cached keywords as candidates (avoid huge prompts)
    kw_sample = cached_keywords[:50]
    cached_kw_text = ", ".join(f'"{k}"' for k in kw_sample) if kw_sample else "none"

    prompt = (
        f"User raw query: \"{raw_keyword}\"\n\n"
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
