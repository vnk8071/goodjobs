import re
from datetime import datetime, timezone

from .constants import SYNONYMS, COMPILED_SKILLS


def _word_similarity(a: str, b: str) -> float:
    """Return character-level Jaccard similarity between two strings."""
    if not a or not b:
        return 0.0
    set_a = set(a)
    set_b = set(b)
    return len(set_a & set_b) / len(set_a | set_b)


def _expand(text: str) -> set[str]:
    """Return the set of synonym equivalents for a word or phrase."""
    for group in SYNONYMS:
        if text in group:
            return group
    return {text}


def _normalize(text: str) -> str:
    """Apply multi-word synonym normalisation (full stack → fullstack, etc.)."""
    text = re.sub(r"\bfull[\s\-]stack\b", "fullstack", text)
    text = re.sub(r"\bfront[\s\-]end\b", "frontend", text)
    text = re.sub(r"\bback[\s\-]end\b", "backend", text)
    text = re.sub(r"\bmachine\s+learning\b", "ml", text)
    text = re.sub(r"\bartificial\s+intelligence\b", "ai", text)
    return text


def title_matches(title: str, keyword: str) -> bool:
    """Return True if the job title is relevant to the keyword phrase.

    Strategy: exact phrase match, then synonym-expanded per-word AND match,
    then Jaccard similarity >= 0.5 as fallback.
    """
    title_lower = _normalize(title.strip().lower())
    kw_phrase   = _normalize(keyword.strip().lower())

    if kw_phrase in title_lower:
        return True

    kw_words    = [w for w in re.split(r"\s+", kw_phrase) if len(w) >= 2]
    title_words = re.split(r"[\s/\(\)\-,\.]+", title_lower)
    title_words = [w for w in title_words if w]

    if not kw_words:
        return True

    def _word_matches(kw: str) -> bool:
        kw_variants = _expand(kw)
        for variant in kw_variants:
            if variant in title_lower:
                return True
            if len(variant) >= 3 and any(tw.startswith(variant) for tw in title_words):
                return True
        if any(_word_similarity(kw, tw) > 0.6 for tw in title_words if abs(len(tw) - len(kw)) <= 2):
            return True
        return False

    if all(_word_matches(kw) for kw in kw_words):
        return True

    kw_set    = set(kw_words)
    title_set = set(title_words)
    overlap   = len(kw_set & title_set) / len(kw_set | title_set)
    return overlap >= 0.5


def extract_skills(title: str, description: str) -> list[str]:
    """Return list of canonical skill names found in title or description."""
    text = f"{title} {description}"
    return [name for name, patterns in COMPILED_SKILLS if any(p.search(text) for p in patterns)]


def posted_ts(j: dict) -> float:
    """Return Unix timestamp for a job dict, parsed from posted_date. Returns 0.0 on failure."""
    raw = j.get("posted_date", "")
    if not raw:
        return 0.0
    normalised = re.sub(r"\.(\d+)Z$", lambda m: f".{m.group(1)[:6].ljust(6, '0')}Z", raw)
    for fmt, s in (
        ("%Y-%m-%dT%H:%M:%S.%fZ", normalised),
        ("%Y-%m-%dT%H:%M:%SZ",    normalised[:20]),
        ("%Y-%m-%dT%H:%M:%S",     normalised[:19]),
        ("%Y-%m-%d",              normalised[:10]),
    ):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    return 0.0
