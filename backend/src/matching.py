import re
from datetime import datetime, timezone

from .constants import SYNONYMS, COMPILED_SKILLS

_LEVEL_WORDS = {
    "senior", "sr", "junior", "jr", "intern", "internship",
    "fresher", "lead", "staff", "principal", "mid", "entry",
    "associate", "head",
}


def strip_level(keyword: str) -> str:
    """Return keyword with seniority/level words removed, lowercased and stripped."""
    words = keyword.lower().strip().split()
    return " ".join(w for w in words if w not in _LEVEL_WORDS).strip() or keyword.lower().strip()


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

    Strategy: match primarily against the core title (text before any parenthetical).
    A keyword word found only inside parentheses does not count as a primary match.
    Falls back to exact phrase match and Jaccard similarity on the core title only.
    """
    kw_phrase  = _normalize(keyword.strip().lower())

    # Split off parenthetical qualifiers — use only the core title for matching.
    # e.g. "Data Engineer (AI Platform)" → core = "data engineer"
    core_title = _normalize(re.split(r"\s*[\(\[]", title.strip().lower())[0].strip())

    if kw_phrase in core_title:
        return True

    kw_words    = [w for w in re.split(r"\s+", kw_phrase) if len(w) >= 2]
    core_words  = re.split(r"[\s/\-,\.]+", core_title)
    core_words  = [w for w in core_words if w]

    if not kw_words:
        return True

    def _match_index(kw: str) -> int:
        """Return the earliest core_words index that matches kw, or -1 if none."""
        kw_variants = _expand(kw)
        for i, tw in enumerate(core_words):
            for variant in kw_variants:
                if variant == tw or (len(variant) >= 3 and tw.startswith(variant)):
                    return i
            if _word_similarity(kw, tw) > 0.6 and abs(len(tw) - len(kw)) <= 2:
                return i
        return -1

    # All keyword words must match AND appear in order (subsequence) in the core title.
    # e.g. "AI Engineer" requires "ai" to appear before "engineer" in core_words.
    # This prevents "Engineer AI" from matching "AI Engineer".
    indices = [_match_index(kw) for kw in kw_words]
    if all(idx >= 0 for idx in indices) and indices == sorted(indices):
        return True

    kw_set   = set(kw_words)
    core_set = set(core_words)
    overlap  = len(kw_set & core_set) / len(kw_set | core_set)
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


def posted_relative(posted_ts_val: float) -> str:
    """Return a human-readable relative time string (e.g. '2 hours ago') from a Unix timestamp."""
    if posted_ts_val <= 0:
        return ""
    now = datetime.now(timezone.utc).timestamp()
    delta_secs = int(now - posted_ts_val)
    if delta_secs < 0:
        delta_secs = 0
    if delta_secs < 60:
        return f"{delta_secs} second{'s' if delta_secs != 1 else ''} ago"
    delta_mins = delta_secs // 60
    if delta_mins < 60:
        return f"{delta_mins} minute{'s' if delta_mins != 1 else ''} ago"
    delta_hours = delta_mins // 60
    if delta_hours < 24:
        return f"{delta_hours} hour{'s' if delta_hours != 1 else ''} ago"
    delta_days = delta_hours // 24
    if delta_days < 7:
        return f"{delta_days} day{'s' if delta_days != 1 else ''} ago"
    delta_weeks = delta_days // 7
    return f"{delta_weeks} week{'s' if delta_weeks != 1 else ''} ago"
