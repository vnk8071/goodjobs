import re
from datetime import datetime, timezone

from .constants import SYNONYMS, COMPILED_SKILLS

_LEVEL_WORDS = {
    "senior", "sr", "junior", "jr", "intern", "internship",
    "fresher", "staff", "principal", "mid", "entry",
    "associate",
}

# Common job-related English vocabulary for typo correction
_JOB_VOCABULARY = {
    # Roles & positions
    "engineer", "developer", "architect", "manager", "director", "lead", "lead",
    "specialist", "analyst", "consultant", "coordinator", "officer", "executive",
    # Engineering roles
    "backend", "frontend", "fullstack", "devops", "platform", "infrastructure",
    "reliability", "security", "quality", "automation", "database", "cloud",
    # Tech stack
    "python", "java", "javascript", "typescript", "golang", "rust", "php", "ruby",
    "csharp", "dotnet", "cpp", "nodejs", "react", "angular", "vue", "django",
    # Job titles
    "scientist", "researcher", "designer", "product", "business", "data",
    "machine", "learning", "artificial", "intelligence", "ai", "ml",
    # Additional common words
    "tech", "technical", "lead", "head", "manager",
}


def strip_level(keyword: str) -> str:
    """Return keyword with seniority/level words removed, lowercased and stripped."""
    words = keyword.lower().strip().split()
    return " ".join(w for w in words if w not in _LEVEL_WORDS).strip() or keyword.lower().strip()


def _correct_typo_word(typo: str, all_words: list[str]) -> str:
    """Return the best spelling correction using vocabulary + fuzzy matching.

    Priority order:
    1. Exact match in vocabulary (fastest)
    2. Close fuzzy match in vocabulary (similarity > 0.65, first char match, len diff <= 2)
    3. Fuzzy match in all_words (warmup keywords + vocabulary combined)
    4. Original typo if no match found

    Vocabulary-based correction prevents false positives like "dotnet" → "frontend".
    """
    typo_lower = typo.lower()

    # 1. Exact match in vocabulary
    if typo_lower in _JOB_VOCABULARY:
        return typo_lower

    # 2. Fuzzy match in vocabulary (higher priority than all_words)
    vocab_match = None
    best_vocab_score = 0.0
    for vocab_word in _JOB_VOCABULARY:
        if not (typo_lower and vocab_word and typo_lower[0] == vocab_word[0]):
            continue
        score = _word_similarity(typo_lower, vocab_word)
        if score > 0.65 and abs(len(vocab_word) - len(typo_lower)) <= 2 and score > best_vocab_score:
            vocab_match = vocab_word
            best_vocab_score = score

    if vocab_match:
        return vocab_match

    # 3. Fuzzy match in all_words (warmup keywords) as fallback
    best_match = typo_lower
    best_score = 0.0
    for candidate in all_words:
        if typo_lower == candidate:
            return candidate
        if not (typo_lower and candidate and typo_lower[0] == candidate[0]):
            continue
        score = _word_similarity(typo_lower, candidate)
        if score > 0.65 and abs(len(candidate) - len(typo_lower)) <= 2 and score > best_score:
            best_match = candidate
            best_score = score

    return best_match


def correct_keyword_typos(keyword: str, all_known_keywords: list[str]) -> str:
    """Correct typos in keyword by matching against words from known warmup keywords.

    For each word in the keyword, find the best match from all words in all_known_keywords.
    Example: "enginner" (typo) → finds "engineer" from "Backend Engineer", "Frontend Engineer", etc.
    """
    keyword_lower = keyword.lower().strip()
    words = keyword_lower.split()

    all_known_words = set()
    for kw in all_known_keywords:
        kw_words = kw.lower().split()
        all_known_words.update(kw_words)

    corrected = []
    for word in words:
        if len(word) >= 3:
            corrected_word = _correct_typo_word(word, list(all_known_words))
            corrected.append(corrected_word)
        else:
            corrected.append(word)

    return " ".join(corrected)


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


_SHORT_ROLE_EXPANSIONS = {
    # Tech
    r"\btest\b":   "tester",
    r"\bdev\b":    "developer",
    r"\beng\b":    "engineer",
    r"\barch\b":   "architect",
    r"\bpm\b":     "product manager",
    r"\bpo\b":     "product owner",
    r"\bba\b":     "business analyst",
    # Marketing
    r"\bmkt\b":    "marketing",
    r"\bcmo\b":    "chief marketing officer",
    # Finance
    r"\bfin\b":    "finance",
    r"\bacc\b":    "accountant",
    r"\bcfo\b":    "chief financial officer",
    r"\bcpa\b":    "certified public accountant",
    r"\baudit\b":  "auditor",
    # HR / Admin
    r"\bhr\b":     "human resources",
    r"\bta\b":     "talent acquisition",
    r"\badmin\b":  "administrator",
    # Sales
    r"\bbiz\b":    "business",
    r"\bbd\b":     "business development",
    r"\bam\b":     "account manager",
    r"\bae\b":     "account executive",
    # Design
    r"\bux\b":     "ux designer",
    r"\bui\b":     "ui designer",
    # Operations
    r"\bops\b":    "operations",
    r"\bscm\b":    "supply chain",
    r"\bcs\b":     "customer service",
}


def normalize_keyword(text: str) -> str:
    """Apply multi-word synonym normalisation and special character cleanup."""
    for pattern, replacement in _SHORT_ROLE_EXPANSIONS.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    text = re.sub(r"\bfull[\s\-]stack\b", "fullstack", text)
    text = re.sub(r"\bfront[\s\-]end\b", "frontend", text)
    text = re.sub(r"\bback[\s\-]end\b", "backend", text)
    text = re.sub(r"\bmachine\s+learning\b", "ml", text)
    text = re.sub(r"\bartificial\s+intelligence\b", "ai", text)
    # Special characters: .net → dotnet, c# → csharp, c++ → cpp, node.js → nodejs
    # Match ".net", "asp.net", "net" but NOT "dotnet" (already normalized)
    text = re.sub(r"(?:asp\s*)?(?<!dot)\.?\s*net\b", "dotnet", text)
    text = re.sub(r"\bc\s*#", "csharp", text)
    text = re.sub(r"\bc\s*\+\+", "cpp", text)
    text = re.sub(r"\bnode\.js\b", "nodejs", text)
    text = re.sub(r"\bnode\s+js\b", "nodejs", text)
    return text


def title_matches(title: str, keyword: str) -> bool:
    """Return True if the job title is relevant to the keyword phrase.

    Strategy: match primarily against the core title (text before any parenthetical).
    A keyword word found only inside parentheses does not count as a primary match.
    Falls back to exact phrase match and Jaccard similarity on the core title only.
    """
    kw_phrase  = normalize_keyword(keyword.strip().lower())

    # Strip leading bracket tags like [Remote], [HCM], [Urgent] from the title.
    # e.g. "[Remote] Python Developer" → "Python Developer"
    t = re.sub(r"^\s*(\[[^\]]*\]\s*)+", "", title.strip().lower())

    # Split off parenthetical qualifiers and dash-separated suffixes — use only the core role.
    # e.g. "Data Engineer (AI Platform)" → "data engineer"
    # e.g. "Python Developer - Machine Learning Focus" → "python developer"
    core_title = normalize_keyword(re.split(r"\s*[\(\[|]|\s+-\s+", t)[0].strip())

    if kw_phrase in core_title:
        return True

    kw_words    = [w for w in re.split(r"\s+", kw_phrase) if len(w) >= 2]
    core_words  = re.split(r"[\s/\-,\.&_]+", core_title)
    core_words  = [w for w in core_words if w]

    if not kw_words:
        return True

    def _match_index(kw: str) -> int:
        """Return the earliest core_words index that matches kw, or -1 if none."""
        is_level = kw in _LEVEL_WORDS
        kw_variants = _expand(kw)
        for i, tw in enumerate(core_words):
            for variant in kw_variants:
                if variant == tw or (len(variant) >= 3 and tw.startswith(variant)):
                    return i
            if not is_level and _word_similarity(kw, tw) > 0.6 and abs(len(tw) - len(kw)) <= 2:
                return i
        return -1

    # All keyword words must match AND appear in order (subsequence) in the core title.
    # e.g. "AI Engineer" requires "ai" to appear before "engineer" in core_words.
    # This prevents "Engineer AI" from matching "AI Engineer".
    indices = [_match_index(kw) for kw in kw_words]
    if all(idx >= 0 for idx in indices) and indices == sorted(indices):
        return True

    # Level words in the keyword are hard requirements — if any failed to match, reject.
    level_kw_words = [w for w in kw_words if w in _LEVEL_WORDS]
    if any(_match_index(w) < 0 for w in level_kw_words):
        return False

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
