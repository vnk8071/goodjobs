import collections
import time

_KEYWORD_ALIASES: dict[str, str] = {
    "ai/ml engineer":       "AI Engineer",
    "ml engineer":          "AI Engineer",
    "machine learning engineer": "AI Engineer",
    "mobile engineer":      "Flutter Developer",
    "mobile developer":     "Flutter Developer",
    "ios developer":        "Flutter Developer",
    "android developer":    "Flutter Developer",
    "react native developer": "Flutter Developer",
    "project manager":      "Product Manager",
    "product owner":        "Product Manager",
    "scrum master":         "Product Manager",
    "it ba":                "Business Analyst",
    "data analyst":         "Business Analyst",
    "bi analyst":           "Business Analyst",
    "bi engineer":          "Business Analyst",
    "business intelligence": "Business Analyst",
    "tester":               "QA Engineer",
    "qa tester":            "QA Engineer",
    "quality assurance":    "QA Engineer",
    "test engineer":        "QA Engineer",
    "automation engineer":  "QA Engineer",
}

_RATE_LIMIT_WINDOW  = 60
_RATE_LIMIT_MAX     = 2
_RATE_LIMIT_MAX_CONCURRENT = 1

_ip_timestamps: dict[str, collections.deque] = collections.defaultdict(collections.deque)
_ip_active: dict[str, int] = collections.defaultdict(int)


def check_rate_limit(ip: str) -> str | None:
    """Return an error message if the IP is rate-limited, else None."""
    now = time.time()
    window_start = now - _RATE_LIMIT_WINDOW
    dq = _ip_timestamps[ip]
    while dq and dq[0] < window_start:
        dq.popleft()
    if len(dq) >= _RATE_LIMIT_MAX:
        return "Too many requests. Please wait before searching again."
    if _ip_active[ip] >= _RATE_LIMIT_MAX_CONCURRENT:
        return "You already have a search in progress. Please wait for it to finish."
    dq.append(now)
    return None


def ip_active_inc(ip: str) -> None:
    """Increment the active scrape counter for an IP."""
    _ip_active[ip] += 1


def ip_active_dec(ip: str) -> None:
    """Decrement the active scrape counter for an IP (floor at 0)."""
    _ip_active[ip] = max(0, _ip_active[ip] - 1)
