"""TechJobs (techjobs.vn) scraper.

techjobs.vn is a Next.js app that server-renders its job list into the RSC
payload (`self.__next_f.push(...)` scripts). Searching with `?q=` and paging with
`page=` / `pageSize=` returns the same JSON-like job objects the site uses, so
no browser is needed.
"""

import json
import unicodedata
from urllib.parse import urlsplit, urlunsplit
import re
from datetime import datetime, timezone

import requests

from ..constants import HEADERS, RECENT_DAYS
from ..matching import strip_generic_role
from ..utils import _relative_display, apply_host, apply_label, board_label

_BASE = "https://techjobs.vn/jobs"
_PAGE_SIZE = 50
_MAX_PAGES = 3
_TIMEOUT = 20

_RSC_CHUNK_RE = re.compile(r'self\.__next_f\.push\(\[1,"((?:\\.|[^"\\])*)"\]\)')
# Flat job objects: every field is a string, number, null or boolean. Nested objects are skipped.
_JOB_OBJ_RE = re.compile(r'\{"id":\d+,(?:"[a-z_]+":(?:"(?:\\.|[^"\\])*"|null|-?\d+(?:\.\d+)?|true|false),?)+\}')

# Substrings (accent-free, lowercase) that identify a city in a TechJobs location string.
# TechJobs writes locations both with and without accents ("Hồ Chí Minh", "Ho Chi Minh").
_CITY_TERMS = {
    "ho chi minh": ("ho chi minh", "hcm", "tp.hcm", "saigon", "sai gon"),
    "hanoi": ("ha noi", "hanoi", "hn"),
    "da nang": ("da nang", "danang"),
}


def _strip_accents(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower().replace("đ", "d")


def _city_key(location: str) -> str | None:
    key = _strip_accents(location or "")
    for city, terms in _CITY_TERMS.items():
        if any(term in key for term in terms):
            return city
    return None


def _rsc_payload(html: str) -> str:
    """Join and unescape the Next.js RSC chunks embedded in the page."""
    parts = []
    for chunk in _RSC_CHUNK_RE.findall(html):
        try:
            parts.append(json.loads('"' + chunk + '"'))
        except ValueError:
            continue
    return "".join(parts)


def _canonical_apply_url(url: str) -> str:
    """Return the form other scrapers use, so one posting has one link.

    ITViec serves the same posting at /viec-lam-it/<slug>-<id> (TechJobs) and
    /it-jobs/<slug>-<id> (ITViec scraper). LinkedIn and ITViec links also carry
    tracking query strings. Link-based dedup only works when the links match exactly.
    """
    parts = urlsplit(url)
    host = parts.netloc.lower().removeprefix("www.")
    if host not in ("itviec.com", "linkedin.com"):
        return url
    path = parts.path
    if host == "itviec.com" and path.startswith("/viec-lam-it/"):
        path = "/it-jobs/" + path[len("/viec-lam-it/"):]
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _parse_iso(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _parse_jobs(payload: str) -> list[dict]:
    """Turn RSC job objects into scraper dicts. Objects missing a title or apply_url are dropped."""
    now = datetime.now(timezone.utc).timestamp()
    cutoff = now - RECENT_DAYS * 86400
    jobs: list[dict] = []
    seen: set[str] = set()

    for raw in _JOB_OBJ_RE.findall(payload):
        try:
            obj = json.loads(raw)
        except ValueError:
            continue
        title = (obj.get("title") or "").strip()
        link = _canonical_apply_url((obj.get("apply_url") or "").strip())
        if not title or not link.startswith("http") or link in seen:
            continue
        # Prefer the posting date; fall back to when TechJobs first saw the listing.
        posted_ts = _parse_iso(obj.get("posted_at")) or _parse_iso(obj.get("first_seen_at"))
        if posted_ts <= 0 or posted_ts < cutoff:
            continue
        seen.add(link)
        days_ago = max(0, int((now - posted_ts) // 86400))
        jobs.append({
            "title": title,
            "company": (obj.get("company_name") or "").strip() or "N/A",
            "location": (obj.get("location") or "").strip(),
            "posted": _relative_display(days_ago),
            "posted_ts": posted_ts,
            "link": link,
            "description": "",
            "source": board_label(link, apply_label(link) or "TechJobs"),
            "skills": [],
        })
    return jobs


def scrape_techjobs(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """Search techjobs.vn for a keyword in one city. Returns [] on any failure."""
    city = _city_key(location or "")
    if city is None:
        return []
    # Search on the distinctive word ("Backend Engineer" → "backend"); the title filter
    # downstream keeps the relevant jobs. The full phrase returns far fewer results.
    query = strip_generic_role(keyword.strip()) if keyword.strip() else ""
    if not query:
        return []

    jobs: list[dict] = []
    seen_links: set[str] = set()
    try:
        for page in range(1, _MAX_PAGES + 1):
            # sort=newest puts the last RECENT_DAYS of postings first, so paging stops early.
            params = {"q": query, "sort": "newest", "pageSize": _PAGE_SIZE}
            if page > 1:
                params["page"] = page
            resp = requests.get(_BASE, params=params, headers=HEADERS, timeout=_TIMEOUT)
            if resp.status_code != 200:
                break
            page_jobs = [
                j for j in _parse_jobs(_rsc_payload(resp.text))
                if j["link"] not in seen_links and _city_key(j["location"]) == city
            ]
            if not page_jobs:
                break
            for j in page_jobs:
                seen_links.add(j["link"])
            jobs.extend(page_jobs)
            if len(jobs) >= max_results * 2:
                break
    except Exception as e:
        print(f"[techjobs] {e}")
        return []

    jobs.sort(key=lambda j: j["posted_ts"], reverse=True)
    return jobs[:max_results]
