"""TechJobs (techjobs.vn) scraper.

techjobs.vn is a Next.js app that server-renders its job list into the RSC
payload (`self.__next_f.push(...)` scripts). Searching with `?q=` and paging with
`page=` / `pageSize=` returns the same JSON-like job objects the site uses, so
no browser is needed.
"""

import json
import re
from datetime import datetime, timezone

import requests

from ..constants import HEADERS, RECENT_DAYS
from ..matching import strip_generic_role
from ..utils import _relative_display

_BASE = "https://techjobs.vn/jobs"
_PAGE_SIZE = 50
_MAX_PAGES = 3
_TIMEOUT = 20

_RSC_CHUNK_RE = re.compile(r'self\.__next_f\.push\(\[1,"((?:\\.|[^"\\])*)"\]\)')
# Flat job objects: every field is a string, number, null or boolean. Nested objects are skipped.
_JOB_OBJ_RE = re.compile(r'\{"id":\d+,(?:"[a-z_]+":(?:"(?:\\.|[^"\\])*"|null|-?\d+(?:\.\d+)?|true|false),?)+\}')

_CITY_PARAMS = {
    "ho chi minh": "Hồ Chí Minh",
    "hcm": "Hồ Chí Minh",
    "hồ chí minh": "Hồ Chí Minh",
    "hanoi": "Hà Nội",
    "ha noi": "Hà Nội",
    "hà nội": "Hà Nội",
    "da nang": "Đà Nẵng",
    "danang": "Đà Nẵng",
    "đà nẵng": "Đà Nẵng",
}


def _city_param(location: str) -> str | None:
    key = location.strip().lower()
    for candidate, label in _CITY_PARAMS.items():
        if candidate in key:
            return label
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
        link = (obj.get("apply_url") or "").strip()
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
            "source": "TechJobs",
            "skills": [],
        })
    return jobs


def scrape_techjobs(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """Search techjobs.vn for a keyword in one city. Returns [] on any failure."""
    city = _city_param(location or "")
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
            params = {"q": query, "loc": city, "sort": "newest", "pageSize": _PAGE_SIZE}
            if page > 1:
                params["page"] = page
            resp = requests.get(_BASE, params=params, headers=HEADERS, timeout=_TIMEOUT)
            if resp.status_code != 200:
                break
            page_jobs = [j for j in _parse_jobs(_rsc_payload(resp.text)) if j["link"] not in seen_links]
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
