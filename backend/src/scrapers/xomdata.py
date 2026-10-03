"""Xóm Jobs (jobs.xomdata.com) scraper.

jobs.xomdata.com is a Next.js site that server-renders its job cards into the
homepage. It has no search endpoint: the query parameter is ignored and the
same ~20 newest data-focused jobs come back. So the scraper reads that listing
and leaves title relevance to the caller's title filter.
"""

import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

from ..constants import HEADERS, RECENT_DAYS
from ..utils import _relative_display, board_label
from .topcv import _topcv_location_matches

_BASE = "https://jobs.xomdata.com"
_TIMEOUT = 20

# "22 giờ trước", "3 ngày trước", "vừa xong", "hôm qua". Units are Vietnamese.
_AGO_RE = re.compile(r"(\d+)\s*(phút|giờ|ngày|tuần|tháng)\s*trước", re.IGNORECASE)
_UNIT_HOURS = {"phút": 1 / 60, "giờ": 1, "ngày": 24, "tuần": 24 * 7, "tháng": 24 * 30}

# Cities the listing covers. Anything else returns [] rather than the whole listing.
_SUPPORTED_CITIES = ("ho chi minh", "hcm", "hồ chí minh", "hanoi", "ha noi", "hà nội", "da nang", "danang", "đà nẵng")


def _posted_ts(text: str, now: datetime) -> float:
    """Convert a Vietnamese relative time string to a unix timestamp. 0.0 if unparseable."""
    lowered = text.lower()
    if "vừa" in lowered or "hôm nay" in lowered:
        return now.timestamp()
    if "hôm qua" in lowered:
        return (now - timedelta(days=1)).timestamp()
    m = _AGO_RE.search(lowered)
    if not m:
        return 0.0
    hours = int(m.group(1)) * _UNIT_HOURS[m.group(2)]
    return (now - timedelta(hours=hours)).timestamp()


def _parse_cards(html: str, search_location: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - RECENT_DAYS * 86400
    jobs: list[dict] = []
    seen: set[str] = set()

    for card in soup.select('a[href^="/jobs/"]'):
        title_el = card.select_one("h3")
        if not title_el:
            continue
        title = title_el.get_text(" ", strip=True)
        href = card.get("href", "").split("?")[0]
        link = _BASE + href
        if not title or link in seen:
            continue

        company_el = card.select_one("p")
        company = company_el.get_text(" ", strip=True) if company_el else "N/A"

        # The location is the first meta span after the "Thoả thuận" salary label.
        meta = [s.get_text(" ", strip=True) for s in card.select("div.mt-2 span")]
        meta = [m for m in meta if m and m not in ("·",)]
        location = meta[1] if len(meta) > 1 else ""
        if location and not _topcv_location_matches(location, search_location):
            continue

        posted_ts = _posted_ts(card.get_text(" ", strip=True), now)
        if posted_ts <= 0 or posted_ts < cutoff:
            continue
        seen.add(link)
        days_ago = max(0, int((now.timestamp() - posted_ts) // 86400))
        jobs.append({
            "title": title,
            "company": company,
            "location": location,
            "posted": _relative_display(days_ago),
            "posted_ts": posted_ts,
            "link": link,
            "description": "",
            "source": "XomData",
            "skills": [],
        })
    return jobs


_APPLY_RE = re.compile(r"ứng tuyển|apply", re.IGNORECASE)
_DETAIL_WORKERS = 8


def _apply_url(detail_link: str) -> str:
    """Return the external apply link on a Xóm Jobs detail page, or "" if none."""
    try:
        resp = requests.get(detail_link, headers=HEADERS, timeout=_TIMEOUT)
        if resp.status_code != 200:
            return ""
    except Exception:
        return ""
    soup = BeautifulSoup(resp.text, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "xomdata" not in href and _APPLY_RE.search(a.get_text(" ", strip=True)):
            return href
    return ""


def _label_by_apply_board(jobs: list[dict]) -> None:
    """Label a job LinkedIn/ITViec when it is applied there, otherwise keep XomData.

    A relabelled job also takes its apply URL as the link. The board's detail
    enrichment fetches the link, so the link must be on the board it is labelled as.
    """
    with ThreadPoolExecutor(max_workers=_DETAIL_WORKERS) as pool:
        apply_urls = list(pool.map(lambda j: _apply_url(j["link"]), jobs))
    for job, apply_url in zip(jobs, apply_urls):
        label = board_label(apply_url, "XomData")
        job["source"] = label
        if label != "XomData":
            job["link"] = apply_url


def scrape_xomdata(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """Read the Xóm Jobs listing and keep jobs in the requested city. Returns [] on any failure.

    `keyword` is accepted for the scraper contract but not sent: the site has no
    search endpoint, so title relevance is applied by the caller.
    """
    key = (location or "").strip().lower()
    if not any(city in key for city in _SUPPORTED_CITIES):
        return []
    try:
        resp = requests.get(_BASE + "/", headers=HEADERS, timeout=_TIMEOUT)
        if resp.status_code != 200:
            return []
        jobs = _parse_cards(resp.text, location or "")
        _label_by_apply_board(jobs)
    except Exception as e:
        print(f"[xomdata] {e}")
        return []
    jobs.sort(key=lambda j: j["posted_ts"], reverse=True)
    return jobs[:max_results]
