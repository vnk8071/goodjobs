import time as _time
from datetime import datetime, timezone

import requests

from ..constants import USAJOBS_API_KEY, USAJOBS_USER_AGENT, RECENT_DAYS
from ..utils import _clean_html, _truncate, _relative_display
from ..logger import log_app

_API_URL = "https://data.usajobs.gov/api/search"


def scrape_usajobs(keyword: str, location: str = "") -> list[dict]:
    """Scrape USAJOBS.gov's official public API — no scraping, no anti-bot concerns,
    this is a first-party API intended for third-party job boards to consume.

    Requires USAJOBS_API_KEY and USAJOBS_USER_AGENT (the email address registered
    for the key) to be configured — returns [] gracefully if either is missing,
    matching this project's no-evasion, degrade-gracefully contract.
    """
    if not USAJOBS_API_KEY or not USAJOBS_USER_AGENT:
        log_app("[USAJOBS] USAJOBS_API_KEY/USAJOBS_USER_AGENT not configured — skipping", "WARN")
        return []

    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": USAJOBS_USER_AGENT,
        "Authorization-Key": USAJOBS_API_KEY,
    }
    params: dict[str, str] = {"Keyword": keyword}
    if location:
        params["LocationName"] = location

    try:
        resp = requests.get(_API_URL, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log_app(f"[USAJOBS] fetch error: {e}", "ERROR")
        return []

    now = _time.time()
    cutoff = now - RECENT_DAYS * 86400
    jobs: list[dict] = []

    items = ((data.get("SearchResult") or {}).get("SearchResultItems")) or []
    for item in items:
        descriptor = item.get("MatchedObjectDescriptor") or {}
        title = descriptor.get("PositionTitle", "")
        if not title:
            continue

        pub_date_raw = descriptor.get("PublicationStartDate", "")
        try:
            dt = datetime.fromisoformat(pub_date_raw.replace("Z", "+00:00"))
            posted_ts = dt.timestamp()
        except (ValueError, TypeError):
            continue
        if posted_ts < cutoff:
            continue
        days_ago = int((now - posted_ts) // 86400)

        summary = ""
        details = (descriptor.get("UserArea") or {}).get("Details") or {}
        summary = details.get("JobSummary", "")
        if not summary:
            formatted = descriptor.get("PositionFormattedDescription") or []
            if isinstance(formatted, list) and formatted:
                summary = formatted[0].get("Content", "")

        jobs.append({
            "title": title,
            "company": descriptor.get("OrganizationName") or descriptor.get("DepartmentName", ""),
            "location": descriptor.get("PositionLocationDisplay", ""),
            "posted": _relative_display(days_ago),
            "posted_date": datetime.fromtimestamp(posted_ts, tz=timezone.utc).strftime("%Y-%m-%d"),
            "posted_ts": posted_ts,
            "link": descriptor.get("PositionURI", ""),
            "description": _truncate(_clean_html(summary)),
            "source": "USAJOBS",
            "skills": [],
            "logo": "",
        })

    return jobs
