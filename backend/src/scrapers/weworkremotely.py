import time as _time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree

import requests

from ..constants import HEADERS, RECENT_DAYS
from ..utils import _clean_html, _truncate, _relative_display
from ..logger import log_app

_RSS_URL = "https://weworkremotely.com/categories/remote-programming-jobs.rss"


def scrape_weworkremotely(keyword: str, location: str = "") -> list[dict]:
    """Scrape We Work Remotely's public RSS feed. No Playwright — plain XML, no anti-bot wall."""
    try:
        resp = requests.get(_RSS_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        root = ElementTree.fromstring(resp.content)
    except Exception as e:
        log_app(f"[WeWorkRemotely] fetch error: {e}", "ERROR")
        return []

    kw_lower = keyword.lower().strip()
    now = _time.time()
    cutoff = now - RECENT_DAYS * 86400
    jobs: list[dict] = []

    for item in root.iter("item"):
        raw_title = (item.findtext("title") or "").strip()
        if not raw_title:
            continue
        if kw_lower and kw_lower not in raw_title.lower():
            continue

        company, _, job_title = raw_title.partition(":")
        job_title = job_title.strip() or raw_title
        company = company.strip() if job_title != raw_title else ""

        pub_date_raw = item.findtext("pubDate") or ""
        try:
            dt = parsedate_to_datetime(pub_date_raw)
            posted_ts = dt.timestamp()
        except Exception:
            continue
        if posted_ts < cutoff:
            continue
        days_ago = int((now - posted_ts) // 86400)

        region = (item.findtext("region") or "").strip()
        description_html = item.findtext("description") or ""

        jobs.append({
            "title": job_title,
            "company": company,
            "location": region or "Remote",
            "posted": _relative_display(days_ago),
            "posted_date": datetime.fromtimestamp(posted_ts, tz=timezone.utc).strftime("%Y-%m-%d"),
            "posted_ts": posted_ts,
            "link": item.findtext("link") or "",
            "description": _truncate(_clean_html(description_html)),
            "source": "WeWorkRemotely",
            "skills": [],
            "logo": "",
        })

    return jobs
