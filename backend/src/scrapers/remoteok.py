import time as _time
from datetime import datetime, timezone

import requests

from ..constants import HEADERS, RECENT_DAYS
from ..utils import _clean_html, _truncate, _relative_display
from ..logger import log_app

_API_URL = "https://remoteok.com/api"


def scrape_remoteok(keyword: str, location: str = "") -> list[dict]:
    """Scrape RemoteOK's public JSON API. No Playwright — plain JSON, no anti-bot wall."""
    try:
        resp = requests.get(_API_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log_app(f"[RemoteOK] fetch error: {e}", "ERROR")
        return []

    kw_lower = keyword.lower().strip()
    now = _time.time()
    cutoff = now - RECENT_DAYS * 86400
    jobs: list[dict] = []

    for item in data:
        if "position" not in item:
            continue  # skip the leading {"legal": ...} metadata entry

        title = item.get("position", "")
        tags = item.get("tags", []) or []
        haystack = f"{title} {' '.join(tags)}".lower()
        if kw_lower and kw_lower not in haystack:
            continue

        epoch = item.get("epoch")
        try:
            posted_ts = float(epoch)
        except (TypeError, ValueError):
            continue
        if posted_ts < cutoff:
            continue
        days_ago = int((now - posted_ts) // 86400)

        jobs.append({
            "title": title,
            "company": item.get("company", ""),
            "location": item.get("location", "") or "Remote",
            "posted": _relative_display(days_ago),
            "posted_date": datetime.fromtimestamp(posted_ts, tz=timezone.utc).strftime("%Y-%m-%d"),
            "posted_ts": posted_ts,
            "link": item.get("url") or item.get("apply_url", ""),
            "description": _truncate(_clean_html(item.get("description", ""))),
            "source": "RemoteOK",
            "skills": [t for t in tags if isinstance(t, str)],
            "logo": item.get("company_logo") or item.get("logo", ""),
        })

    return jobs
