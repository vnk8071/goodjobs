"""Google Careers (google.com/about/careers) scraper.

The results page is client-rendered, so this uses headless Chromium. The keyword
query returns "No results" for most phrases, so the scraper reads the Vietnam-wide
listing and leaves title relevance to the caller's title filter. Cards carry no
posting date, so posted_ts uses the scrape time (as ViecOi does).
"""

import time
import unicodedata
from datetime import datetime, timezone

from ..constants import HEADERS, CHROMIUM_ARGS
from ..utils import _relative_display

_BASE = "https://www.google.com/about/careers/applications/jobs/results"
_MAX_PAGES = 2
_CARD_SELECTOR = "a[href*='jobs/results/']"

# Accent-free terms that identify a city in Google's location text.
_CITY_TERMS = {
    "ho chi minh": ("ho chi minh", "hcm", "tp.hcm", "saigon"),
    "hanoi": ("ha noi", "hanoi"),
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


def _parse_card(text: str, href: str, now: float) -> dict | None:
    """Turn one card's visible text into a job dict. Lines are: title, company, location, ..."""
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if not lines:
        return None
    title = lines[0]
    location = ""
    if "place" in lines:
        idx = lines.index("place")
        if idx + 1 < len(lines):
            location = lines[idx + 1]
    if not title or not href:
        return None
    link = href.split("?")[0]
    if not link.startswith("http"):
        link = "https://www.google.com/about/careers/applications/" + link.lstrip("/")
    days_ago = 0
    return {
        "title": title,
        "company": "Google",
        "location": location,
        "posted": _relative_display(days_ago),
        "posted_ts": now,
        "link": link,
        "description": "",
        "source": "Google",
        "skills": [],
    }


def scrape_google(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """Read Google's Vietnam listing and keep jobs in the requested city. Returns [] on any failure.

    `keyword` is accepted for the scraper contract but not sent (keyword queries return
    "No results" for most phrases). Title relevance is applied by the caller.
    """
    city = _city_key(location or "")
    if city is None:
        return []
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return []

    now = datetime.now(timezone.utc).timestamp()
    raw: list[dict] = []
    seen: set[str] = set()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            try:
                ctx = browser.new_context(user_agent=HEADERS["User-Agent"], locale="en-US")
                page = ctx.new_page()
                for page_no in range(1, _MAX_PAGES + 1):
                    url = f"{_BASE}?location=Vietnam" + (f"&page={page_no}" if page_no > 1 else "")
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        page.wait_for_selector(_CARD_SELECTOR, timeout=15000)
                    except Exception:
                        break
                    page.wait_for_timeout(1500)
                    cards = page.evaluate(
                        """(sel) => Array.from(document.querySelectorAll(sel)).map(a => {
                            const li = a.closest('li') || a.parentElement;
                            return {href: a.getAttribute('href') || '', text: li ? li.innerText : ''};
                        })""",
                        _CARD_SELECTOR,
                    )
                    added = 0
                    for card in cards:
                        job = _parse_card(card["text"], card["href"], now)
                        if job and job["link"] not in seen:
                            seen.add(job["link"])
                            raw.append(job)
                            added += 1
                    if added == 0:
                        break
                    time.sleep(0.5)
                ctx.close()
            finally:
                browser.close()
    except Exception as e:
        print(f"[Google Playwright] {e}")
        return []

    jobs = [j for j in raw if _city_key(j["location"]) == city]
    return jobs[:max_results]
