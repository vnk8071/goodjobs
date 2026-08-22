import re
import time as _time
from datetime import datetime, timedelta, timezone

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _clean_html, _truncate
from ..logger import log_app

# Confirmed via live inspection 2026-08-22 (see plan Task 5). Extend only after
# confirming a candidate locId the same way — try `&locId={n}&locT=N` and check
# the resulting page <title> names the target country.
_GLASSDOOR_LOC_IDS: dict[str, str] = {
    "US": "1",
    "UK": "2",
}

_AGE_RE = re.compile(r"(\d+)\s*([dhm])", re.IGNORECASE)


def _parse_age(age_text: str) -> float:
    """Parse Glassdoor's 'job-age' text (e.g. '30d+', '5d', '2h') into a Unix timestamp."""
    now = datetime.now(timezone.utc)
    m = _AGE_RE.search(age_text or "")
    if not m:
        return now.timestamp()
    n, unit = int(m.group(1)), m.group(2).lower()
    if unit == "d":
        return (now - timedelta(days=n)).timestamp()
    if unit == "h":
        return (now - timedelta(hours=n)).timestamp()
    return now.timestamp()


def scrape_glassdoor(keyword: str, location: str = "", country: str = "US") -> list[dict]:
    """Scrape Glassdoor's list page. Best-effort: detail pages are anti-bot protected,
    so description comes from the list-page snippet only (see plan Task 5)."""
    loc_id = _GLASSDOOR_LOC_IDS.get(country.upper())
    if not loc_id:
        log_app(f"[Glassdoor] no locId mapped for country={country!r} — skipping", "WARN")
        return []

    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth

        url = f"https://www.glassdoor.com/Job/jobs.htm?sc.keyword={keyword}&locId={loc_id}&locT=N"
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                locale="en-US",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            _time.sleep(2)

            if "just a moment" in page.title().lower():
                log_app("[Glassdoor] blocked by anti-bot wall — skipping", "WARN")
                browser.close()
                return []

            cards = page.query_selector_all('li[data-test="jobListing"]')
            cutoff_ts = _time.time() - RECENT_DAYS * 86400
            jobs: list[dict] = []

            for card in cards:
                title_el = card.query_selector('a[data-test="job-title"]')
                if not title_el:
                    continue
                title = (title_el.inner_text() or "").strip()
                link = title_el.get_attribute("href") or ""

                company_el = card.query_selector('[class*="EmployerProfile_compactEmployerName"]')
                company = (company_el.inner_text() or "").strip() if company_el else ""

                loc_el = card.query_selector('div[data-test="emp-location"]')
                location_text = (loc_el.inner_text() or "").strip() if loc_el else ""

                age_el = card.query_selector('div[data-test="job-age"]')
                posted_ts = _parse_age(age_el.inner_text() if age_el else "")
                if posted_ts < cutoff_ts:
                    continue
                days_ago = int((_time.time() - posted_ts) // 86400)

                desc_el = card.query_selector('div[data-test="descSnippet"]')
                description = _truncate(_clean_html(desc_el.inner_html())) if desc_el else ""

                jobs.append({
                    "title": title,
                    "company": company,
                    "location": location_text,
                    "posted": (age_el.inner_text().strip() if age_el else "Today"),
                    "posted_date": datetime.fromtimestamp(posted_ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                    "posted_ts": posted_ts,
                    "link": link,
                    "description": description,
                    "source": "Glassdoor",
                    "skills": [],
                    "logo": "",
                })
                _ = days_ago  # retained for parity with other scrapers' posted display logic

            browser.close()
        return jobs
    except Exception as e:
        log_app(f"[Glassdoor] error: {e}", "ERROR")
        return []
