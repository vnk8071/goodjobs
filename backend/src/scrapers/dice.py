import re
import time as _time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _clean_html, _truncate
from ..logger import log_app

_AGE_RE = re.compile(r"(\d+)\s*(day|hour|minute)s?\s*ago", re.IGNORECASE)


def _parse_posted_text(text: str) -> float:
    """Parse Dice's relative posted text ('Today', 'Yesterday', 'X days ago')
    into a Unix timestamp. Defaults to now for unrecognized formats."""
    now = datetime.now(timezone.utc)
    t = (text or "").strip().lower()
    if t == "today":
        return now.timestamp()
    if t == "yesterday":
        return (now - timedelta(days=1)).timestamp()
    m = _AGE_RE.search(t)
    if m:
        n, unit = int(m.group(1)), m.group(2).lower()
        if unit == "day":
            return (now - timedelta(days=n)).timestamp()
        return now.timestamp()  # hours/minutes ago — treat as today
    return now.timestamp()


def scrape_dice(keyword: str, location: str = "") -> list[dict]:
    """Scrape Dice.com's list page (confirmed live: no anti-bot wall) and fetch
    each job's full description from its detail page (also unblocked).

    The list page is parsed from a static HTML snapshot (BeautifulSoup) rather
    than live Playwright element handles, so the browser is free to navigate to
    each detail page afterward without invalidating earlier list-card references
    (Playwright ElementHandles from a page go stale once that page navigates away).
    """
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth

        url = f"https://www.dice.com/jobs?q={quote_plus(keyword)}"
        if location:
            url += f"&location={quote_plus(location)}"

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

            soup = BeautifulSoup(page.content(), "html.parser")
            cards = soup.select('[data-testid="job-card"]')
            cutoff_ts = _time.time() - RECENT_DAYS * 86400

            listings: list[dict] = []
            for card in cards[:25]:
                title_el = card.select_one('a[data-testid="job-search-job-detail-link"]')
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                href = title_el.get("href", "")
                link = f"https://www.dice.com{href}" if href.startswith("/") else href

                company_el = card.select_one('[data-testid="job-card-company-name"]')
                company = company_el.get_text(strip=True) if company_el else ""

                loc_el = card.select_one('p.font-normal.text-zinc-600')
                loc_posted = loc_el.get_text(strip=True) if loc_el else ""
                location_text, _, posted_text = loc_posted.partition("•")
                location_text = location_text.strip()
                posted_text = posted_text.strip()

                posted_ts = _parse_posted_text(posted_text)
                if posted_ts < cutoff_ts:
                    continue

                listings.append({
                    "title": title,
                    "company": company,
                    "location": location_text,
                    "posted": posted_text or "Today",
                    "posted_date": datetime.fromtimestamp(posted_ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                    "posted_ts": posted_ts,
                    "link": link,
                })

            jobs: list[dict] = []
            for item in listings:
                description = ""
                if item["link"]:
                    try:
                        page.goto(item["link"], wait_until="domcontentloaded", timeout=20000)
                        _time.sleep(1)
                        desc_el = page.query_selector('[class*="jobDescription"]')
                        if desc_el:
                            description = _truncate(_clean_html(desc_el.inner_html()))
                    except Exception as e:
                        log_app(f"[Dice detail] {e}", "WARNING")

                jobs.append({
                    **item,
                    "description": description,
                    "source": "Dice",
                    "skills": [],
                    "logo": "",
                })

            browser.close()
        return jobs
    except Exception as e:
        log_app(f"[Dice] error: {e}", "ERROR")
        return []
