import re
from datetime import date, timedelta
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _parse_iso_date, _relative_display

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TOPDEV_CITY_SLUGS: dict[str, str] = {
    "ho chi minh": "ho-chi-minh",
    "hcm":         "ho-chi-minh",
    "hồ chí minh": "ho-chi-minh",
    "hanoi":       "ha-noi",
    "ha noi":      "ha-noi",
    "hà nội":      "ha-noi",
    "da nang":     "da-nang",
    "danang":      "da-nang",
    "đà nẵng":     "da-nang",
}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scrape_topdev(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """
    Scrapes TopDev (Next.js SSR) using Playwright.
    Only returns jobs posted within RECENT_DAYS days.
    """
    keyword_enc = quote_plus(keyword)
    city_slug   = _topdev_city_slug(location or "Ho Chi Minh City")
    url = f"https://topdev.vn/jobs/search?q={keyword_enc}&city={city_slug}"
    return _topdev_playwright(url, max_results)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _topdev_city_slug(location: str) -> str:
    key = location.strip().lower()
    for candidate, slug in _TOPDEV_CITY_SLUGS.items():
        if candidate in key:
            return slug
    return "ho-chi-minh"


def _topdev_playwright(url: str, max_results: int) -> list[dict]:
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://topdev.vn/",
                },
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                page.wait_for_selector(
                    "div[class*='JobCard'], article[class*='job'], div[class*='job-card'], a[href*='/jobs/']",
                    timeout=12000,
                )
            except Exception:
                pass
            soup = BeautifulSoup(page.content(), "html.parser")
            browser.close()
        return _parse_topdev(soup, max_results)
    except Exception as e:
        print(f"[TopDev Playwright] {e}")
        return []


def _parse_topdev(soup: BeautifulSoup, max_results: int) -> list[dict]:
    from .topcv import _topcv_days_ago, _topcv_display  # shared Vietnamese date helpers

    cards = (
        soup.select("div[class*='JobCard']")
        or soup.select("article[class*='job']")
        or soup.select("div[class*='job-card']")
        or [a.parent for a in soup.select("a[href*='/jobs/'][href$='.html']") if a.parent]
    )
    jobs = []

    for card in cards:
        title_el = card.select_one("h2 a, h3 a, a[href*='/jobs/'][href$='.html']")
        if not title_el:
            continue

        title = title_el.get_text(strip=True)
        href  = title_el.get("href", "").split("?")[0]
        if not href:
            continue
        if not href.startswith("http"):
            href = "https://topdev.vn" + href

        company_el = card.select_one(
            "a[href*='/companies/'], span[class*='company'], div[class*='company']"
        )
        company = company_el.get_text(strip=True) if company_el else "N/A"

        loc_el = card.select_one(
            "span[class*='location'], div[class*='location'], span[class*='city']"
        )
        location = loc_el.get_text(strip=True) if loc_el else ""

        salary_el = card.select_one(
            "span[class*='salary'], div[class*='salary'], span[class*='Salary']"
        )
        salary = salary_el.get_text(strip=True) if salary_el else ""

        time_el = card.select_one("time[datetime]")
        if time_el:
            posted_date = time_el.get("datetime", "")[:10]
            parsed      = _parse_iso_date(posted_date)
            days_ago    = (date.today() - parsed).days if parsed else 9999
            posted_display = _relative_display(days_ago) if parsed else ""
        else:
            date_el     = card.select_one("span[class*='date'], div[class*='date'], span[class*='ago']")
            posted_text = date_el.get_text(strip=True) if date_el else ""
            days_ago    = _topcv_days_ago(posted_text)
            posted_date = (
                (date.today() - timedelta(days=days_ago)).isoformat()
                if days_ago < 9999 else ""
            )
            posted_display = _topcv_display(posted_text, days_ago)

        if days_ago > RECENT_DAYS:
            continue

        logo_el  = card.select_one("img[src*='logo'], img[class*='logo'], img[alt]")
        logo_url = logo_el.get("src", "") if logo_el else ""
        if logo_url and not logo_url.startswith("http"):
            logo_url = "https://topdev.vn" + logo_url

        jobs.append({
            "title":       title,
            "company":     company,
            "location":    location,
            "link":        href,
            "source":      "TopDev",
            "posted":      posted_display,
            "posted_date": posted_date,
            "description": "",
            "logo":        logo_url,
            "salary":      salary,
        })

        if len(jobs) >= max_results:
            break

    return jobs
