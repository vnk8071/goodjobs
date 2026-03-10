import re
import time as _time
from datetime import date, timedelta

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _relative_display, _clean_html, _extract_html, _truncate

_TOPCV_CITY_PARAMS: dict[str, tuple[str, str, str]] = {
    "ho chi minh": ("ho-chi-minh", "kl2", "l2"),
    "hcm":         ("ho-chi-minh", "kl2", "l2"),
    "hồ chí minh": ("ho-chi-minh", "kl2", "l2"),
    "hanoi":       ("ha-noi",      "kl1", "l1"),
    "ha noi":      ("ha-noi",      "kl1", "l1"),
    "hà nội":      ("ha-noi",      "kl1", "l1"),
    "da nang":     ("da-nang",     "kl8", "l8"),
    "danang":      ("da-nang",     "kl8", "l8"),
    "đà nẵng":     ("da-nang",     "kl8", "l8"),
}

_TOPCV_CITY_KEYWORDS: dict[str, list[str]] = {
    "ho chi minh": ["hồ chí minh", "ho chi minh", "hcm", "tp.hcm", "tp hcm"],
    "ha noi":      ["hà nội", "ha noi", "hanoi"],
    "da nang":     ["đà nẵng", "da nang", "danang"],
}

_TOPCV_BOILERPLATE = re.compile(
    r"Cách\s*thức\s*ứng\s*tuyển", re.IGNORECASE
)

def scrape_topcv(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """
    Scrapes TopCV using Playwright (Vue-rendered page).
    Only returns jobs posted within RECENT_DAYS days.
    """
    keyword_slug              = keyword.strip().lower().replace(" ", "-")
    result = _topcv_city_params(location or "Ho Chi Minh City")
    if result is None:
        return []
    city_slug, city_code, loc_param = result
    url = (
        f"https://www.topcv.vn/tim-viec-lam-{keyword_slug}"
        f"-tai-{city_slug}-{city_code}"
        f"?type_keyword=1&sba=1&locations={loc_param}"
    )
    return _topcv_playwright(url, max_results, location or "Ho Chi Minh City")


def scrape_topcv_detail_one(job: dict, cooldown: float) -> None:
    """
    Fetch and fill description + logo for a single TopCV job in-place.
    Uses wait_until='commit' + wait_for_selector on div.container.job-detail.
    """
    if cooldown > 0:
        _time.sleep(cooldown)
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                locale="vi-VN",
            )
            page = context.new_page()
            try:
                page.goto(job["link"], wait_until="commit", timeout=15000)
                page.wait_for_selector("div.container.job-detail", timeout=8000)
            except Exception:
                pass
            detail_soup = BeautifulSoup(page.content(), "html.parser")
            browser.close()

        desc = _parse_topcv_description(detail_soup)
        if desc:
            job["description"] = desc
        if not job.get("logo"):
            logo_el = detail_soup.select_one(
                "a.company-logo img.img-responsive, div.job-detail__company img.img-responsive"
            )
            if logo_el:
                job["logo"] = logo_el.get("src", "")
    except Exception as e:
        print(f"[TopCV detail] {e}")

def _topcv_city_params(location: str) -> tuple[str, str, str] | None:
    """Return (city_slug, city_code, loc_param) for the TopCV URL from a location string."""
    key = location.strip().lower()
    for candidate, params in _TOPCV_CITY_PARAMS.items():
        if candidate in key:
            return params
    return None


def _topcv_playwright(url: str, max_results: int, location: str = "") -> list[dict]:
    """Scrape TopCV job listings via headless Chromium."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                locale="vi-VN",
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=15000)
            try:
                page.wait_for_selector("div.job-item-search-result", timeout=10000)
            except Exception:
                pass
            soup = BeautifulSoup(page.content(), "html.parser")
            jobs = _parse_topcv(soup, max_results, location)
            browser.close()
        return jobs
    except Exception as e:
        print(f"[TopCV Playwright] {e}")
        return []


def _topcv_days_ago(text: str) -> int:
    """Parse a Vietnamese relative date string into a days-ago integer (9999 if unparseable)."""
    text = text.lower()
    if "hôm nay" in text or "vừa đăng" in text or "giờ" in text:
        return 0
    m = re.search(r"(\d+)\s*ngày", text)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*tuần", text)
    if m:
        return int(m.group(1)) * 7
    m = re.search(r"(\d+)\s*tháng", text)
    if m:
        return int(m.group(1)) * 30
    return 9999


def _topcv_display(text: str, days_ago: int) -> str:
    """Return a human-readable posted date string for a TopCV job card."""
    if days_ago < 9999:
        return _relative_display(days_ago)
    return text


def _topcv_location_matches(card_location: str, search_location: str) -> bool:
    """Return True if a job card location matches the requested search location."""
    if not search_location:
        return True
    key = search_location.strip().lower()
    for city_key, keywords in _TOPCV_CITY_KEYWORDS.items():
        if any(kw in key for kw in keywords) or city_key in key:
            card_loc_lower = card_location.strip().lower()
            return any(kw in card_loc_lower for kw in keywords)
    return True


def _strip_topcv_boilerplate(el) -> None:
    """Remove the "Cách thức ứng tuyển" (How to apply) section and everything after it."""
    for tag in el.find_all(["h2", "h3", "h4", "p", "div", "strong", "b"]):
        if _TOPCV_BOILERPLATE.search(tag.get_text()):
            for sib in list(tag.find_next_siblings()):
                sib.decompose()
            tag.decompose()
            break


def _parse_topcv_description(soup: BeautifulSoup) -> str:
    """Extract sanitized job description HTML from a TopCV detail page."""
    for sel in (
        "div.job-description__text",
        "div.job-description",
        "div[class*='job-description']",
        "div.content-tab",
        "section.job-detail__body",
    ):
        el = soup.select_one(sel)
        if el:
            _strip_topcv_boilerplate(el)
            html_content = _extract_html(el)
            if html_content:
                return _truncate(html_content)

    heading = None
    for tag in ("h2", "h3", "h4", "div", "p"):
        heading = soup.find(tag, string=re.compile(r"Mô\s*tả\s*công\s*việc", re.IGNORECASE))
        if heading:
            break

    if heading:
        parts = [str(heading)]
        for sib in heading.next_siblings:
            if _TOPCV_BOILERPLATE.search(sib.get_text() if hasattr(sib, "get_text") else str(sib)):
                break
            parts.append(str(sib))
        desc = "".join(parts).strip()
        if desc:
            return _truncate(_clean_html(desc))

    return ""


def _parse_topcv(soup: BeautifulSoup, max_results: int, search_location: str = "") -> list[dict]:
    """Parse job card elements from a TopCV search results page."""
    cards = soup.select("div.job-item-search-result")
    jobs = []

    for card in cards:
        title_el = card.select_one("h3.title a, h2.title a, a.job-title")
        if not title_el:
            title_el = card.select_one("a[href*='/viec-lam/']")
        if not title_el:
            continue

        title = title_el.get_text(strip=True)
        href  = title_el.get("href", "").split("?")[0]
        if not href:
            continue

        company_el = card.select_one("a.company, div.company a, span.company-name")
        company = company_el.get_text(strip=True) if company_el else "N/A"

        loc_el = card.select_one("div.address, span.address, label.address")
        location = loc_el.get_text(strip=True) if loc_el else ""

        salary_el = card.select_one("label.title-salary, div.salary, span.salary, label[class*='salary']")
        salary = salary_el.get_text(strip=True) if salary_el else ""

        date_el = card.select_one("label.deadline, div.deadline, span[class*='date'], label[class*='date']")
        posted_text = date_el.get_text(strip=True) if date_el else ""
        days_ago    = _topcv_days_ago(posted_text)

        posted_date = (
            (date.today() - timedelta(days=days_ago)).isoformat()
            if days_ago < 9999 else ""
        )

        if days_ago > RECENT_DAYS:
            continue

        if location and not _topcv_location_matches(location, search_location):
            continue

        jobs.append({
            "title":       title,
            "company":     company,
            "location":    location,
            "link":        href,
            "source":      "TopCV",
            "posted":      _topcv_display(posted_text, days_ago),
            "posted_date": posted_date,
            "description": "",
            "logo":        "",
            "salary":      salary,
        })

        if len(jobs) >= max_results:
            break

    return jobs
