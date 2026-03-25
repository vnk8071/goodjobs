import re
import time as _time
from datetime import date, timedelta
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _relative_display, _extract_html, _truncate

_TOPDEV_REGION_IDS: dict[str, str] = {
    "ho chi minh": "79",
    "hcm":         "79",
    "hồ chí minh": "79",
    "hanoi":       "01",
    "ha noi":      "01",
    "hà nội":      "01",
    "da nang":     "48",
    "danang":      "48",
    "đà nẵng":     "48",
}


def scrape_topdev(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """
    Scrapes TopDev using Playwright (Next.js SSR).
    Only returns jobs posted within RECENT_DAYS days.
    """
    keyword_enc = quote_plus(keyword)
    region_id   = _topdev_region_id(location or "Ho Chi Minh City")
    if region_id is None:
        return []
    url = f"https://topdev.vn/jobs/search?keyword={keyword_enc}&page=1&region_ids={region_id}"
    return _topdev_playwright(url, max_results)


def scrape_topdev_detail_one(job: dict, cooldown: float) -> None:
    """
    Fetch and fill description for a single TopDev job in-place (Phase 2 enrichment).
    """
    if cooldown > 0:
        _time.sleep(cooldown)
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            try:
                context = browser.new_context(
                    user_agent=HEADERS["User-Agent"],
                    locale="en-US",
                )
                page = context.new_page()
                try:
                    page.goto(job["link"], wait_until="networkidle", timeout=20000)
                except Exception:
                    pass
                desc = page.evaluate("""() => {
                    // Find the most specific container with job description content
                    const candidates = [];
                    document.querySelectorAll('div, section, article').forEach(el => {
                        const txt = el.innerText || '';
                        if (txt.length > 200 && el.innerHTML.length < 15000) {
                            const cls = el.className || '';
                            if (cls.includes('border-text-200') || cls.includes('job-description') || cls.includes('job-detail')) {
                                candidates.push({el, htmlLen: el.innerHTML.length});
                            }
                        }
                    });
                    candidates.sort((a, b) => a.htmlLen - b.htmlLen);
                    return candidates.length > 0 ? candidates[0].el.innerHTML.trim() : '';
                }""")
                if desc:
                    job["description"] = desc
            finally:
                browser.close()
    except Exception as e:
        print(f"[TopDev detail] {e}")


def _topdev_region_id(location: str) -> str | None:
    key = location.strip().lower()
    for candidate, rid in _TOPDEV_REGION_IDS.items():
        if candidate in key:
            return rid
    return None


def _topdev_playwright(url: str, max_results: int) -> list[dict]:
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            try:
                context = browser.new_context(
                    user_agent=HEADERS["User-Agent"],
                    locale="en-US",
                    extra_http_headers={
                        "Accept-Language": "en-US,en;q=0.9",
                        "Referer": "https://topdev.vn/",
                    },
                )
                page = context.new_page()
                page.goto(url, wait_until="networkidle", timeout=15000)
                soup = BeautifulSoup(page.content(), "html.parser")
            finally:
                browser.close()
        return _parse_topdev(soup, max_results)
    except Exception as e:
        print(f"[TopDev Playwright] {e}")
        return []


def _topdev_days_ago(text: str) -> int:
    """Parse English relative date string into days-ago integer (9999 if unparseable)."""
    t = text.lower()
    if "just now" in t or "hour" in t or "today" in t:
        return 0
    m = re.search(r"(\d+)\s*day", t)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*week", t)
    if m:
        return int(m.group(1)) * 7
    m = re.search(r"(\d+)\s*month", t)
    if m:
        return int(m.group(1)) * 30
    return 9999


def _parse_topdev(soup: BeautifulSoup, max_results: int) -> list[dict]:
    all_divs = soup.find_all("div", class_=lambda c: c and "text-card-foreground" in (c if isinstance(c, list) else [c]))
    job_cards = [d for d in all_divs if d.find("a", href=lambda h: h and "/detail-jobs/" in h)]

    jobs = []
    for card in job_cards:
        title_a = card.find("a", href=lambda h: h and "/detail-jobs/" in h)
        if not title_a:
            continue

        title = title_a.get_text(strip=True)
        href  = title_a.get("href", "").split("?")[0]
        if not href:
            continue
        if not href.startswith("http"):
            href = "https://topdev.vn" + href

        company_el = title_a.find_next_sibling("span")
        company = company_el.get_text(strip=True) if company_el else "N/A"

        grid = card.find("div", class_=lambda c: c and "grid-cols-2" in str(c))
        loc_el = grid.find("span", class_=lambda c: c and "line-clamp-1" in (c if isinstance(c, list) else [c])) if grid else None
        location = loc_el.get_text(strip=True) if loc_el else ""

        salary_el = card.find("span", class_=lambda c: c and "text-brand-500" in (c if isinstance(c, list) else [c]))
        salary = salary_el.get_text(strip=True) if salary_el else ""
        if salary and "login" in salary.lower():
            salary = ""

        posted_text = ""
        for sp in card.find_all("span"):
            t = sp.get_text(strip=True)
            if re.search(r"(ago|hour|day|week|month)", t, re.I) and len(t) < 30:
                posted_text = t
                break

        days_ago = _topdev_days_ago(posted_text)
        posted_date = (date.today() - timedelta(days=days_ago)).isoformat() if days_ago < 9999 else ""

        if days_ago > RECENT_DAYS:
            continue

        img = card.find("img", alt="job-image")
        logo_url = img.get("src", "") if img else ""

        jobs.append({
            "title":       title,
            "company":     company,
            "location":    location,
            "link":        href,
            "source":      "TopDev",
            "posted":      _relative_display(days_ago) if days_ago < 9999 else posted_text,
            "posted_date": posted_date,
            "description": "",
            "summary_description": "",  # No enrichment for basic listings
            "logo":        logo_url,
            "salary":      salary,
        })

        if len(jobs) >= max_results:
            break

    return jobs
