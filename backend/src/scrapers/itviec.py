import re
from datetime import date, timedelta

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _relative_display

_ITVIEC_CITY_SLUGS = {
    "ho chi minh":  "ho-chi-minh-hcm",
    "hcm":          "ho-chi-minh-hcm",
    "hồ chí minh":  "ho-chi-minh-hcm",
    "hanoi":        "ha-noi",
    "hà nội":       "ha-noi",
    "ha noi":       "ha-noi",
    "danang":       "da-nang",
    "da nang":      "da-nang",
    "đà nẵng":      "da-nang",
}

def scrape_itviec(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """
    Scrapes ITViec using Playwright (required for Cloudflare).
    Fetches description from /content endpoint via the same browser session.
    Only returns jobs posted within RECENT_DAYS days, sorted newest first.
    """
    keyword_slug = keyword.strip().lower().replace(" ", "-")
    city_slug    = _itviec_city_slug(location or "Ho Chi Minh City")
    if city_slug is None:
        return []
    url = f"https://itviec.com/it-jobs/{keyword_slug}/{city_slug}"
    return _itviec_playwright(url, max_results)

def _itviec_city_slug(location: str) -> str | None:
    """Return the ITViec URL city slug for a given location string."""
    key = location.strip().lower()
    for candidate, slug in _ITVIEC_CITY_SLUGS.items():
        if candidate in key:
            return slug
    return None


def _itviec_display(text: str) -> str:
    """Normalise an ITViec English date string to a relative time display."""
    t = text.lower()
    if "today" in t or "just now" in t or "hour" in t:
        return _relative_display(0)
    m = re.search(r"(\d+)\s*day", t)
    if m:
        return _relative_display(int(m.group(1)))
    m = re.search(r"(\d+)\s*week", t)
    if m:
        return _relative_display(int(m.group(1)) * 7)
    m = re.search(r"(\d+)\s*month", t)
    if m:
        return _relative_display(int(m.group(1)) * 30)
    return text


def _itviec_playwright(url: str, max_results: int) -> list[dict]:
    """Scrape ITViec job listings and descriptions via headless Chromium.

    Strategy to bypass Cloudflare:
    1. Load the listing page in one browser context to extract all card metadata.
    2. For each job, open a **fresh incognito context** (new_context) to load
       the individual job page — Cloudflare does not challenge fresh contexts
       the same way it blocks re-navigations within an existing session.
    3. Extract description from `.jd-main` on the job detail page.
    """
    try:
        from playwright.sync_api import sync_playwright
        import time as _time

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)

            # ── Step 1: load listing, extract all card data ───────────────
            ctx = browser.new_context(user_agent=HEADERS["User-Agent"], locale="en-US")
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                page.wait_for_selector("div.job-card", timeout=10000)
            except Exception:
                pass

            page_title = page.title()
            if "security service" in page_title.lower() or "just a moment" in page_title.lower():
                ctx.close()
                browser.close()
                return []

            jobs = _extract_itviec_cards_js(page)
            ctx.close()

            jobs = [j for j in jobs if j["_days_ago"] <= RECENT_DAYS][:max_results]

            # ── Step 2: fetch each job description in a fresh context ─────
            for i, job in enumerate(jobs):
                job.pop("_content_url", "")
                days_ago      = job.pop("_days_ago", 9999)
                card_location = job.pop("_card_location", "")
                job["posted_date"] = (
                    (date.today() - timedelta(days=days_ago)).isoformat()
                    if days_ago < 9999 else ""
                )
                job["description"] = ""
                if not job.get("location") and card_location:
                    job["location"] = card_location

                if i > 0:
                    _time.sleep(1.5)

                job_ctx = browser.new_context(user_agent=HEADERS["User-Agent"], locale="en-US")
                try:
                    job_page = job_ctx.new_page()
                    job_page.goto(job["link"], wait_until="domcontentloaded", timeout=20000)
                    _time.sleep(1.5)
                    detail_title = job_page.title()
                    if "just a moment" not in detail_title.lower():
                        desc = job_page.evaluate("""() => {
                            const el = document.querySelector('.jd-main');
                            return el ? el.innerText.trim() : '';
                        }""")
                        job["description"] = desc or ""
                except Exception as e:
                    print(f"[ITViec desc] {job['link']}: {e}")
                finally:
                    job_ctx.close()

            browser.close()
        return jobs
    except Exception as e:
        print(f"[ITViec Playwright] {e}")
        return []




def _extract_itviec_cards_js(page) -> list[dict]:
    """Extract job card data from ITViec listing page using JavaScript (avoids page.content() which triggers Cloudflare)."""
    cards_data = page.evaluate("""() => {
        const cards = Array.from(document.querySelectorAll('div.job-card'));
        return cards.map(card => {
            const titleEl = card.querySelector("h3[data-search--job-selection-target='jobTitle']");
            const imgEl = card.querySelector('a.logo-employer-card img');
            const locEl = card.querySelector('div.search-tag');
            const postedEl = card.querySelector('span.small-text.text-dark-grey');
            const salaryEl = card.querySelector("div.job-card__salary, span.salary, div[class*='salary']");
            return {
                title: titleEl ? titleEl.textContent.trim() : '',
                slug: card.getAttribute('data-search--job-selection-job-slug-value') || '',
                contentUrl: card.getAttribute('data-search--job-selection-job-url-value') || '',
                logoUrl: imgEl ? (imgEl.getAttribute('data-src') || imgEl.getAttribute('src') || '') : '',
                company: imgEl ? imgEl.getAttribute('alt') || '' : 'N/A',
                location: locEl ? locEl.textContent.trim() : '',
                postedText: postedEl ? postedEl.textContent.trim() : '',
                salary: salaryEl ? salaryEl.textContent.trim() : '',
                cardText: card.textContent || '',
            };
        });
    }""")
    import re as _re
    jobs = []
    for c in cards_data:
        if not c["title"] or not c["slug"]:
            continue
        company = _re.sub(r"\s*(Vietnam)?\s*(?:Big|Small)\s*Logo\s*$", "", c["company"], flags=_re.IGNORECASE).strip() or "N/A"
        salary = c["salary"]
        if salary and "sign in" in salary.lower():
            salary = ""
        if not salary:
            m = _re.search(r"(?:Up\s+to|From)?\s*\$[\d,]+(?:\s*-\s*\$[\d,]+)?", c["cardText"], _re.IGNORECASE)
            if m:
                salary = m.group(0).strip()
        days_ago = _parse_itviec_days_ago(c["postedText"])
        jobs.append({
            "title":          c["title"],
            "company":        company,
            "location":       c["location"],
            "link":           f"https://itviec.com/it-jobs/{c['slug']}",
            "source":         "ITViec",
            "posted":         _itviec_display(c["postedText"]),
            "description":    "",
            "logo":           c["logoUrl"],
            "salary":         salary,
            "_days_ago":      days_ago,
            "_content_url":   c["contentUrl"],
            "_card_location": c["location"],
        })
    jobs.sort(key=lambda j: j["_days_ago"])
    return jobs


def _parse_itviec_days_ago(text: str) -> int:
    """Parse an ITViec relative date string into days-ago integer (9999 if unparseable)."""
    text = text.lower()
    if "today" in text or "just now" in text or "hour" in text:
        return 0
    m = re.search(r"(\d+)\s*day", text)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*week", text)
    if m:
        return int(m.group(1)) * 7
    m = re.search(r"(\d+)\s*month", text)
    if m:
        return int(m.group(1)) * 30
    return 9999


def _parse_itviec_cards(soup: BeautifulSoup) -> list[dict]:
    """Parse job card elements from an ITViec search results page."""
    cards = soup.select("div.job-card")
    jobs = []

    for card in cards:
        title_el = card.select_one("h3[data-search--job-selection-target='jobTitle']")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)

        slug = card.get("data-search--job-selection-job-slug-value", "")
        link = f"https://itviec.com/it-jobs/{slug}" if slug else ""
        content_path = card.get("data-search--job-selection-job-url-value", "")

        img_el = card.select_one("a.logo-employer-card img")
        logo_url = ""
        if img_el:
            alt = img_el.get("alt", "")
            company = re.sub(
                r"\s*(Vietnam)?\s*(?:Big|Small)\s*Logo\s*$", "", alt,
                flags=re.IGNORECASE
            ).strip()
            logo_url = img_el.get("data-src") or img_el.get("src") or ""
        else:
            company = "N/A"

        loc_el = card.select_one("div.search-tag")
        location = loc_el.get_text(strip=True) if loc_el else ""

        posted_el = card.select_one("span.small-text.text-dark-grey")
        posted_text = " ".join(posted_el.get_text().split()) if posted_el else ""
        days_ago = _parse_itviec_days_ago(posted_text)

        salary_el = card.select_one("div.job-card__salary, span.salary, div[class*='salary']")
        salary = salary_el.get_text(strip=True) if salary_el else ""
        if salary and "sign in" in salary.lower():
            salary = ""
        if not salary:
            card_text = card.get_text(separator=" ", strip=True)
            m = re.search(r"(?:Up\s+to|Lên\s+đến|From|Từ)?\s*\$[\d,]+(?:\s*-\s*\$[\d,]+)?", card_text, re.IGNORECASE)
            if m:
                salary = m.group(0).strip()
            else:
                m = re.search(r"[\d,]+\s*-\s*[\d,]+\s*(?:USD|VND|triệu)", card_text, re.IGNORECASE)
                if m:
                    salary = m.group(0).strip()

        if title and link:
            jobs.append({
                "title":           title,
                "company":         company,
                "location":        location,
                "link":            link,
                "source":          "ITViec",
                "posted":          _itviec_display(posted_text),
                "description":     "",
                "logo":            logo_url,
                "salary":          salary,
                "_days_ago":       days_ago,
                "_content_url":    content_path,
                "_card_location":  location,
            })

    jobs.sort(key=lambda j: j["_days_ago"])
    return jobs


