import json
import time as _time
from datetime import date, timedelta
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from ..constants import CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _parse_iso_date, _relative_display, _clean_html, _truncate, _fmt_num

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scrape_indeed(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """
    Scrapes Indeed Vietnam (vn.indeed.com) using Playwright.
    Uses JSON-LD on each detail page to get datePosted and description.
    Only returns jobs posted within RECENT_DAYS days.
    """
    keyword_enc  = quote_plus(keyword)
    location_enc = quote_plus(location or "Ho Chi Minh City")
    url = f"https://jobs.vn.indeed.com/jobs?q={keyword_enc}&l={location_enc}&sort=date"
    return _indeed_playwright(url, max_results)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _indeed_playwright(url: str, max_results: int) -> list[dict]:
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            _time.sleep(3)

            soup = BeautifulSoup(page.content(), "html.parser")

            title_text = soup.title.string if soup.title else ""
            if "just a moment" in title_text.lower() or "enable javascript" in soup.get_text().lower():
                print("[Indeed] Blocked by Cloudflare — skipping")
                browser.close()
                return []

            cards = soup.select("div.job_seen_beacon")
            cutoff = date.today() - timedelta(days=RECENT_DAYS)
            jobs = []

            for card in cards:
                title_el = card.select_one("h2.jobTitle span[title]")
                if not title_el:
                    title_el = card.select_one("h2.jobTitle span")
                if not title_el:
                    continue
                title = title_el.get("title") or title_el.get_text(strip=True)

                link_el = card.select_one("a[data-jk]")
                if not link_el:
                    continue
                jk   = link_el.get("data-jk", "")
                href = link_el.get("href", "")
                link = (
                    f"https://jobs.vn.indeed.com{href}"
                    if href.startswith("/") else href
                )
                view_link = f"https://jobs.vn.indeed.com/viewjob?jk={jk}" if jk else link

                company_el = card.select_one("span[data-testid='company-name']")
                company = company_el.get_text(strip=True) if company_el else "N/A"

                loc_el = card.select_one("div[data-testid='text-location']")
                location_text = loc_el.get_text(strip=True) if loc_el else ""

                posted_date    = ""
                posted_display = ""
                description    = ""
                salary         = ""
                days_ago       = 9999

                if jk:
                    try:
                        page.goto(view_link, wait_until="domcontentloaded", timeout=20000)
                        _time.sleep(1)
                        jsonld_text = page.evaluate("""() => {
                            const el = document.querySelector('script[type="application/ld+json"]');
                            return el ? el.textContent : '';
                        }""")
                        if jsonld_text:
                            data = json.loads(jsonld_text)
                            date_posted_raw = data.get("datePosted", "")
                            if date_posted_raw:
                                posted_date = date_posted_raw[:10]
                                parsed = _parse_iso_date(posted_date)
                                if parsed:
                                    days_ago = (date.today() - parsed).days
                                    posted_display = _relative_display(days_ago)

                            base = data.get("baseSalary") or data.get("estimatedSalary")
                            if isinstance(base, dict):
                                currency = base.get("currency", "")
                                val = base.get("value", {})
                                if isinstance(val, dict):
                                    lo = val.get("minValue", "")
                                    hi = val.get("maxValue", "")
                                    if lo and hi:
                                        salary = f"{currency} {_fmt_num(lo)} - {_fmt_num(hi)}"
                                    elif lo:
                                        salary = f"{currency} {_fmt_num(lo)}+"
                                elif val:
                                    salary = f"{currency} {_fmt_num(val)}"

                            desc_html = data.get("description", "")
                            if desc_html:
                                description = _truncate(_clean_html(desc_html))

                        if not salary:
                            sal_text = page.evaluate("""() => {
                                const el = document.querySelector('#salaryInfoAndJobType, span[class*="salary"], div[class*="salary"]');
                                return el ? el.innerText : '';
                            }""")
                            if sal_text:
                                salary = sal_text.strip()

                        if not description:
                            desc_html = page.evaluate("""() => {
                                const el = document.querySelector('#jobDescriptionText');
                                return el ? el.innerHTML : '';
                            }""")
                            if desc_html:
                                description = _truncate(_clean_html(desc_html))
                    except Exception as e:
                        print(f"[Indeed detail] {e}")

                if days_ago > RECENT_DAYS:
                    continue

                jobs.append({
                    "title":       title,
                    "company":     company,
                    "location":    location_text,
                    "link":        view_link,
                    "source":      "Indeed",
                    "posted":      posted_display,
                    "posted_date": posted_date,
                    "description": description,
                    "salary":      salary,
                })

                if len(jobs) >= max_results:
                    break

            browser.close()
        return jobs
    except Exception as e:
        print(f"[Indeed Playwright] {e}")
        return []
