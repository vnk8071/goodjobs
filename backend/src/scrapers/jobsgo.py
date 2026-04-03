import re
import time as _time
from datetime import date, timedelta

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _relative_display, _clean_html, _extract_html, _truncate

_JOBSGO_CITY_SLUGS: dict[str, str] = {
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


def scrape_jobsgo(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    if location.strip().lower() == "remote":
        keyword  = f"remote {keyword}"
        location = "Ho Chi Minh City"
    keyword_slug = keyword.strip().lower().replace(" ", "-")
    city_slug    = _jobsgo_city_slug(location or "Ho Chi Minh City")
    if city_slug is None:
        return []
    url = f"https://jobsgo.vn/viec-lam-{keyword_slug}-tai-{city_slug}.html"
    return _jobsgo_playwright(url, max_results)


def scrape_jobsgo_detail_one(job: dict, cooldown: float) -> None:
    if cooldown > 0:
        _time.sleep(cooldown)
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            ctx = browser.new_context(user_agent=HEADERS["User-Agent"], locale="vi-VN")
            page = ctx.new_page()
            try:
                page.goto(job["link"], wait_until="domcontentloaded", timeout=20000)
                _time.sleep(1)
                title_text = page.title()
                if "just a moment" in title_text.lower():
                    return
                desc = page.evaluate("""() => {
                    const card = document.querySelector('.job-detail-card');
                    if (card && card.innerHTML.trim()) return card.innerHTML.trim();
                    const tab = document.querySelector('.tab-content');
                    if (tab && tab.innerHTML.trim()) return tab.innerHTML.trim();
                    return '';
                }""")
                if desc:
                    job["description"] = _truncate(_clean_html(desc))
                    job["summary_description"] = None  # Filled by background task
                if not job.get("logo"):
                    logo = page.evaluate("""() => {
                        const el = document.querySelector('.company-logo img, .employer-logo img, [class*="logo"] img');
                        return el ? (el.getAttribute('src') || '') : '';
                    }""")
                    if logo:
                        if not logo.startswith("http"):
                            logo = "https://jobsgo.vn" + logo
                        job["logo"] = logo
            except Exception as e:
                print(f"[JobsGo desc] {job['link']}: {e}")
            finally:
                try:
                    ctx.close()
                except Exception:
                    pass
                browser.close()
    except Exception as e:
        print(f"[JobsGo detail] {e}")


def _jobsgo_city_slug(location: str) -> str | None:
    key = location.strip().lower()
    for candidate, slug in _JOBSGO_CITY_SLUGS.items():
        if candidate in key:
            return slug
    return None


def _jobsgo_playwright(url: str, max_results: int) -> list[dict]:
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            try:
                ctx = browser.new_context(user_agent=HEADERS["User-Agent"], locale="vi-VN")
                page = ctx.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_selector(
                        ".job-item, .job-card, [class*='job-item'], [class*='job-card']",
                        timeout=12000,
                    )
                except Exception:
                    pass

                title_text = page.title()
                if "just a moment" in title_text.lower() or "security" in title_text.lower():
                    return []

                jobs = _extract_jobsgo_cards_js(page)
                ctx.close()
            finally:
                browser.close()

        jobs = [j for j in jobs if j["_days_ago"] <= RECENT_DAYS][:max_results]
        for job in jobs:
            days_ago = job.pop("_days_ago", 9999)
            job["posted_date"] = (
                (date.today() - timedelta(days=days_ago)).isoformat()
                if days_ago < 9999 else ""
            )
            job["description"] = ""
            job["summary_description"] = ""  # No description for listing-only
        return jobs
    except Exception as e:
        print(f"[JobsGo Playwright] {e}")
        return []


def _extract_jobsgo_cards_js(page) -> list[dict]:
    cards_data = page.evaluate("""() => {
        const cards = Array.from(document.querySelectorAll('.job-card'));
        return cards.map(card => {
            const titleEl = card.querySelector('.job-title');
            const linkEl = card.querySelector('a[href*="/viec-lam/"]');
            const companyEl = card.querySelector('.company-title');
            const logoEl = card.querySelector('img');
            // salary and location are inside the salary/location row spans
            const spans = Array.from(card.querySelectorAll('.mt-1.text-primary span'));
            const salary = spans[0] ? spans[0].textContent.trim() : '';
            const location = spans[2] ? spans[2].textContent.trim() : (spans[1] ? spans[1].textContent.trim() : '');
            // date badge has title="Thời gian cập nhật"
            const dateBadge = card.querySelector('span[title="Thời gian cập nhật"]');
            return {
                title: titleEl ? titleEl.textContent.trim() : '',
                href: linkEl ? linkEl.getAttribute('href') : '',
                company: companyEl ? companyEl.textContent.trim() : 'N/A',
                location: location,
                salary: salary,
                postedText: dateBadge ? dateBadge.textContent.trim() : '',
                logo: logoEl ? (logoEl.getAttribute('src') || logoEl.getAttribute('data-src') || '') : '',
            };
        });
    }""")

    jobs = []
    for c in cards_data:
        title = c.get("title", "").strip()
        href  = c.get("href", "") or ""
        if not title or not href:
            continue
        if not href.startswith("http"):
            href = "https://jobsgo.vn" + href

        logo = c.get("logo", "")
        if logo and not logo.startswith("http"):
            logo = "https://jobsgo.vn" + logo

        posted_text = c.get("postedText", "")
        days_ago    = _jobsgo_days_ago(posted_text)

        jobs.append({
            "title":       title,
            "company":     c.get("company", "N/A"),
            "location":    c.get("location", ""),
            "link":        href,
            "source":      "JobsGo",
            "posted":      _relative_display(days_ago) if days_ago < 9999 else posted_text,
            "logo":        logo,
            "salary":      c.get("salary", ""),
            "_days_ago":   days_ago,
        })

    return jobs


def _jobsgo_days_ago(text: str) -> int:
    text = text.lower()
    if "hôm nay" in text or "vừa đăng" in text or "giờ" in text or "phút" in text:
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
    if "today" in text or "hour" in text or "just now" in text:
        return 0
    m = re.search(r"(\d+)\s*day", text)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*week", text)
    if m:
        return int(m.group(1)) * 7
    return 9999
