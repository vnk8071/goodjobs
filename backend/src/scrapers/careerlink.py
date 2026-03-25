import re
import time as _time
from datetime import date, timedelta

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _relative_display, _clean_html, _truncate

_CAREERLINK_CITY_PARAMS: dict[str, tuple[str, str]] = {
    "ho chi minh": ("ho-chi-minh", "HCM"),
    "hcm":         ("ho-chi-minh", "HCM"),
    "hồ chí minh": ("ho-chi-minh", "HCM"),
    "hanoi":       ("ha-noi",      "HN"),
    "ha noi":      ("ha-noi",      "HN"),
    "hà nội":      ("ha-noi",      "HN"),
    "da nang":     ("da-nang",     "DN"),
    "danang":      ("da-nang",     "DN"),
    "đà nẵng":     ("da-nang",     "DN"),
}


def scrape_careerlink(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    keyword_slug = keyword.strip().lower().replace(" ", "-")
    params = _careerlink_city_params(location or "Ho Chi Minh City")
    if params is None:
        return []
    city_slug, area_code = params
    url = f"https://www.careerlink.vn/tim-viec-lam-tai/k/{keyword_slug}/{city_slug}/{area_code}"
    return _careerlink_playwright(url, area_code, max_results)


def scrape_careerlink_detail_one(job: dict, cooldown: float) -> None:
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
                    const parts = [];
                    document.querySelectorAll('.rich-text-content').forEach(el => parts.push(el.innerHTML.trim()));
                    if (parts.length) return parts.join('<hr>');
                    const el = document.querySelector('#job-description');
                    return el ? el.innerHTML.trim() : '';
                }""")
                if desc:
                    job["description"] = _truncate(_clean_html(desc))
                    job["summary_description"] = None  # Filled by background task
                if not job.get("logo"):
                    logo = page.evaluate("""() => {
                        const el = document.querySelector('.job-logo img');
                        return el ? (el.getAttribute('src') || '') : '';
                    }""")
                    if logo:
                        if not logo.startswith("http"):
                            logo = "https://www.careerlink.vn" + logo
                        job["logo"] = logo
            except Exception as e:
                print(f"[CareerLink desc] {job['link']}: {e}")
            finally:
                try:
                    ctx.close()
                except Exception:
                    pass
                browser.close()
    except Exception as e:
        print(f"[CareerLink detail] {e}")


def _careerlink_city_params(location: str) -> tuple[str, str] | None:
    key = location.strip().lower()
    for candidate, params in _CAREERLINK_CITY_PARAMS.items():
        if candidate in key:
            return params
    return None


def _careerlink_playwright(url: str, area_code: str, max_results: int) -> list[dict]:
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            try:
                ctx = browser.new_context(user_agent=HEADERS["User-Agent"], locale="vi-VN")
                page = ctx.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_selector(".job-item", timeout=12000)
                except Exception:
                    pass

                title_text = page.title()
                if "just a moment" in title_text.lower() or "security" in title_text.lower():
                    return []

                jobs = _extract_careerlink_cards_js(page, area_code)
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
        print(f"[CareerLink Playwright] {e}")
        return []


def _extract_careerlink_cards_js(page, area_code: str) -> list[dict]:
    cards_data = page.evaluate("""() => {
        const cards = Array.from(document.querySelectorAll('.job-item'));
        return cards.map(card => {
            const linkEl = card.querySelector('a.job-link');
            const titleEl = card.querySelector('.job-name');
            const companyEl = card.querySelector('a.job-company');
            const locEl = card.querySelector('.job-location');
            const salaryEl = card.querySelector('.job-salary');
            const logoEl = card.querySelector('.job-logo img');
            const dateEl = card.querySelector('.cl-datetime');
            return {
                href: linkEl ? linkEl.getAttribute('href') : '',
                title: titleEl ? titleEl.textContent.trim() : '',
                company: companyEl ? companyEl.textContent.trim() : 'N/A',
                location: locEl ? locEl.textContent.trim() : '',
                salary: salaryEl ? salaryEl.textContent.trim() : '',
                logo: logoEl ? (logoEl.getAttribute('src') || '') : '',
                postedText: dateEl ? dateEl.textContent.trim() : '',
                datetime: dateEl ? (dateEl.getAttribute('data-datetime') || '') : '',
            };
        });
    }""")

    jobs = []
    for c in cards_data:
        title = c.get("title", "").strip()
        href  = c.get("href", "").strip()
        if not title or not href:
            continue
        if not href.startswith("http"):
            href = "https://www.careerlink.vn" + href
        # strip query string, add area_code param cleanly
        href = href.split("?")[0] + f"?area_code={area_code}&source=site"

        logo = c.get("logo", "")
        if logo and not logo.startswith("http"):
            logo = "https://www.careerlink.vn" + logo

        # prefer unix timestamp if available
        dt = c.get("datetime", "")
        posted_text = c.get("postedText", "")
        days_ago = _careerlink_days_ago_from_ts(dt) if dt else _careerlink_days_ago(posted_text)

        jobs.append({
            "title":       title,
            "company":     c.get("company", "N/A"),
            "location":    c.get("location", "").strip(),
            "link":        href,
            "source":      "CareerLink",
            "posted":      _relative_display(days_ago) if days_ago < 9999 else posted_text,
            "logo":        logo,
            "salary":      c.get("salary", "").strip(),
            "_days_ago":   days_ago,
        })

    return jobs


def _careerlink_days_ago_from_ts(ts_str: str) -> int:
    try:
        import time as _t
        ts = int(ts_str)
        days = int((_t.time() - ts) / 86400)
        return max(0, days)
    except Exception:
        return 9999


def _careerlink_days_ago(text: str) -> int:
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
    if "today" in text or "hour" in text:
        return 0
    m = re.search(r"(\d+)\s*day", text)
    if m:
        return int(m.group(1))
    return 9999
