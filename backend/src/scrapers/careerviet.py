import re
from datetime import date, timedelta
from urllib.parse import unquote

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _relative_display, _clean_html, _extract_html, _truncate

_CAREERVIET_CITY_SLUGS: dict[str, str] = {
    "ho chi minh": "tai-ho-chi-minh",
    "hcm":         "tai-ho-chi-minh",
    "hồ chí minh": "tai-ho-chi-minh",
    "hanoi":       "tai-ha-noi",
    "ha noi":      "tai-ha-noi",
    "hà nội":      "tai-ha-noi",
    "da nang":     "tai-da-nang",
    "danang":      "tai-da-nang",
    "đà nẵng":     "tai-da-nang",
}

_CAREERVIET_CITY_CODES: dict[str, str] = {
    "ho chi minh": "kl8",
    "hcm":         "kl8",
    "hồ chí minh": "kl8",
    "hanoi":       "kl1",
    "ha noi":      "kl1",
    "hà nội":      "kl1",
    "da nang":     "kl3",
    "danang":      "kl3",
    "đà nẵng":     "kl3",
}

def scrape_careerviet(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """
    Scrapes CareerViet (Next.js SSR) using Playwright.
    Only returns jobs posted within RECENT_DAYS days.
    """
    keyword_slug = keyword.strip().lower().replace(" ", "-")
    city = _careerviet_city(location or "Ho Chi Minh City")
    if city is None:
        return []
    city_slug, city_code = city
    url = f"https://careerviet.vn/viec-lam/{keyword_slug}-{city_slug}-{city_code}-vi.html"
    return _careerviet_playwright(url, max_results)

def _careerviet_city(location: str) -> tuple[str, str] | None:
    """Return (city_slug, city_code) for the CareerViet URL from a location string."""
    key = location.strip().lower()
    for candidate in _CAREERVIET_CITY_SLUGS:
        if candidate in key:
            return _CAREERVIET_CITY_SLUGS[candidate], _CAREERVIET_CITY_CODES[candidate]
    return None


def _careerviet_playwright(url: str, max_results: int) -> list[dict]:
    """Scrape CareerViet job listings via headless Chromium (listing only, no detail pages)."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            try:
                context = browser.new_context(
                    user_agent=HEADERS["User-Agent"],
                    locale="vi-VN",
                )
                page = context.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_selector("div.job-item, div[class*='job-item']", timeout=12000)
                except Exception:
                    pass
                soup = BeautifulSoup(page.content(), "html.parser")
                jobs = _parse_careerviet(soup, max_results)
            finally:
                browser.close()
        return jobs
    except Exception as e:
        print(f"[CareerViet Playwright] {e}")
        return []


def scrape_careerviet_detail_one(job: dict, cooldown: float) -> None:
    """Fetch and fill description for a single CareerViet job in-place."""
    import time as _time
    if cooldown > 0:
        _time.sleep(cooldown)
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            try:
                context = browser.new_context(
                    user_agent=HEADERS["User-Agent"],
                    locale="vi-VN",
                )
                page = context.new_page()
                try:
                    page.goto(job["link"], wait_until="domcontentloaded", timeout=20000)
                    try:
                        page.wait_for_function(
                            """() => {
                                const rows = document.querySelectorAll('div.detail-row');
                                return Array.from(rows).some(r => {
                                    const h2 = r.querySelector('h2.detail-title, h2');
                                    return h2 && /Mô tả|Yêu cầu/i.test(h2.textContent) && r.innerText.trim().length > 80;
                                });
                            }""",
                            timeout=15000,
                        )
                    except Exception:
                        page.wait_for_timeout(2000)
                    desc_html = page.evaluate("""() => {
                        const rows = Array.from(document.querySelectorAll('div.detail-row'));
                        const jobRows = rows.filter(r => {
                            const h2 = r.querySelector('h2.detail-title, h2');
                            if (!h2) return false;
                            return /Mô tả|Yêu cầu|Quyền lợi|Địa điểm|Job Description|Requirements|Benefits|Location/i.test(h2.textContent);
                        });
                        if (jobRows.length > 0) return jobRows.map(r => r.outerHTML).join('');
                        const fck = document.querySelector('div.content_fck');
                        return fck && fck.innerText.trim().length > 50 ? fck.innerHTML : '';
                    }""")
                    desc = _truncate(_clean_html(desc_html)) if desc_html and desc_html.strip() else ""
                    if desc:
                        job["description"] = desc
                    else:
                        print(f"[CareerViet detail] empty description for {job['link']}")
                except Exception as e:
                    print(f"[CareerViet detail] {e}")
            finally:
                browser.close()
    except Exception as e:
        print(f"[CareerViet detail] {e}")


def _parse_careerviet_date(text: str) -> tuple[int, str, str]:
    """Parse a CareerViet date string into (days_ago, iso_date, display_text)."""
    text = text.strip()
    lowered = text.lower()
    if "hôm nay" in lowered or "vừa đăng" in lowered or "giờ" in lowered:
        return 0, date.today().isoformat(), _relative_display(0)
    m = re.search(r"(\d+)\s*ngày", lowered)
    if m:
        n = int(m.group(1))
        return n, (date.today() - timedelta(days=n)).isoformat(), _relative_display(n)
    m = re.search(r"(\d+)\s*tuần", lowered)
    if m:
        n = int(m.group(1)) * 7
        return n, (date.today() - timedelta(days=n)).isoformat(), _relative_display(n)
    m = re.search(r"(\d+)\s*tháng", lowered)
    if m:
        n = int(m.group(1)) * 30
        return n, (date.today() - timedelta(days=n)).isoformat(), _relative_display(n)
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        try:
            d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            days_ago = (date.today() - d).days
            return days_ago, d.isoformat(), _relative_display(days_ago)
        except Exception:
            pass
    return 9999, "", ""


def _parse_careerviet(soup: BeautifulSoup, max_results: int) -> list[dict]:
    """Parse job card elements from a CareerViet search results page."""
    cards = soup.select("div.job-item")
    jobs = []

    for card in cards:
        title_el = card.select_one("div.title h2 a.job_link")
        if not title_el:
            title_el = card.select_one("h2 a, div.title a")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        href  = title_el.get("href", "").split("?")[0]
        if not href:
            continue
        if not href.startswith("http"):
            href = "https://careerviet.vn" + href

        company_el = card.select_one("a.company-name")
        company = company_el.get_text(strip=True) if company_el else "N/A"

        loc_el = card.select_one("div.location ul li")
        location = loc_el.get_text(strip=True) if loc_el else ""

        salary_el = card.select_one("div.salary p")
        if salary_el:
            for em in salary_el.find_all("em"):
                em.decompose()
            salary = salary_el.get_text(strip=True).lstrip("Lương:").strip()
        else:
            salary = ""

        posted_text = ""
        time_lis = card.select("div.time ul li")
        for li in time_lis:
            if "Cập nhật" in li.get_text():
                t = li.select_one("time")
                posted_text = t.get_text(strip=True) if t else li.get_text(strip=True)
                break

        m = re.search(r"(\d{1,2})-(\d{1,2})-(\d{4})", posted_text)
        if m:
            try:
                d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                days_ago = (date.today() - d).days
                posted_date = d.isoformat()
                posted_display = _relative_display(days_ago)
            except Exception:
                days_ago, posted_date, posted_display = 9999, "", ""
        else:
            days_ago, posted_date, posted_display = _parse_careerviet_date(posted_text)

        if days_ago > RECENT_DAYS:
            continue

        logo_url = ""
        logo_el = card.select_one("div.img-job-logo img, a.logo img, img[alt]")
        if logo_el:
            for candidate in [logo_el.get("srcset", ""), logo_el.get("src", "")]:
                m = re.search(r"url=([^&\s,]+)", candidate)
                if m:
                    logo_url = unquote(m.group(1))
                    break
            if not logo_url:
                raw = logo_el.get("src", "")
                if raw and not raw.startswith("/_next"):
                    logo_url = raw if raw.startswith("http") else "https://careerviet.vn" + raw

        jobs.append({
            "title":       title,
            "company":     company,
            "location":    location,
            "link":        href,
            "source":      "CareerViet",
            "posted":      posted_display,
            "posted_date": posted_date,
            "description": "",
            "logo":        logo_url,
            "salary":      salary,
        })

        if len(jobs) >= max_results:
            break

    return jobs
