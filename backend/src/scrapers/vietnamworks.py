import re
from datetime import date, timedelta
from urllib.parse import unquote

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _parse_iso_date, _relative_display, _clean_html, _extract_html, _truncate

_VIETNAMWORKS_CITY_CODES: dict[str, str] = {
    "ho chi minh": "29",
    "hcm":         "29",
    "hồ chí minh": "29",
    "hanoi":       "24",
    "ha noi":      "24",
    "hà nội":      "24",
    "da nang":     "34",
    "danang":      "34",
    "đà nẵng":     "34",
}

def scrape_vietnamworks(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    """
    Scrapes VietnamWorks (React SPA) using Playwright.
    Only returns jobs posted within RECENT_DAYS days.
    """
    keyword_slug = keyword.strip().lower().replace(" ", "-")
    city_code    = _vietnamworks_city_code(location or "Ho Chi Minh City")
    url = f"https://www.vietnamworks.com/viec-lam?q={keyword_slug}&l={city_code}"
    return _vietnamworks_playwright(url, max_results)

def _vietnamworks_city_code(location: str) -> str:
    """Return the VietnamWorks city code for a given location string."""
    key = location.strip().lower()
    for candidate, code in _VIETNAMWORKS_CITY_CODES.items():
        if candidate in key:
            return code
    return "29"


def _vietnamworks_playwright(url: str, max_results: int) -> list[dict]:
    """Scrape VietnamWorks job listings and detail pages via headless Chromium."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                locale="vi-VN",
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                page.wait_for_selector("div.new-job-card, div.job-item, article[class*='job']", timeout=12000)
            except Exception:
                pass
            soup = BeautifulSoup(page.content(), "html.parser")
            jobs = _parse_vietnamworks(soup, max_results)

            for job in jobs:
                try:
                    page.goto(job["link"], wait_until="domcontentloaded", timeout=20000)
                    try:
                        page.wait_for_function(
                            """() => {
                                const headings = Array.from(document.querySelectorAll('h2, h3, h4, strong'));
                                const sec = headings.find(h => /Mô tả|Yêu cầu|Job Description|Requirements/i.test(h.textContent));
                                if (sec) {
                                    const sib = sec.nextElementSibling;
                                    if (sib && sib.innerText && sib.innerText.trim().length > 20) return true;
                                }
                                const desc = document.querySelector('[class*="description"], [class*="job-detail"]');
                                return desc && desc.innerText && desc.innerText.trim().length > 50;
                            }""",
                            timeout=12000,
                        )
                    except Exception:
                        pass
                    try:
                        expand_btn = page.query_selector("button:has-text('Xem đầy đủ'), a:has-text('Xem đầy đủ'), span:has-text('Xem đầy đủ')")
                        if expand_btn:
                            expand_btn.click()
                            page.wait_for_timeout(800)
                    except Exception:
                        pass
                    desc_html = page.evaluate("""() => {
                        const jobSections = Array.from(document.querySelectorAll('h2, h3, h4, strong'))
                            .filter(h => /Mô tả|Yêu cầu|Quyền lợi|Thông tin|Kỹ năng|Job Description|Requirements|Benefits/i.test(h.textContent));

                        if (jobSections.length === 0) {
                            const container = document.querySelector('[class*="description"]:not(meta):not(script)');
                            return container ? container.innerHTML : '';
                        }

                        let ancestor = jobSections[0].parentElement;
                        while (ancestor) {
                            if (jobSections.every(h => ancestor.contains(h))) break;
                            ancestor = ancestor.parentElement;
                        }
                        if (!ancestor) {
                            const sib = jobSections[0].nextElementSibling;
                            return sib ? sib.innerHTML : '';
                        }

                        const _isNoise = (el) => {
                            if (jobSections.some(h => el.contains(h))) return false;
                            const t = el.textContent.trim();
                            return /Các phúc lợi dành cho bạn|Thông tin việc làm|NGÀY ĐĂNG|CẤP BẬC|KỸ NĂNG|Từ khoá|Xem thêm việc làm|Chia sẻ|Báo cáo/i.test(t);
                        };

                        let html = '';
                        let started = false;
                        for (const child of ancestor.children) {
                            if (!started && jobSections.some(h => child.contains(h) || child === h)) {
                                started = true;
                            }
                            if (started) {
                                if (_isNoise(child)) break;
                                const c = child.cloneNode(true);
                                c.querySelectorAll('button, a, span, p').forEach(el => {
                                    if (/Xem đầy đủ/i.test(el.innerText || el.textContent)) el.remove();
                                });
                                html += c.outerHTML;
                            }
                        }
                        return html || ancestor.innerHTML;
                    }""")
                    desc = _truncate(_clean_html(desc_html)) if desc_html and desc_html.strip() else ""
                    job["description"] = desc
                    if not desc:
                        print(f"[VietnamWorks desc] empty for {job['link']}")
                except Exception as e:
                    print(f"[VietnamWorks desc] {e}")

            browser.close()
        return jobs
    except Exception as e:
        print(f"[VietnamWorks Playwright] {e}")
        return []


def _parse_vietnamworks_date(text: str) -> tuple[int, str, str]:
    """Parse a VietnamWorks date string into (days_ago, iso_date, display_text)."""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        try:
            d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            days_ago = (date.today() - d).days
            return days_ago, d.isoformat(), _relative_display(days_ago)
        except Exception:
            pass

    lowered = text.lower()
    if "hôm nay" in lowered or "vừa đăng" in lowered or "giờ" in lowered:
        return 0, date.today().isoformat(), _relative_display(0)
    m2 = re.search(r"(\d+)\s*ngày", lowered)
    if m2:
        n = int(m2.group(1))
        return n, (date.today() - timedelta(days=n)).isoformat(), _relative_display(n)
    m2 = re.search(r"(\d+)\s*tuần", lowered)
    if m2:
        n = int(m2.group(1))
        days = n * 7
        return days, (date.today() - timedelta(days=days)).isoformat(), _relative_display(days)
    m2 = re.search(r"(\d+)\s*tháng", lowered)
    if m2:
        n = int(m2.group(1))
        days = n * 30
        return days, (date.today() - timedelta(days=days)).isoformat(), _relative_display(days)
    return 9999, "", ""


def _parse_vietnamworks(soup: BeautifulSoup, max_results: int) -> list[dict]:
    """Parse job card elements from a VietnamWorks search results page."""
    cards = (
        soup.select("div.new-job-card")
        or soup.select("div.job-item")
        or soup.select("article[class*='job']")
        or soup.select("li[class*='job']")
    )
    jobs = []

    _SALARY_PAT = re.compile(
        r"triệu|VND|USD|\$|₫|thỏa thuận|thoả thuận|negotiable|competitive|đ/tháng|tr/tháng",
        re.IGNORECASE,
    )

    for card in cards:
        links = card.select("a")
        title_link = next((l for l in links if l.get_text(strip=True)), None)
        if not title_link:
            continue

        title = title_link.get_text(strip=True)
        raw_href = title_link.get("href", "").split("?")[0]
        if not raw_href:
            continue
        href = raw_href if raw_href.startswith("http") else "https://www.vietnamworks.com" + raw_href

        company_el = card.select_one("a[href*='/nha-tuyen-dung/']")
        company = company_el.get_text(strip=True) if company_el else "N/A"

        full_text = card.get_text(separator="|", strip=True)
        parts = [p.strip() for p in full_text.split("|") if p.strip()]

        posted_text = ""
        for part in parts:
            if "cập nhật" in part.lower() or re.search(r"\d{1,2}/\d{1,2}/\d{4}", part):
                posted_text = part
                break

        days_ago, posted_date, posted_display = _parse_vietnamworks_date(posted_text)

        if not posted_date:
            time_el = card.select_one("time[datetime]")
            if time_el:
                posted_date = time_el.get("datetime", "")[:10]
                parsed = _parse_iso_date(posted_date)
                days_ago = (date.today() - parsed).days if parsed else 9999
                posted_display = _relative_display(days_ago) if parsed else ""

        if days_ago > RECENT_DAYS:
            continue

        location = ""
        for loc_sel in (
            "div[class*='location']",
            "span[class*='location']",
            "div[class*='address']",
            "span[class*='address']",
        ):
            loc_el = card.select_one(loc_sel)
            if loc_el:
                t = loc_el.get_text(strip=True)
                if t and not _SALARY_PAT.search(t):
                    location = t
                    break

        if not location:
            date_idx = next(
                (i for i, p in enumerate(parts)
                 if "cập nhật" in p.lower() or re.search(r"\d{1,2}/\d{1,2}/\d{4}", p)),
                -1,
            )
            for i in range(date_idx - 1, -1, -1):
                if not _SALARY_PAT.search(parts[i]) and parts[i] != title and parts[i] != company:
                    location = parts[i]
                    break

        salary = ""
        sal_el = card.select_one("span[class*='salary'], div[class*='salary']")
        if sal_el:
            salary = sal_el.get_text(strip=True)
        else:
            for part in parts:
                if _SALARY_PAT.search(part):
                    salary = part
                    break

        logo_url = ""
        for img_el in card.select("img[alt]"):
            for candidate in [img_el.get("srcset", ""), img_el.get("src", "")]:
                m = re.search(r"url=([^&\s,]+)", candidate)
                if m:
                    decoded = unquote(m.group(1))
                    if "banner-default" in decoded:
                        continue
                    logo_url = decoded
                    break
            if logo_url:
                break

        jobs.append({
            "title":       title,
            "company":     company,
            "location":    location,
            "link":        href,
            "source":      "VietnamWorks",
            "posted":      posted_display,
            "posted_date": posted_date,
            "description": "",
            "logo":        logo_url,
            "salary":      salary,
        })

        if len(jobs) >= max_results:
            break

    return jobs
