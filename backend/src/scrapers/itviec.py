import re
from datetime import date, timedelta

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _relative_display, _clean_html, _extract_html, _truncate

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
    url = f"https://itviec.com/it-jobs/{keyword_slug}/{city_slug}"
    return _itviec_playwright(url, max_results)

def _itviec_city_slug(location: str) -> str:
    """Return the ITViec URL city slug for a given location string."""
    key = location.strip().lower()
    for candidate, slug in _ITVIEC_CITY_SLUGS.items():
        if candidate in key:
            return slug
    return "ho-chi-minh-hcm"


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
    """Scrape ITViec job listings and detail pages via headless Chromium."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                locale="en-US",
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                page.wait_for_selector("div.job-card", timeout=10000)
            except Exception:
                pass

            list_html = page.content()
            soup = BeautifulSoup(list_html, "html.parser")
            jobs = _parse_itviec_cards(soup)

            jobs = [j for j in jobs if j["_days_ago"] <= RECENT_DAYS][:max_results]

            for job in jobs:
                content_path  = job.pop("_content_url", "")
                days_ago      = job.pop("_days_ago", 9999)
                card_location = job.pop("_card_location", "")
                job["posted_date"] = (
                    (date.today() - timedelta(days=days_ago)).isoformat()
                    if days_ago < 9999 else ""
                )
                job["description"] = ""
                detail_url = job.get("link", "")
                if detail_url:
                    try:
                        page.goto(detail_url, wait_until="domcontentloaded", timeout=15000)
                        try:
                            page.wait_for_selector(
                                "div.job-detail__body, div[class*='salary'], span[class*='salary']",
                                timeout=6000,
                            )
                        except Exception:
                            pass
                        detail_soup = BeautifulSoup(page.content(), "html.parser")

                        salary_text = ""
                        for sal_sel in (
                            "div.job-detail__salary span",
                            "div[class*='salary'] span",
                            "span[class*='salary']",
                            "div.salary-text",
                            "div.job-detail__info-salary",
                        ):
                            sal_el = detail_soup.select_one(sal_sel)
                            if sal_el:
                                t = sal_el.get_text(strip=True)
                                if t and "sign in" not in t.lower():
                                    salary_text = t
                                    break
                        if salary_text:
                            job["salary"] = salary_text

                        loc = _parse_itviec_location(detail_soup)
                        if loc:
                            job["location"] = loc

                        desc, loc_from_desc = _parse_itviec_description(page.content())
                        job["description"] = desc
                        if loc_from_desc and not job.get("location"):
                            job["location"] = loc_from_desc
                    except Exception as e:
                        print(f"[ITViec detail] {e}")
                        job["description"] = ""

                if not job.get("location") and card_location:
                    job["location"] = card_location

            browser.close()
        return jobs
    except Exception as e:
        print(f"[ITViec Playwright] {e}")
        return []


def _parse_itviec_location(soup: BeautifulSoup) -> str:
    """Extract city name(s) from an ITViec detail page, joined by " | "."""
    _CITY_PATTERNS = [
        ("Ha Noi",      re.compile(r"\bHa\s*Noi\b|\bHà\s*Nội\b",             re.IGNORECASE)),
        ("Ho Chi Minh", re.compile(r"\bHo\s*Chi\s*Minh\b|\bHồ\s*Chí\s*Minh\b", re.IGNORECASE)),
        ("Da Nang",     re.compile(r"\bDa\s*Nang\b|\bĐà\s*Nẵng\b",           re.IGNORECASE)),
        ("Hai Phong",   re.compile(r"\bHai\s*Phong\b|\bHải\s*Phòng\b",        re.IGNORECASE)),
        ("Can Tho",     re.compile(r"\bCan\s*Tho\b|\bCần\s*Thơ\b",           re.IGNORECASE)),
        ("Binh Duong",  re.compile(r"\bBinh\s*Duong\b|\bBình\s*Dương\b",      re.IGNORECASE)),
    ]
    _ADDR_PAT = re.compile(r"Tầng|Đường|Phường|Quận|Street|Floor|số\s+\d", re.IGNORECASE)

    cities: list[str] = []
    for el in soup.find_all(["p", "span", "div", "li", "a"]):
        if len(el.find_all(recursive=False)) > 2:
            continue
        text = el.get_text(separator=" ", strip=True)
        if not _ADDR_PAT.search(text):
            continue
        for city_name, pattern in _CITY_PATTERNS:
            if pattern.search(text) and city_name not in cities:
                cities.append(city_name)

    if cities:
        return " | ".join(cities)

    _WORK_CONTEXT = re.compile(r"At office|office|Hybrid|Remote|On-site", re.IGNORECASE)
    full_text = soup.get_text(separator="\n")
    for line in full_text.splitlines():
        line = line.strip()
        if not _WORK_CONTEXT.search(line):
            continue
        for city_name, pattern in _CITY_PATTERNS:
            if pattern.search(line) and city_name not in cities:
                cities.append(city_name)

    return " | ".join(cities)


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


def _parse_itviec_description(html: str) -> tuple[str, str]:
    """Extract (description_html, location) from an ITViec job detail page."""
    soup = BeautifulSoup(html, "html.parser")
    location = _parse_itviec_location(soup)

    heading = None
    for tag in ("h2", "h3", "h4"):
        heading = soup.find(tag, string=re.compile(r"Job\s*description", re.IGNORECASE))
        if heading:
            break

    if heading:
        _JOB_SECTION = re.compile(
            r"Your skills|Why you.ll love|Skills and experience|Benefits|Quyền lợi",
            re.IGNORECASE,
        )
        node = heading
        while node.parent and node.parent.name not in ("body", "[document]"):
            node = node.parent
            siblings_text = " ".join(
                s.get_text() for s in node.next_siblings if hasattr(s, "get_text")
            )
            if _JOB_SECTION.search(siblings_text):
                parts = [str(node)]
                for sib in node.next_siblings:
                    parts.append(str(sib))
                desc = "".join(parts).strip()
                if desc:
                    return _truncate(_clean_html(desc)), location
                break

        parts = [str(heading)]
        for sib in heading.next_siblings:
            parts.append(str(sib))
        desc = "".join(parts).strip()
        if desc:
            return _truncate(_clean_html(desc)), location

    for sel in ("div.preview-job-content", "div.job-content", "div[class*='description']"):
        el = soup.select_one(sel)
        if el:
            html_content = _extract_html(el)
            if html_content:
                return _truncate(html_content), location

    paragraphs = soup.find_all("p")
    if paragraphs:
        html_parts = [str(p) for p in paragraphs if p.get_text(strip=True)]
        if html_parts:
            return _truncate(_clean_html("".join(html_parts))), location

    return "", location
