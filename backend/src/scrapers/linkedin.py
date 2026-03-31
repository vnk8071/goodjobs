import json
import re
import time as _time
from datetime import date, timedelta
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS, new_session
from ..logger import log_app
from ..utils import _parse_iso_date, _relative_display, _clean_html, _extract_html, _truncate, _fmt_num

_LINKEDIN_GEO_IDS: dict[str, list[str]] = {
    "Ho Chi Minh City": ["103697962", "102267004", "90010187", "109912426", "100126839"],
    "Hanoi":            ["105790653", "90010186", ],
    "Da Nang":          ["115237480", "102868289", "105668258", "90010189"],
    "Vietnam":          ["104195383"],
    "Remote":           ["104195383"],
}

_LINKEDIN_LOCATION_MAP: dict[str, str] = {
    "ha noi":       "Hanoi",
    "hanoi":        "Hanoi",
    "hà nội":       "Hanoi",
    "ho chi minh":  "Ho Chi Minh City",
    "hcm":          "Ho Chi Minh City",
    "hồ chí minh":  "Ho Chi Minh City",
    "da nang":      "Da Nang",
    "danang":       "Da Nang",
    "đà nẵng":      "Da Nang",
    "remote":       "Remote",
}

_LINKEDIN_PAGE_SIZE   = 25
_LINKEDIN_MAX_RESULTS = 75

_LINKEDIN_BOILERPLATE = re.compile(
    r"Applicants:.*$|To all Staffing and Recruiting Agencies.*$"
    r"|Qualcomm expects its employees.*$|If you would like more information about this role.*$",
    re.IGNORECASE | re.DOTALL,
)


def scrape_linkedin(keyword: str, location: str = "Ho Chi Minh City", since_seconds: int | None = None) -> list[dict]:
    """Scrape LinkedIn public jobs sorted by date using geoId for precise location matching.

    Also searches one keyword variant (engineer↔developer, ai↔ml) to surface jobs
    LinkedIn ranks differently under alternate terms. Deduplicates by link.
    Returns jobs without descriptions (fast). Call scrape_linkedin_details() to fill them.

    since_seconds: when set, adds f_TPR=r{since_seconds} to only fetch jobs posted in that window.
    """
    mapped_location = _linkedin_location(location or "Ho Chi Minh City")
    geo_ids         = _LINKEDIN_GEO_IDS.get(mapped_location)
    tpr_param       = f"&f_TPR=r{since_seconds}" if since_seconds else ""
    remote_param    = "&f_WT=2" if mapped_location == "Remote" else ""

    def _fetch_all_pages(kw: str) -> list[dict]:
        kw_enc    = quote_plus(kw)
        all_jobs: list[dict] = []
        seen: set[str] = set()
        loc_ids = geo_ids if geo_ids else [None]
        for geo_id in loc_ids:
            start = 0
            while len(all_jobs) < _LINKEDIN_MAX_RESULTS:
                if geo_id:
                    loc_param = f"&geoId={geo_id}"
                else:
                    loc_param = f"&location={quote_plus(mapped_location)}"
                url = (
                    f"https://www.linkedin.com/jobs/search/"
                    f"?keywords={kw_enc}"
                    f"{loc_param}"
                    f"&sortBy=DD"
                    f"{tpr_param}"
                    f"{remote_param}"
                    f"&start={start}"
                )
                page_jobs = _linkedin_requests(url, _LINKEDIN_PAGE_SIZE * 2)
                if not page_jobs:
                    if start == 0:
                        log_app(f"[LinkedIn] requests blocked for geoId={geo_id} — trying Playwright")
                        page_jobs = _linkedin_playwright(url, _LINKEDIN_PAGE_SIZE * 2)
                    if not page_jobs:
                        break
                added = 0
                for j in page_jobs:
                    if j["link"] not in seen:
                        seen.add(j["link"])
                        all_jobs.append(j)
                        added += 1
                if added == 0 or len(page_jobs) < _LINKEDIN_PAGE_SIZE:
                    break
                start += _LINKEDIN_PAGE_SIZE
            pages = start // _LINKEDIN_PAGE_SIZE + 1
            loc_label = f"geoId={geo_id}" if geo_id else f"location={mapped_location!r}"
            log_app(f"[LinkedIn] '{kw}' {loc_label} → {len(all_jobs)} jobs across {pages} page(s){f' (f_TPR=r{since_seconds}s)' if since_seconds else ''}")
        return all_jobs

    all_jobs: list[dict] = _fetch_all_pages(keyword)
    seen_links: set[str] = {j["link"] for j in all_jobs}

    for variant in _linkedin_keyword_variants(keyword):
        variant_jobs = _fetch_all_pages(variant)
        for j in variant_jobs:
            if j["link"] not in seen_links:
                seen_links.add(j["link"])
                all_jobs.append(j)

    return all_jobs


def scrape_linkedin_details(jobs: list[dict]) -> None:
    """Fill in descriptions for LinkedIn jobs in-place by fetching detail pages.

    Call after scrape_linkedin() to enrich results without blocking listing.
    """
    _time.sleep(5.0)
    for i, job in enumerate(jobs[:_LINKEDIN_MAX_RESULTS]):
        if i > 0:
            _time.sleep(2.0)
        desc, salary = _linkedin_fetch_detail(job["link"])
        if desc == "_RATE_LIMITED_":
            break
        if desc:
            job["description"] = desc
            job["summary_description"] = None  # Filled by background task
        if salary and not job["salary"]:
            job["salary"] = salary


def scrape_linkedin_detail_one(job: dict, cooldown: float) -> bool:
    """Fetch and fill description for a single LinkedIn job in-place.

    Returns False if rate-limited (caller should stop), True otherwise.
    """
    if cooldown > 0:
        _time.sleep(cooldown)
    desc, salary = _linkedin_fetch_detail(job["link"])
    if desc == "_RATE_LIMITED_":
        return False
    if desc:
        job["description"] = desc
        job["summary_description"] = None  # Filled by background task
    if salary and not job.get("salary"):
        job["salary"] = salary
    return True


def _linkedin_location(location: str) -> str:
    """Normalise a free-text location string to a canonical LinkedIn location name."""
    key = location.strip().lower()
    for candidate, mapped in _LINKEDIN_LOCATION_MAP.items():
        if candidate in key:
            return mapped
    return location


def _linkedin_keyword_variants(keyword: str) -> list[str]:
    """Return alternate keyword forms (engineer↔developer, ai↔ml) to broaden coverage."""
    kw = keyword.strip().lower()
    variants: list[str] = []
    swaps = [
        (r"\bengineer\b", "developer"),
        (r"\bdeveloper\b", "engineer"),
        (r"\bml\b",        "ai"),
        (r"\bai\b",        "ml"),
        (r"\bmachine learning\b", "ai"),
    ]
    for pattern, replacement in swaps:
        swapped = re.sub(pattern, replacement, kw)
        if swapped != kw and swapped not in variants:
            variants.append(swapped)
            break
    return variants


def _linkedin_requests(url: str, max_results: int) -> list[dict]:
    """Fetch LinkedIn job listings via plain HTTP requests. Returns [] on failure or block."""
    try:
        # Fresh session per call so LinkedIn cannot fingerprint a long-lived session.
        s = new_session()
        resp = s.get(url, timeout=12)
        if resp.status_code != 200:
            return []
        if "authwall" in resp.url or "login" in resp.url:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        return _parse_linkedin(soup, max_results)
    except Exception as e:
        print(f"[LinkedIn requests] {e}")
        return []


def _linkedin_playwright(url: str, max_results: int) -> list[dict]:
    """Fetch LinkedIn job listings via headless Chromium (fallback when requests are blocked)."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            try:
                page = browser.new_page()
                page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_selector("div.job-search-card", timeout=8000)
                except Exception:
                    pass
                try:
                    content = page.content()
                except Exception:
                    return []
            finally:
                try:
                    browser.close()
                except Exception:
                    pass
        soup = BeautifulSoup(content, "html.parser")
        return _parse_linkedin(soup, max_results)
    except Exception as e:
        print(f"[LinkedIn Playwright] {e}")
        return []


def _parse_linkedin(soup: BeautifulSoup, max_results: int) -> list[dict]:
    """Parse job cards from a LinkedIn search results page, filtering to RECENT_DAYS."""
    cards = soup.select("div.job-search-card")
    cutoff = date.today() - timedelta(days=RECENT_DAYS)
    jobs = []

    for card in cards:
        title_el   = card.select_one("h3.base-search-card__title")
        company_el = card.select_one("h4.base-search-card__subtitle a")
        loc_el     = card.select_one("span.job-search-card__location")
        link_el    = card.select_one("a.base-card__full-link")
        time_el    = card.select_one("time")

        if not (title_el and link_el):
            continue

        datetime_attr = time_el.get("datetime", "") if time_el else ""
        posted_text   = time_el.get_text(strip=True) if time_el else ""
        posted_date   = _parse_iso_date(datetime_attr)

        if posted_date and posted_date < cutoff:
            continue

        href = link_el.get("href", "").split("?")[0]
        href = href.replace("vn.linkedin", "linkedin")

        salary = ""
        sal_el = card.select_one("span.job-search-card__salary-info")
        if sal_el:
            salary = sal_el.get_text(strip=True)

        logo_url = ""
        logo_el = (card.select_one("img[data-delayed-url]")
                   or card.select_one("img[alt*='logo']")
                   or card.select_one(".base-search-card__media img"))
        if logo_el:
            logo_url = logo_el.get("data-delayed-url") or logo_el.get("src") or ""

        jobs.append({
            "title":       title_el.get_text(strip=True),
            "company":     company_el.get_text(strip=True) if company_el else "N/A",
            "location":    loc_el.get_text(strip=True) if loc_el else "",
            "link":        href,
            "source":      "LinkedIn",
            "posted":      posted_text,
            "posted_date": datetime_attr,
            "description": "",
            "logo":        logo_url,
            "salary":      salary,
        })

        if len(jobs) >= max_results:
            break

    return jobs


def _linkedin_strip_boilerplate(html: str) -> str:
    """Remove boilerplate paragraphs (agency disclaimers, policy notices) from LinkedIn HTML."""
    return _LINKEDIN_BOILERPLATE.sub("", html)


def _linkedin_extract_description_from_soup(soup: BeautifulSoup) -> str:
    """Extract job description HTML from a LinkedIn detail page, trying JSON-LD then DOM selectors."""
    jsonld_el = soup.find("script", {"type": "application/ld+json"})
    if jsonld_el and jsonld_el.string:
        try:
            data = json.loads(jsonld_el.string)
            desc_html = data.get("description", "")
            if desc_html:
                return _truncate(_clean_html(desc_html))
        except Exception:
            pass

    guest_el = soup.find("span", {"data-testid": "expandable-text-box"})
    if guest_el:
        return _truncate(_clean_html(_linkedin_strip_boilerplate(str(guest_el))))

    for sel in (
        "div.show-more-less-html__markup",
        "div.description__text",
        "section.description",
        "div.decorated-job-posting__details",
    ):
        el = soup.select_one(sel)
        if el:
            html = _extract_html(el)
            if html:
                return _truncate(html)
    return ""


def _is_bogus_salary(salary: str) -> bool:
    """Return True if the salary string contains clearly erroneous values (e.g. $1–$9,999,999)."""
    if not salary:
        return False
    nums = [float(n.replace(",", "")) for n in re.findall(r"[\d,]+\.?\d*", salary)]
    if len(nums) >= 2:
        lo, hi = nums[0], nums[-1]
        if lo <= 1 and hi >= 999_999:
            return True
        if lo > 0 and hi / lo >= 10_000:
            return True
    return False


def _linkedin_extract_salary_from_soup(soup: BeautifulSoup) -> str:
    """Extract salary text from a LinkedIn detail page, trying JSON-LD then DOM selectors."""
    jsonld_el = soup.find("script", {"type": "application/ld+json"})
    if jsonld_el and jsonld_el.string:
        try:
            data = json.loads(jsonld_el.string)
            base = data.get("baseSalary") or data.get("estimatedSalary")
            if isinstance(base, dict):
                currency = base.get("currency", "")
                val = base.get("value", {})
                if isinstance(val, dict):
                    lo = val.get("minValue", "")
                    hi = val.get("maxValue", "")
                    if lo and hi:
                        result = f"{currency} {_fmt_num(lo)} - {_fmt_num(hi)}"
                        return "" if _is_bogus_salary(result) else result
                    elif lo:
                        return f"{currency} {_fmt_num(lo)}+"
                elif val:
                    return f"{currency} {_fmt_num(val)}"
        except Exception:
            pass

    for sel in (
        "div.salary-main-container",
        "span[class*='salary']",
        "div[class*='salary']",
    ):
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if text:
                return text
    return ""


def _linkedin_fetch_detail(job_url: str) -> tuple[str, str]:
    """Fetch description and salary for one job via LinkedIn's guest API.

    Returns ("_RATE_LIMITED_", "") after 3 failed 429 retries.
    """
    m = re.search(r"-(\d{7,})$|/(?:view|jobPosting)/(\d+)", job_url)
    if not m:
        print(f"[LinkedIn desc guest API] could not extract job ID from: {job_url}")
        return "", ""
    job_id = m.group(1) or m.group(2)
    api_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"

    for attempt in range(3):
        try:
            s = new_session()
            resp = s.get(api_url, timeout=10)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 15 * (attempt + 1)))
                print(f"[LinkedIn desc guest API] {job_id} → 429, backing off {retry_after}s (attempt {attempt+1}/3)")
                _time.sleep(retry_after)
                continue
            if resp.status_code != 200:
                print(f"[LinkedIn desc guest API] {job_id} → HTTP {resp.status_code}")
                return "", ""
            soup = BeautifulSoup(resp.text, "html.parser")
            desc = _linkedin_extract_description_from_soup(soup)
            salary = _linkedin_extract_salary_from_soup(soup)
            if not desc:
                print(f"[LinkedIn desc guest API] {job_id} → no description found (html len={len(resp.text)})")
            return desc, salary
        except Exception as e:
            print(f"[LinkedIn desc guest API] {e}")
            return "", ""
    print(f"[LinkedIn desc guest API] {job_id} → rate limited after 3 retries, stopping")
    return "_RATE_LIMITED_", ""
