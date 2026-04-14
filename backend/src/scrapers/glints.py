import re
import time as _time

from ..constants import HEADERS, CHROMIUM_ARGS, RECENT_DAYS
from ..utils import _relative_display, _clean_html, _truncate

_GLINTS_CITY_PARAMS: dict[str, str] = {
    "ho chi minh": "locationName=Th%C3%A0nh+ph%E1%BB%91+H%E1%BB%93+Ch%C3%AD+Minh&lowestLocationLevel=1",
    "hcm":         "locationName=Th%C3%A0nh+ph%E1%BB%91+H%E1%BB%93+Ch%C3%AD+Minh&lowestLocationLevel=1",
    "hồ chí minh": "locationName=Th%C3%A0nh+ph%E1%BB%91+H%E1%BB%93+Ch%C3%AD+Minh&lowestLocationLevel=1",
    "hanoi":       "locationId=9f4287bd-76a1-4da6-9396-589ea47c93e2&locationName=H%C3%A0+N%E1%BB%99i&lowestLocationLevel=2",
    "ha noi":      "locationId=9f4287bd-76a1-4da6-9396-589ea47c93e2&locationName=H%C3%A0+N%E1%BB%99i&lowestLocationLevel=2",
    "hà nội":      "locationId=9f4287bd-76a1-4da6-9396-589ea47c93e2&locationName=H%C3%A0+N%E1%BB%99i&lowestLocationLevel=2",
}

_NON_SKILL_TAGS = {
    "full-time", "part-time", "internship", "contract", "freelance",
    "onsite", "hybrid", "remote/wfh",
    "no experience", "fresh graduate",
    "minimum bachelor's degree", "bachelor's degree", "master's degree",
}


_GLINTS_LOCATION_KEYWORDS: dict[str, list[str]] = {
    "ho chi minh": ["ho chi minh", "hồ chí minh", "hcm", "saigon", "sài gòn"],
    "hanoi":       ["hanoi", "ha noi", "hà nội", "hà nội"],
    "ha noi":      ["hanoi", "ha noi", "hà nội"],
    "hà nội":      ["hanoi", "ha noi", "hà nội"],
    "hcm":         ["ho chi minh", "hồ chí minh", "hcm", "saigon"],
    "hồ chí minh": ["ho chi minh", "hồ chí minh", "hcm", "saigon"],
}


def scrape_glints(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    if location.strip().lower() == "remote":
        location = "Ho Chi Minh City"
    effective_location = location or "Ho Chi Minh City"
    loc_param = _glints_loc_param(effective_location)
    if loc_param is None:
        return []
    kw_encoded = keyword.strip().replace(" ", "+")
    url = (
        f"https://glints.com/vn/en/opportunities/jobs/explore"
        f"?keyword={kw_encoded}&country=VN&{loc_param}"
    )
    jobs = _glints_playwright_list(url, max_results)
    # Filter out jobs whose card location doesn't match the searched city
    loc_key = effective_location.strip().lower()
    allowed_terms = next(
        (terms for candidate, terms in _GLINTS_LOCATION_KEYWORDS.items() if candidate in loc_key),
        None,
    )
    if allowed_terms:
        jobs = [
            j for j in jobs
            if not j.get("location")
            or any(t in j["location"].lower() for t in allowed_terms)
        ]
    return jobs


def scrape_glints_detail_one(job: dict, cooldown: float) -> None:
    if cooldown > 0:
        _time.sleep(cooldown)
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            ctx = browser.new_context(user_agent=HEADERS["User-Agent"], locale="en-US")
            page = ctx.new_page()
            try:
                page.goto(job["link"], wait_until="domcontentloaded", timeout=20000)
                _time.sleep(2)
                title_text = page.title()
                if "just a moment" in title_text.lower():
                    return
                desc = page.evaluate("""() => {
                    const el = document.querySelector('[class*="DraftjsReadersc__ContentContainer"]');
                    return el ? el.innerHTML.trim() : '';
                }""")
                if desc:
                    job["description"] = _truncate(_clean_html(desc))
                    job["summary_description"] = None
            except Exception as e:
                print(f"[Glints desc] {job['link']}: {e}")
            finally:
                try:
                    ctx.close()
                except Exception:
                    pass
                browser.close()
    except Exception as e:
        print(f"[Glints detail] {e}")


def _glints_loc_param(location: str) -> str | None:
    key = location.strip().lower()
    for candidate, param in _GLINTS_CITY_PARAMS.items():
        if candidate in key:
            return param
    return None


def _glints_playwright_list(url: str, max_results: int) -> list[dict]:
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_ARGS)
            try:
                ctx = browser.new_context(user_agent=HEADERS["User-Agent"], locale="en-US")
                page = ctx.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_selector('[class*="JobCardsc__JobcardContainer"]', timeout=15000)
                except Exception:
                    pass

                title_text = page.title()
                if "just a moment" in title_text.lower() or "security" in title_text.lower():
                    return []

                jobs_data = _extract_glints_cards_js(page)
                ctx.close()
            finally:
                browser.close()

        import time as _t
        now = _t.time()
        jobs = []
        for c in jobs_data:
            title = c.get("title", "").strip()
            link = c.get("link", "").strip()
            if not title or not link:
                continue
            posted_text = c.get("postedText", "")
            days_ago = _glints_days_ago(posted_text)
            if days_ago > RECENT_DAYS:
                continue
            posted_ts_val = now - days_ago * 86400

            raw_skills = [
                s.strip()
                for s in c.get("skills", "").split(",")
                if s.strip()
                and s.strip().lower() not in _NON_SKILL_TAGS
                and not s.strip().startswith("+")
            ]

            jobs.append({
                "title":       title,
                "company":     c.get("company", "N/A"),
                "location":    c.get("location", ""),
                "link":        link,
                "source":      "Glints",
                "posted":      _relative_display(days_ago),
                "posted_ts":   posted_ts_val,
                "logo":        c.get("logo", ""),
                "skills":      raw_skills,
                "description": "",
                "summary_description": "",
            })
            if len(jobs) >= max_results:
                break
        return jobs
    except Exception as e:
        print(f"[Glints Playwright] {e}")
        return []


def _extract_glints_cards_js(page) -> list[dict]:
    return page.evaluate("""() => {
        const cards = Array.from(document.querySelectorAll('[class*="JobCardsc__JobcardContainer"]'));
        return cards.map(card => {
            const titleEl = card.querySelector('[class*="JobCardTitleNoStyleAnchor"]');
            const companyEl = card.querySelector('[class*="CompanyLink-sc"]');
            const postedEl = card.querySelector('[class*="UpdatedAtMessage"], [class*="UpdatedAt"], [class*="PostedAt"], [class*="posted-at"], [class*="time-posted"]');
            const logoEl = card.querySelector('img[src*="aliyuncs"]');
            const locationEls = Array.from(card.querySelectorAll('[class*="JobCardLocationNoStyleAnchor"]'));
            const location = locationEls.map(e => e.textContent.trim()).filter(Boolean).join(', ');
            const skillEls = Array.from(card.querySelectorAll('[class*="TagContentWrapper"]'));
            const skills = skillEls.map(e => e.textContent.trim()).filter(Boolean).join(',');
            let link = titleEl ? titleEl.getAttribute('href') : '';
            if (link) {
                link = 'https://glints.com' + link.split('?')[0];
            }
            return {
                title: titleEl ? titleEl.textContent.trim() : '',
                link: link,
                company: companyEl ? companyEl.textContent.trim() : 'N/A',
                location: location,
                postedText: postedEl ? postedEl.textContent.trim() : '',
                logo: logoEl ? logoEl.getAttribute('src') : '',
                skills: skills,
            };
        });
    }""")


def _glints_days_ago(text: str) -> int:
    text = text.lower().strip()
    if not text:
        return 0  # unknown posted date — treat as fresh
    if "today" in text or "hour" in text or "just now" in text or "minute" in text:
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
