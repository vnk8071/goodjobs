import time as _time

import requests
from bs4 import BeautifulSoup

from ..constants import HEADERS

# ViecOi returns brotli-encoded responses which requests can't decode natively.
# Override Accept-Encoding to request only gzip/deflate.
_VIECOI_HEADERS = {**HEADERS, "Accept-Encoding": "gzip, deflate"}
from ..utils import _relative_display, _clean_html, _truncate

_VIECOI_CITY_SLUGS: dict[str, tuple[str, str]] = {
    "ho chi minh": ("tp-ho-chi-minh", "1"),
    "hcm":         ("tp-ho-chi-minh", "1"),
    "hồ chí minh": ("tp-ho-chi-minh", "1"),
    "hanoi":       ("ha-noi", "62"),
    "ha noi":      ("ha-noi", "62"),
    "hà nội":      ("ha-noi", "62"),
}


def scrape_viecoi(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
    if location.strip().lower() == "remote":
        location = "Ho Chi Minh City"
    params = _viecoi_city_params(location or "Ho Chi Minh City")
    if params is None:
        return []
    city_slug, city_id = params
    keyword_slug = keyword.strip().lower().replace(" ", "-")
    url = f"https://viecoi.vn/tim-viec/key-{keyword_slug}-khu-vuc-{city_slug}-{city_id}.html"
    return _viecoi_requests(url, max_results)


def scrape_viecoi_detail_one(job: dict, cooldown: float) -> None:
    if cooldown > 0:
        _time.sleep(cooldown)
    try:
        resp = requests.get(job["link"], headers=_VIECOI_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        desc_el = soup.select_one("#des_company")
        if desc_el:
            job["description"] = _truncate(_clean_html(str(desc_el)))
            job["summary_description"] = None
    except Exception as e:
        print(f"[ViecOi detail] {job.get('link')}: {e}")


def _viecoi_city_params(location: str) -> tuple[str, str] | None:
    key = location.strip().lower()
    for candidate, params in _VIECOI_CITY_SLUGS.items():
        if candidate in key:
            return params
    return None


def _viecoi_requests(url: str, max_results: int) -> list[dict]:
    try:
        resp = requests.get(url, headers=_VIECOI_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select(".vo-jobs-item.item_job")
        jobs = []
        now = _time.time()
        for card in cards:
            title_el = card.select_one(".job-title-name")
            link_el = card.select_one('a[href*="viecoi.vn/viec-lam/"]')
            company_el = card.select_one(".a-company")
            logo_el = card.select_one("img[data-src]")
            location_el = card.select_one("a.added_detail_information")
            skill_els = card.select("a.cp-tag")

            title = title_el.get_text(strip=True) if title_el else ""
            link = link_el["href"] if link_el and link_el.get("href") else ""
            if not title or not link:
                continue
            if not link.startswith("http"):
                link = "https://viecoi.vn" + link

            company = company_el.get_text(strip=True) if company_el else "N/A"
            logo = logo_el.get("data-src", "") if logo_el else ""
            location_text = location_el.get_text(strip=True) if location_el else ""
            skills = [el.get_text(strip=True) for el in skill_els if el.get_text(strip=True)]
            # ViecOi shows a deadline date, not a posted date — use scrape time as posted_ts
            posted_ts_val = now
            days_ago = 0

            jobs.append({
                "title":       title,
                "company":     company,
                "location":    location_text,
                "link":        link,
                "source":      "ViecOi",
                "posted":      _relative_display(days_ago),
                "posted_ts":   posted_ts_val,
                "logo":        logo,
                "skills":      skills,
                "description": "",
                "summary_description": "",
            })
            if len(jobs) >= max_results:
                break
        return jobs
    except Exception as e:
        print(f"[ViecOi] {e}")
        return []
