---
name: add-scraper
description: End-to-end recipe for adding a new job board (scraper) to Good Jobs — file creation, registration, enrichment, and verification. Use when the user asks to add support for a new job website or job board.
---

# Add a New Job Board Scraper

## 1. Create the scraper module

Create `backend/src/scrapers/<site>.py`. Pick the method based on how the site renders:

| Site type | Method | Reference implementation |
| --------- | ------ | ------------------------ |
| JS-rendered SPA / heavy anti-bot | Playwright headless Chromium | `linkedin.py`, `glints.py` |
| Static HTML | `requests` + BeautifulSoup | `itviec.py`, `topcv.py` |
| Public JSON API | plain `requests` | `remoteok.py`, `usajobs.py` |
| RSS feed | plain `requests` | `weworkremotely.py` |

Required function signature and contract:

```python
def scrape_<site>(keyword: str, location: str = "Ho Chi Minh City", max_results: int = 25) -> list[dict]:
```

Every returned job dict MUST have these keys:

```python
{
    "title": str,        # raw title
    "company": str,
    "location": str,     # as listed on the source site
    "posted": str,       # relative display time, e.g. "3 hours ago" — use _relative_display() from src/utils.py
    "posted_ts": float,  # unix timestamp of posting (for sorting/pruning)
    "link": str,         # absolute URL to the original posting
    "description": str,  # may be "" if enriched in phase 2
    "source": str,       # "<site>" lowercase identifier
    "skills": [str],     # usually [] — filled later by matching.extract_skills
}
```

Rules:
- Filter out jobs older than `RECENT_DAYS` (8) days from `..constants`.
- Return `[]` on any failure — NEVER raise. A blocked/failed board must degrade silently like every other scraper.
- Set a browser-like `User-Agent` from `HEADERS` in constants; reuse `CHROMIUM_ARGS` for Playwright.
- If the site needs phase-2 detail fetching, add `scrape_<site>_detail_one(link) -> str` returning one job description.

## 2. Export it

Add imports + `__all__` entries in `backend/src/scrapers/__init__.py`.

## 3. Register it in backend/main.py

Three places depending on market:

- **Vietnam**: add to `_SCRAPERS` dict (~line 242).
- **Global (US/UK/SG)**: country-independent → `_GLOBAL_SCRAPERS`; per-country domain → bind via lambda in `_global_scraper_registry()` (like indeed/glassdoor); US-only → inside the `if country == "US":` block (like usajobs/dice).
- **Phase-2 enrichment** (VN only): append to `_ENRICH_CFG` (~line 1509): `(site_name, scrape_<site>_detail_one, cooldown_seconds, initial_sleep_seconds)`.

## 4. Verify

```bash
docker compose up -d redis   # or full stack
cd backend && python -c "from src.scrapers import scrape_<site>; jobs = scrape_<site>('python', 'Ho Chi Minh City'); print(len(jobs)); print(jobs[0] if jobs else 'EMPTY')"
```

Checklist:
- [ ] Returns non-empty for a common keyword ("python") in a supported city
- [ ] All 9 contract keys present on each job
- [ ] Unsupported city returns `[]` without raising
- [ ] `/cache/status` shows jobs cached after a live search through `/scrape-stream`

## 5. Docs

Update the board tables in `README.md` (Vietnam or Global section) with the new board, its focus, and method.
