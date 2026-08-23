---
name: debug-scrapers
description: Diagnose broken job scrapers — empty results, broken selectors, anti-bot blocks, Playwright failures. Use when a job board returns no jobs or errors.
---

# Debug Scrapers

## First: isolate the failure

```bash
cd backend && python3 -c "
from src.scrapers import scrape_itviec   # swap in the suspect scraper
jobs = scrape_itviec('python', 'Ho Chi Minh City')
print(len(jobs), 'jobs')
import json; print(json.dumps(jobs[0], indent=2, ensure_ascii=False) if jobs else 'EMPTY')
"
```

Interpretation:

| Symptom | Likely cause |
| ------- | ------------ |
| `[]` instantly | URL slug/keyword mismatch, unsupported city → check city-slug lookup tables (e.g. `_ITVIEC_CITY_SLUGS`) |
| `[]` after long delay | Anti-bot block / timeout — scraper swallowed an exception by design |
| Fewer jobs than site shows | `RECENT_DAYS` filter dropping old postings, or pagination capped (`max_results`) |
| Jobs but missing fields | Site markup changed — stale selectors |

Scrapers must NEVER raise — they return `[]`. To see the real exception, temporarily wrap the internals or run the fetch code directly.

## Common failure modes

### 1. Stale selectors (most common)
Fetch the live page and diff against assumptions:
```bash
curl -s -A "Mozilla/5.0 ..." "https://itviec.com/it-jobs/python/ho-chi-minh-hcm" -o /tmp/page.html
```
For Playwright sites, dump `page.content()` after `wait_for_selector` and inspect. Sites change class names frequently; prefer structural selectors (roles, data attributes) over cosmetic classes.

### 2. Anti-bot blocks
- Reuse `HEADERS` from `src/constants.py`; missing/bot-like User-Agent → 403.
- Playwright sites need `CHROMIUM_ARGS` (headless stealth flags) — don't launch bare Chromium.
- Respect per-site cooldowns in `_ENRICH_CFG` (main.py); hammering detail pages triggers IP bans.
- Glassdoor is **best-effort by design** — a block is treated as an empty result. Do not add anti-bot evasion.

### 3. Date parsing drift
Sites localize date strings ("3 giờ trước", "hace 3 días"). Check the relative-time parser in the scraper and normalize through `_relative_display()` / `posted_ts` from `src/utils.py`. Wrong `posted_ts` silently drops jobs via the 8-day prune.

### 4. City/keyword slugs
Each site has its own slug table (e.g. `linkedin.py::_LINKEDIN_LOCATION_MAP`). Unknown cities must return `[]`, not crash. If a valid city returns nothing, the slug table is the first place to look.

## Runtime debugging

```bash
docker compose logs -f backend | grep -i "<site>"   # per-scraper progress lines
tail -f backend/logs/app.log                        # log_app output
tail -f backend/logs/search.log                     # search logger
```

Playwright trace tip: set `headless=False` locally when reproducing JS-rendered sites, and screenshot on failure to see what the bot actually received.

## Fix checklist

- [ ] Isolated repro script returns expected jobs for keyword + supported city
- [ ] Unsupported city still returns `[]` cleanly
- [ ] No exceptions leak out of the scraper
- [ ] Verified through the full stack (`run-and-verify` skill): SSE search shows the board's results
