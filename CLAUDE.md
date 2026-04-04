# Good Jobs — Claude Context

Job search aggregator for Vietnam job boards. FastAPI backend + vanilla TS frontend.

## Stack

- **Backend**: FastAPI, Python, Redis, Playwright, BeautifulSoup
- **Frontend**: TypeScript (no framework), Vite, SSE streaming
- **Infra**: Docker Compose, GitHub Actions, Cloudflare Tunnel

## Running Locally

```bash
docker compose up
# Frontend: http://localhost
# Backend:  http://localhost:8000
```

## Key Architecture

- Search results stream via **SSE** (`/scrape` endpoint)
- Two phases: Phase 1 = all scrapers concurrently, Phase 2 = LinkedIn/TopCV description enrichment
- Redis cache by `(keyword, location)` — permanent, jobs pruned after 8 days (`RECENT_DAYS`)
- Background warmup runs every 2 hours across ~30 keyword×location pairs
- Semaphore (`MAX_CONCURRENT`) limits concurrent scrapes; user requests take priority over warmup

## Project Layout

```
backend/main.py              # FastAPI app, routes, warmup scheduler
backend/src/
  scrapers/                  # One file per job board
  constants.py               # HEADERS, CHROMIUM_ARGS, RECENT_DAYS, skill patterns
  matching.py                # Title matching, skill extraction
  cache.py                   # Redis helpers
  vector.py                  # Vector search / reranking
frontend/src/
  main.ts                    # SSE client, fetch orchestration
  ui.ts                      # Table rendering, modal, skill pills
  api.ts                     # Stream parser, cached event handling
```

## Scrapers

Each scraper in `backend/src/scrapers/` follows the same contract:
- Function signature: `scrape_<site>(keyword, location, ...) -> list[dict]`
- Returns jobs with keys: `title`, `company`, `location`, `posted`, `posted_ts`, `link`, `description`, `source`, `skills`
- Uses Playwright (headless Chromium) for JS-rendered pages, `requests`+BeautifulSoup for static pages
- LinkedIn scraper is highest priority; has a separate detail-enrichment pass

## Common Tasks

- **Add a new scraper**: create `backend/src/scrapers/<site>.py`, export from `backend/src/scrapers/__init__.py`, register in `main.py`
- **Add a skill keyword**: update `_SKILL_PATTERNS` in `backend/src/constants.py`
- **Add a warmup keyword**: `POST /warmup/keywords` or edit `_WARMUP_KEYWORDS` in `backend/src/warmup.py`
- **Check cache state**: `GET /cache/status`

## Permissions

Claude Code is allowed to run the following git commands without asking for confirmation:
- `git add`
- `git commit`
- `git status`
- `git diff`
- `git log`

## Deployment

- Push to `main` → GitHub Actions builds Docker image → SSH deploys to server
- Frontend deploys to GitHub Pages via separate workflow
- Secrets configured in GitHub Actions (see README for full list)
