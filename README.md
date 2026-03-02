# Good Jobs

Search recently posted jobs in Vietnam across multiple job boards (LinkedIn, ITViec, TopCV, VietnamWorks, CareerViet) — all in one place.


## UI

| Column | Description |
|--------|-------------|
| # | Row number |
| Job Title | Clickable — opens detail modal |
| Company | Logo + name |
| Location | As listed on source |
| Posted | Relative time (e.g. "3 hours ago") |
| Skills | Auto-extracted tech skill tags |
| Description | 160-char snippet |
| Source | Colour-coded badge per job board |
| Link | Direct link to original posting |


## Running Locally

```bash
docker compose up
```

- Frontend: http://localhost
- Backend API: http://localhost:8000

> Set `CLOUDFLARE_TUNNEL_TOKEN` in a `.env` file if you want the tunnel to run locally too.

## Production Deployment

Triggered automatically by GitHub Actions on push to `main` (backend or server config changes).

Required GitHub Actions secrets:

| Secret | Purpose |
|--------|---------|
| `DOCKERHUB_USERNAME` | Docker Hub login |
| `DOCKERHUB_TOKEN` | Docker Hub password |
| `SERVER_HOST` | SSH target host |
| `SERVER_USER` | SSH username |
| `SERVER_SSH_KEY` | SSH private key |
| `SERVER_PASSWORD` | SSH password (fallback) |
| `SERVER_PORT` | SSH port (default: 22) |
| `VITE_API_URL` | Backend API URL baked into frontend build |
| `CLOUDFLARE_TUNNEL_TOKEN` | Cloudflare tunnel auth token |


## Architecture

```
User request
     │
     ▼
 Cache hit? ──yes──▶ Stream cached jobs instantly (event: cached)
     │                       │
     no                      │
     │                       ▼
     ▼               Scrapers run in background
 Semaphore               (only new jobs since cache_ts)
  queue
     │
     ▼
Phase 1 ── All scrapers run concurrently
     │      Results streamed as each finishes
     │
     ▼
Phase 2 ── LinkedIn + TopCV description enrichment
           Streamed job-by-job in background
```

### Caching (Redis)

- Results cached by `(keyword, location)` for **1 hour** (configurable via `CACHE_TTL_SECONDS`)
- On cache hit: cached jobs stream instantly; scrapers still run to find jobs newer than `cache_ts`
- On cache miss: full scrape runs, result stored in Redis
- **Background warmup**: on server startup, all suggestion keywords are pre-scraped and cached automatically (including LinkedIn/TopCV descriptions). Cache is refreshed in the background before TTL expires so suggestion chips always respond instantly.
- Redis connection: `REDIS_URL` env var (default: `redis://redis:6379`)


## Project Structure

```
goodjobs/
├── backend/
│   ├── main.py              # FastAPI app, routes, semaphore queue, warmup scheduler
│   └── src/
│       ├── constants.py     # Shared constants (HEADERS, CHROMIUM_ARGS, RECENT_DAYS, REDIS_URL, skill patterns, ...)
│       ├── models.py        # Pydantic models (Job, ScrapeRequest)
│       ├── matching.py      # Title matching, skill extraction, posted_ts
│       ├── cache.py         # Redis cache helpers (get, set, merge)
│       ├── logger.py        # Rotating search logger
│       ├── utils.py         # HTML parsing utilities
│       └── scrapers/
│           ├── linkedin.py
│           ├── itviec.py
│           ├── topcv.py
│           ├── vietnamworks.py
│           ├── careerviet.py
│           ├── topdev.py
│           └── indeed.py
├── frontend/
│   ├── index.html           # UI layout and styles
│   └── src/
│       ├── main.ts          # SSE client, fetch orchestration, queue callbacks
│       ├── api.ts           # SSE stream parser, cached event, LinkedIn fallback
│       ├── ui.ts            # Table rendering, modal, progress pills, queue banner
│       └── types.ts         # Shared TypeScript interfaces
├── docker-compose.yml         # Local development (redis + backend + frontend + cloudflared)
├── docker-compose.server.yml  # Production (redis + backend + cloudflared)
└── .github/workflows/
    ├── backend.yml          # Build Docker image → push → deploy via SSH
    └── frontend.yml         # Build frontend → deploy to GitHub Pages
```
