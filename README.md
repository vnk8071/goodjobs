# Good Jobs

Search recently posted jobs across multiple job boards, all in one place. Vietnam is the core market (LinkedIn, ITViec, TopCV, VietnamWorks, CareerViet, TopDev, JobsGo, CareerLink, Glints, ViecOi, Indeed), with an opt-in global mode covering the US, UK, and Singapore (LinkedIn, Indeed, RemoteOK, We Work Remotely, Glassdoor). The interface is English-only.

## Search

Type a job title (or paste a CV/skill list) and a city — the location field is free text, AI-normalized (typo correction, abbreviation expansion, e.g. "hcmc" → "Ho Chi Minh City") and used to infer whether to search Vietnam or the global sources. No dropdown, no manual country selector.

## UI

| Column | Description |
| ------ | ----------- |
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

- Frontend: <http://localhost>
- Backend API: <http://localhost:8000>

## Production Deployment

Triggered automatically by GitHub Actions on push to `main` (backend or server config changes).

Required GitHub Actions secrets:

| Secret | Purpose |
| ------ | ------- |
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

```text
User request
     │
     ▼
 Cache hit? ──yes──▶ Stream cached jobs instantly (event: cached)
     │                       │
     no                      ▼
     │               Vector supplement from related
     ▼               cached keywords (event: vector-results)
 Semaphore
  queue
     │
     ▼
Phase 1 ── All scrapers for the resolved country run concurrently
     │      Results streamed as each finishes (event: data)
     │
     ▼
Phase 2 ── Per-site description enrichment — VN sources (LinkedIn, TopCV,
           ITViec, TopDev, JobsGo, CareerLink, Glints, ViecOi,
           VietnamWorks, CareerViet) or global sources (LinkedIn only;
           RemoteOK/We Work Remotely return full descriptions in Phase 1,
           Glassdoor is list-page-only, Indeed enriches inline in Phase 1)
           — streamed job-by-job in background
```

### Global Search (US / UK / SG)

- Country is inferred from the typed city (`_resolve_country` in `main.py`, a small lookup table for the initially-supported cities) — never a separate dropdown. Unrecognized input defaults to Vietnam, so existing VN behavior is unchanged.
- Global scrapers: LinkedIn (reused as-is — it already supports arbitrary locations), Indeed (same scraper, per-country domain), RemoteOK and We Work Remotely (public API/RSS, no browser automation), Glassdoor (best-effort, list-page only — no anti-bot evasion is used; a block is treated as an empty result, same as any other scraper failure).
- Cached on search, but **never included in the background warmup cycle** — global entries only get populated by a live user search. Cache keys are namespaced by country (`jobs:{country}:{kw}:{loc}` vs. the VN-default `jobs:{kw}:{loc}`) so results never collide.

### Scraper Types

| Type | Sites | Method |
| ---- | ----- | ------ |
| Playwright (JS-rendered) | LinkedIn, Glints, Glassdoor | Headless Chromium via `playwright` |
| requests + BeautifulSoup | ITViec, TopCV, TopDev, JobsGo, CareerLink, ViecOi, VietnamWorks, CareerViet, Indeed | Static HTML scraping |
| requests (API/RSS, no HTML parsing) | RemoteOK, We Work Remotely | Public JSON API / RSS feed |

### Caching (Redis)

- Results cached permanently by `(keyword, location)` — no TTL for VN entries, jobs retained for **8 days** (`RECENT_DAYS`). Global (non-VN) entries carry the same TTL but are excluded from the daily non-warmup cleanup job and the VN-facing `/recent-jobs`/`/stats` views — see `is_vn_cache_key()` in `cache.py`.
- On cache hit: cached jobs stream instantly; vector supplement from related keywords appended (VN only)
- On cache miss: full scrape runs, result stored in Redis
- **Background warmup**: scrapes ~30 keyword×location pairs (Vietnam only) every **2 hours** (`SCRAPE_INTERVAL=7200`). Check runs every 10 minutes; a scrape is triggered only when `now - fetched_ts >= 7200s`. LinkedIn uses `f_TPR=r7200` to fetch only jobs from the last 2 hours incrementally; other scrapers always fetch the full `RECENT_DAYS` window.
- New jobs are **merged** into existing cache (deduplicated by link). Jobs older than 8 days are pruned by `_cleanup_old_jobs()` (VN) / TTL expiry (global).
- Cache status: `GET /cache/status` — shows all keys with `fetched_ago` and `job_count`
- Redis connection: `REDIS_URL` env var (default: `redis://redis:6379`)

### Intent Detection

Free-form queries (CV text, skill lists, non-job phrases) are classified by an LLM intent layer (`src/intent.py`) before scraping. Detected intent maps to a canonical job keyword, with Vietnamese↔English translation and domain-specific matching. The same module also normalizes free-text city input (`normalize_city()`, backing `POST /normalize-city`) — typo correction and abbreviation expansion (e.g. "nyc" → "New York"), with a graceful fallback to the raw input if the AI call is unavailable.

### Vector Search

Jobs are embedded and stored in a vector index (`src/vector.py`). On non-cache searches, related jobs from other cached keywords are retrieved and appended as a supplement, filtered to only those with descriptions. Vietnam-only.

## Project Structure

```text
goodjobs/
├── backend/
│   ├── main.py                    # FastAPI app, routes, semaphore queue, warmup scheduler
│   └── src/
│       ├── constants.py           # HEADERS, CHROMIUM_ARGS, RECENT_DAYS, skill patterns
│       ├── models.py              # Pydantic models (Job, ScrapeRequest)
│       ├── matching.py            # Title matching, skill extraction, level tagging
│       ├── cache.py               # Redis cache helpers (get, set, merge, touch, VN/global namespacing)
│       ├── vector.py              # Vector embedding, index, supplement search
│       ├── intent.py              # LLM intent detection, keyword mapping, city normalization
│       ├── warmup.py              # Background warmup scheduler, per-site enrich pipeline (VN only)
│       ├── summarizer.py          # On-demand job description summariser
│       ├── background_summarizer.py  # Async summarisation queue
│       ├── ratelimit.py           # Per-IP rate limiting middleware
│       ├── logger.py              # Rotating search logger
│       ├── utils.py               # HTML cleaning, truncation helpers
│       ├── graphql_schema.py      # GraphQL schema (jobs query)
│       └── scrapers/
│           ├── linkedin.py        # Playwright — JS-rendered SPA (VN + global)
│           ├── glints.py          # Playwright — JS-rendered SPA
│           ├── glassdoor.py       # Playwright — global only, best-effort, list-page only
│           ├── itviec.py          # requests + BeautifulSoup
│           ├── topcv.py           # requests + BeautifulSoup
│           ├── topdev.py          # requests + BeautifulSoup
│           ├── jobsgo.py          # requests + BeautifulSoup
│           ├── careerlink.py      # requests + BeautifulSoup
│           ├── viecoi.py          # requests + BeautifulSoup
│           ├── vietnamworks.py    # requests + BeautifulSoup
│           ├── careerviet.py      # requests + BeautifulSoup
│           ├── indeed.py          # requests + BeautifulSoup (VN + global, per-country domain)
│           ├── remoteok.py        # requests — public JSON API (global only)
│           └── weworkremotely.py  # requests — public RSS feed (global only)
├── frontend/
│   ├── index.html                 # UI layout and styles
│   ├── privacy/                   # Privacy policy
│   ├── terms/                     # Terms of service
│   ├── contact/                   # Contact page
│   ├── admin/                     # Internal admin dashboard
│   └── src/
│       ├── main.ts                # SSE client, fetch orchestration, queue callbacks
│       ├── api.ts                 # SSE stream parser, cached/vector/enriching events, AI classify/normalize calls
│       ├── ui.ts                  # Table rendering, modal, skill pills, site badges
│       └── types.ts               # Shared TypeScript interfaces
├── docker-compose.yml             # Local development (redis + backend + frontend)
├── docker-compose.server.yml      # Production (redis + backend + cloudflared)
└── .github/workflows/
    ├── backend.yml                # Build Docker image → push → deploy via SSH
    └── frontend.yml               # Build frontend → deploy to GitHub Pages
```
