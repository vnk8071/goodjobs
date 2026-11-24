# Good Jobs — agent context

Job search aggregator for Vietnam job boards (plus opt-in US/UK/SG). FastAPI backend, vanilla TypeScript frontend, Cloudflare Worker for job pages and the news desk.

## Stack

- **Backend**: FastAPI, Python 3.12, Redis, Playwright (headless Chromium), BeautifulSoup
- **Frontend**: TypeScript (no framework), Vite, SSE streaming. Vietnamese is the default UI language; EN/VI toggle in `frontend/src/i18n.ts`.
- **Worker**: Cloudflare Worker + D1 (`worker/`): `/jobs/<slug>` pages, `/news` desk, LinkedIn auto-posting
- **Infra**: Docker Compose, GitHub Actions, Cloudflare Tunnel

## Commands

```bash
docker compose up                      # frontend http://localhost, backend http://localhost:8000
cd backend && python -m pytest -q      # backend tests
cd backend && python -c "import main"  # quick import/signature check
cd frontend && npm run build           # tsc && vite build
```

`backend/tests/` is gitignored (`tests/` in `.gitignore`): tests run locally but are not committed.

## Key architecture

- Search streams via SSE (`POST /scrape-stream`); `POST /scrape` is the blocking variant used by the MCP server and career-ops.
- Two phases: Phase 1 runs all scrapers, Phase 2 enriches descriptions.
- Redis cache keyed by `(keyword, location)`; jobs pruned after 14 days (`RECENT_DAYS`).
- Warmup (`backend/src/warmup.py`): full scrape at 10:00 and 17:00 ICT, description enrich at 03:00 and 14:00 ICT, over ~32 keyword×location pairs.
- Concurrency: `_executor` (6 workers) for Chromium scrapers, gated to `CHROMIUM_LIMIT` (3) by `chromium_sem`; `_light_executor` (4 workers) for LinkedIn/ViecOi (`LIGHT_LISTING_SITES`/`LIGHT_DETAIL_SITES` in `backend/src/concurrency.py`). Both the live path and warmup must keep light sites off the Chromium pool.
- Wrap every `loop.run_in_executor` call in `asyncio.wait_for`: a hung thread cannot be cancelled, only the awaiting coroutine can.
- Client IP: prefer the `CF-Connecting-IP` header (all traffic arrives via the Cloudflare tunnel).

## Project layout

```
backend/main.py              # FastAPI app, routes, warmup startup
backend/src/
  scrapers/                  # one file per job board
  warmup.py                  # scrape/enrich scheduler
  concurrency.py             # chromium_sem, light-site sets
  analytics.py               # admin analytics, scrape-cycle health
  constants.py               # HEADERS, ADMIN_SECRET, RECENT_DAYS, skill patterns
  matching.py, cache.py, vector.py, intent.py, submissions.py, ratelimit.py
frontend/src/                # main.ts (SSE client), ui.ts, api.ts, i18n.ts
frontend/admin/index.html    # admin dashboard (single file)
worker/src/                  # Cloudflare Worker: job pages, news desk (news/)
mcp/goodjobs_mcp.py          # read-only MCP server over the public API
```

## Scraper contract

- Signature: `scrape_<site>(keyword, location, ...) -> list[dict]`
- Job keys: `title`, `company`, `location`, `posted`, `posted_ts`, `link`, `description`, `source`, `skills`
- Playwright for JS-rendered pages, `requests` + BeautifulSoup for static pages; always set explicit timeouts.
- LinkedIn is the highest-priority source and has its own detail-enrichment pass.

## Common tasks

- **Add a scraper**: `backend/src/scrapers/<site>.py`, export from `scrapers/__init__.py`, register in `main.py` (see `.claude/skills/add-scraper`).
- **Add a skill keyword**: `_SKILL_PATTERNS` in `backend/src/constants.py`.
- **Add a warmup keyword**: edit `_WARMUP_KEYWORDS` in `backend/src/warmup.py` (or the `WARMUP_KEYWORDS` env var).
- **Check cache state**: `GET /cache/status`.
- **Admin endpoints** (`/admin/*`, `/cache/overview`, `/cache/scrape`): guarded by `require_admin` in `backend/main.py`. Send the secret in the `X-Admin-Secret` header, never in the URL. If `ADMIN_SECRET` is unset they return 503.
- **Admin page rendering** (`frontend/admin/index.html`): `escHtml` for text and plain attributes, `escAttr` only for JS strings inside `onclick="…"`, `extLink()` for any external URL (http/https only).

## Deployment

- Backend: `Backend CI/CD` only builds and pushes an amd64 image; its deploy job is commented out (it targeted an old VPS). Deploy manually to the QCS6490 host (arm64, reachable over Tailscale only): `git pull --ff-only`, `docker build -t goodjobs-backend:latest ./backend`, `docker compose -f docker-compose.server.yml up -d --force-recreate backend`.
- Only one machine may run the goodjobs `cloudflared` tunnel: a second connector with the same token silently takes the traffic.
- Frontend: GitHub Pages via `frontend.yml` on push to `main`.
- Worker: `cd worker && CLOUDFLARE_ACCOUNT_ID=2ea08cf1cba04b632d819356c96d24fd npx wrangler deploy` (not automated).

## Rules

- Never commit secrets or `.env` files. Never put secrets in URLs or logs.
- Never commit anything under `.plans/` (gitignored, local planning only).
