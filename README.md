# 🎯 Good Jobs

<p align="center">
  <a href="#-quick-start"><img alt="Docker" src="https://img.shields.io/badge/Docker-compose-2496ED?logo=docker&logoColor=white"></a>
  <a href="https://fastapi.tiangolo.com"><img alt="FastAPI" src="https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white"></a>
  <a href="https://www.python.org"><img alt="Python" src="https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white"></a>
  <a href="https://www.typescriptlang.org"><img alt="TypeScript" src="https://img.shields.io/badge/TypeScript-vanilla%20TS-3178C6?logo=typescript&logoColor=white"></a>
  <a href="https://redis.io"><img alt="Redis" src="https://img.shields.io/badge/Redis-cache-DC382D?logo=redis&logoColor=white"></a>
  <a href="https://playwright.dev"><img alt="Playwright" src="https://img.shields.io/badge/Playwright-scraping-2EAD33?logo=playwright&logoColor=white"></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/License-MIT-yellow"></a>
  <a href="CONTRIBUTING.md"><img alt="PRs welcome" src="https://img.shields.io/badge/PRs-welcome-brightgreen"></a>
</p>

> **One search box → every job board.** Good Jobs is a self-hosted job search aggregator that scrapes recently posted jobs from **21 sources** and streams results to your browser in real time over SSE — with AI-powered intent detection, skill extraction, vector-based related-job supplements, and a permanent Redis cache warmed every 2 hours.

🇻🇳 **Vietnam is the core market** — LinkedIn, ITViec, TopCV, VietnamWorks, CareerViet, TopDev, JobsGo, CareerLink, Glints, ViecOi, Indeed, TechJobs, Xóm Jobs.
🌍 **Opt-in global mode** covers the US, UK, and Singapore — LinkedIn, Indeed, RemoteOK, We Work Remotely, Glassdoor, USAJOBS, Dice. Just type the city; the country is inferred automatically ("nyc" → New York → US sources).

## 🗓️ Release Timeline

| Date | Feature |
| ---- | ------- |
| 2026-11-17 | **LinkedIn auto-posting** — each published news article is posted automatically to the [Good Jobs company page](https://www.linkedin.com/company/good-jobs-vietnam/) via the LinkedIn REST API (Community Management API, `w_organization_social`) |
| 2026-11-07 | **News desk, homepage filters & All Jobs browser** — [`goodjobs.io.vn/news`](https://goodjobs.io.vn/news) publishes AI-generated Vietnamese articles twice a day (Workers AI + D1, RSS, news sitemap); the homepage feed merges the live Redis cache with the durable Cloudflare D1 archive so every job ever scraped stays browsable, with category/location/source filters and a jobs-per-page selector |
| 2026-10-03 | **MCP server & career-ops submodule** — `mcp/goodjobs_mcp.py` exposes search, recent jobs, semantic search, keyword classification, city normalization, and cache status as read-only tools for MCP clients such as Claude Desktop and Claude Code; career-ops (branch `feat/goodjobs-provider`) vendored at `thirdparty/career-ops` with the `goodjobs` provider tests |
| 2026-08-30 | **career-ops integration** — [career-ops](https://github.com/career-ops-hq/career-ops) added Good Jobs as a `providers/goodjobs.mjs` source; ongoing collaboration on integration depth (`/recent-jobs`, `/search-semantic`) and a self-hosting guide in [career-ops-hq/career-ops#3521](https://github.com/career-ops-hq/career-ops/discussions/3521) |
| 2026-08-22 | **Global search** — opt-in coverage for the US, UK, and Singapore (LinkedIn, Indeed, RemoteOK, We Work Remotely, Glassdoor, USAJOBS, Dice) alongside the Vietnam market |
| 2026-04-11 | **CV / skill matching** — paste a CV or skill list and AI intent detection maps it to a canonical job keyword, with results scored against it |
| 2026-03-25 | **Summary description** — background LLM summarization of long job descriptions, surfaced inline in the UI |

---

## 🔎 The Job Boards

**21 sources, one query.** Every board below is scraped live on each search and merged into a single deduplicated feed.

### 🇻🇳 Vietnam

| Board | Best known for | Scraping |
| ----- | -------------- | -------- |
| [LinkedIn](https://www.linkedin.com/jobs/) | International companies & senior roles | Headless Chromium, full description enrichment |
| [ITViec](https://itviec.com) | IT jobs, transparent salary ranges | Static HTML, full enrichment |
| [TopCV](https://topcv.vn) | Largest VN general job board | Static HTML, full enrichment |
| [VietnamWorks](https://www.vietnamworks.com) | Established enterprises, HR-first | Static HTML, full enrichment |
| [TopDev](https://topdev.vn) | Developer-focused roles | Static HTML, full enrichment |
| [JobsGo](https://jobsgo.vn) | Startups & SMEs | Static HTML, full enrichment |
| [CareerLink](https://careerlink.vn) | Japanese-affiliated companies | Static HTML, full enrichment |
| [CareerViet](https://careerviet.vn) | Broad coverage across industries | Static HTML, full enrichment |
| [Glints](https://glints.com) | Southeast Asia startups | Headless Chromium, full enrichment |
| [ViecOi](https://viecoi.vn) | Foreign-invested firms in VN | Static HTML, full enrichment |
| [Indeed VN](https://vn.indeed.com) | Aggregated listings at scale | Static HTML, inline enrichment |
| [TechJobs](https://techjobs.vn) | IT job aggregator, company career pages | Public website, server-rendered data |
| [Xóm Jobs](https://jobs.xomdata.com) | Data & AI roles | Public website, static HTML |
| [Google Careers](https://www.google.com/about/careers/applications/jobs/results) | Google's own openings in Vietnam | Headless Chromium |

### 🌍 Global (US · UK · SG)

Type any supported city — `new york`, `london`, `singapore` — and the right boards are chosen automatically.

| Board | Best known for | Scraping |
| ----- | -------------- | -------- |
| [LinkedIn](https://www.linkedin.com/jobs/) | All three markets, one scraper | Headless Chromium |
| [Indeed](https://www.indeed.com) | Per-country domains, huge volume | Static HTML |
| [RemoteOK](https://remoteok.com) | Fully remote dev jobs | Public JSON API |
| [We Work Remotely](https://weworkremotely.com) | Remote-first companies | Public RSS feed |
| [Glassdoor](https://www.glassdoor.com) | Salary data + company reviews | Headless Chromium, list-page only |
| [USAJOBS](https://www.usajobs.gov) | US federal government jobs | Official API |
| [Dice](https://www.dice.com) | US tech contracting & full-time | Static HTML |

Every posting keeps its original link — Good Jobs never republishes, it points you home.

## ✨ Highlights

| | |
|---|---|
| ⚡ **Real-time streaming** | Results arrive job-by-job via SSE as each scraper finishes — no waiting for the slowest board |
| 🤖 **AI intent detection** | Paste a CV, a skill list, or a vague phrase — an LLM maps it to a canonical job keyword (with Vietnamese ↔ English translation) |
| 🧠 **Vector supplement** | Related jobs from other cached keywords are retrieved via embeddings and appended on cache misses |
| 🏷️ **Skill extraction** | Tech skills auto-tagged per job from title + description, rendered as pills in the UI |
| 🔁 **Self-warming cache** | ~30 keyword×location pairs re-scraped every 2 hours; new jobs merged by link dedup; jobs pruned after 14 days |
| 🌐 **Free-text locations** | No dropdowns. "hcmc" → Ho Chi Minh City, typos corrected, country inferred from the city |
| 🚦 **Fair concurrency** | A semaphore caps concurrent scrapes; user requests jump ahead of background warmups |
| 🧮 **All Jobs filters** | Homepage feed merges the Redis cache with the D1 archive; filter by category, location, or source, with a jobs-per-page selector |
| 📰 **News desk** | [`/news`](https://goodjobs.io.vn/news) — AI-generated Vietnamese labor-market articles, auto-published twice a day and cross-posted to [LinkedIn](https://www.linkedin.com/company/good-jobs-vietnam/) |

## 🚀 Quick Start

```bash
docker compose up
```

| Service | URL |
| ------- | --- |
| Frontend | <http://localhost> |
| Backend API | <http://localhost:8000> |
| API docs (Swagger) | <http://localhost:8000/docs> |

That's it — type a job title (or paste your CV) and a city, and watch results stream in.

## 📡 API

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/scrape-stream` | POST | Main SSE endpoint — streams `cached`, `data`, `vector-results`, and enrichment events |
| `/recent-jobs`, `/stats` | GET | VN-facing views over the cache |
| `/classify-input` | POST | LLM intent detection: free text → canonical keyword |
| `/normalize-city` | POST | Typo correction / abbreviation expansion for cities |
| `/suggest-query` | POST | Query suggestions |
| `/scrape` | POST | Blocking scrape — **agents only, API key** |
| `/v1/jobs/search` | GET | Agent search — cache-first, `live=true` scrapes on a miss (API key) |
| `/v1/jobs/recent` | GET | Agent view of the latest cached jobs (API key) |
| `/v1/jobs/semantic` | GET | Vector search over cached jobs, `top_k` ≤ 50 (API key) |
| `/v1/classify`, `/v1/normalize-city` | POST | Classifier and city normalization for agents (API key) |
| `/mcp` | GET / POST / DELETE | Remote MCP server, streamable HTTP (API key) |
| `/graphql` | GET / POST | GraphQL endpoint (API key) |
| `/search-semantic` | GET | Vector search across cached jobs — **admin only** |
| `/cache/status` | GET | Cache keys with `fetched_ago` and `job_count` |
| `/cache/overview`, `/cache/scrape` | GET | Cache dashboard and manual scrape — **admin only** |
| `/admin/*` | GET / POST | Admin dashboard data — **admin session or `X-Admin-Secret`** |
| `/warmup/keywords` | GET | Inspect warmup keyword pairs |
| `/health` | GET | Health probe |

## 🔑 API keys

Agents call the API with a key; browser routes (`/scrape-stream`, `/recent-jobs*`, `/jobs-details`,
`/classify-input`, `/normalize-city`, `/suggest-query`, `/chat`) stay open.

**Request a key** — email [khoivn@goodjobs.io.vn](mailto:khoivn@goodjobs.io.vn) or open a
[GitHub issue](https://github.com/vnk8071/goodjobs/issues) with your name, contact and intended use.
You get a token like `gj_…`; it is shown once.

**Send it** as `X-API-Key: gj_…` or `Authorization: Bearer gj_…` (never in the URL). Missing or
revoked keys get `401`, exhausted quotas `429`.

**Daily quotas** (reset at midnight ICT): 500 API calls and 30 live scrapes per key. A search costs
one call; it also costs one live scrape only when `live=true` *and* nothing is cached.

| Endpoint | What it does |
| -------- | ------------ |
| `GET /v1/jobs/search?keyword=&location=&country=&limit=&live=` | Cache-first job search; `source` says `cache`, `live` or `miss` |
| `GET /v1/jobs/recent?n=&source=` | Latest cached jobs across all searches |
| `GET /v1/jobs/semantic?q=&top_k=` | Match by meaning (CV summary, skill list); `top_k` ≤ 50 |
| `POST /v1/classify` `{"text": …}` | Free text → canonical job keyword |
| `POST /v1/normalize-city` `{"text": …}` | City typos/abbreviations → canonical city |

```bash
curl -s "https://api.goodjobs.io.vn/v1/jobs/search?keyword=backend%20engineer&location=hcmc&limit=10" \
  -H "X-API-Key: gj_your_key_here"
```

## 🛠️ Admin dashboard

The admin page (`/admin/`) logs in with a username and password: set `ADMIN_USERNAME` and
`ADMIN_PASSWORD_HASH` (generate the hash with `cd backend && python -m src.admin_auth hash`). Agent
API keys are created, listed and revoked in its **API Keys** tab. `X-Admin-Secret` (`ADMIN_SECRET`)
still works for curl and break-glass access. Both are sent as headers, never in the URL.

## 🏗️ Architecture

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

### Caching & warmup

- Results are cached permanently in Redis by `(keyword, location)`; individual jobs are pruned after **14 days** (`RECENT_DAYS`). Global entries are namespaced (`jobs:{country}:…`) and excluded from VN-facing views and warmup.
- 📝 **Direct submissions**: Headhunters can submit a job via `POST /submissions`; after admin review (`/admin/submissions/*`, gated by the admin login or `ADMIN_SECRET`) it's merged into search results as source `"Direct"`. Stored in Redis under `submissions:{pending,approved,rejected}`, never written into the keyword/location cache.
- **Background warmup**: ~30 VN keyword×location pairs every **2 hours**, checked every 10 minutes. LinkedIn uses `f_TPR=r7200` for incremental 2-hour windows; other scrapers refetch the full window. New jobs merge into existing caches, deduplicated by link.

### Project structure

```text
goodjobs/
├── backend/
│   ├── main.py                    # FastAPI app, routes, semaphore queue, warmup scheduler
│   └── src/
│       ├── constants.py           # HEADERS, CHROMIUM_ARGS, RECENT_DAYS, skill patterns
│       ├── models.py              # Pydantic models (Job, ScrapeRequest)
│       ├── matching.py            # Title matching, skill extraction, level tagging
│       ├── cache.py               # Redis helpers (VN/global namespacing, merge, prune)
│       ├── vector.py              # Embeddings, vector index, supplement search
│       ├── intent.py              # LLM intent detection, city normalization
│       ├── warmup.py              # Warmup scheduler, per-site enrich pipeline
│       ├── summarizer.py          # On-demand description summariser
│       ├── ratelimit.py           # Per-IP rate limiting middleware
│       ├── submissions.py         # Headhunter job-submission store (pending/approved/rejected)
│       ├── graphql_schema.py      # GraphQL schema (jobs query)
│       └── scrapers/              # One module per job board (see tables above)
├── frontend/
│   ├── index.html                 # UI layout and styles
│   ├── privacy/, terms/, contact/, admin/, post-job/
│   └── src/
│       ├── main.ts                # SSE client, fetch orchestration
│       ├── api.ts                 # Stream parser, AI classify/normalize calls
│       ├── ui.ts                  # Table rendering, modal, skill pills, badges
│       └── types.ts               # Shared TypeScript interfaces
├── thirdparty/
│   └── career-ops/                # Git submodule: career-ops (consumes this API via providers/goodjobs.mjs)
├── docker-compose.yml             # Local development
├── docker-compose.server.yml      # Production (+ cloudflared tunnel)
└── .claude/skills/                # Agent skills for common repo tasks
```

## 🛠️ Extending

Common tasks (also available to coding agents as [skills](.claude/skills/)):

- **Add a scraper**: create `backend/src/scrapers/<site>.py`, export from `backend/src/scrapers/__init__.py`, register in `backend/main.py` → see [`add-scraper`](.claude/skills/add-scraper/SKILL.md)
- **Add a skill keyword**: update `_SKILL_PATTERNS` in `backend/src/constants.py`
- **Add a warmup keyword**: `POST /warmup/keywords` or edit `_WARMUP_KEYWORDS` in `backend/src/warmup.py`
- **Check cache state**: `GET /cache/status`

## ☁️ Deployment

Pushing to `main` triggers GitHub Actions:

1. **Backend** — Docker image built → pushed to Docker Hub → deployed over SSH (`backend.yml`)
2. **Frontend** — Built and published to GitHub Pages (`frontend.yml`)

Required repository secrets:

| Secret | Purpose |
| ------ | ------- |
| `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` | Docker Hub login |
| `SERVER_HOST` / `SERVER_USER` / `SERVER_SSH_KEY` / `SERVER_PASSWORD` / `SERVER_PORT` | SSH deploy target |
| `VITE_API_URL` | Backend API URL baked into the frontend build |
| `CLOUDFLARE_TUNNEL_TOKEN` | Cloudflare tunnel auth token |

A separately-operated instance runs outside this CI/CD flow, with its own cron watchdog (restarts on DNS breakage, a crashed container, a wedged event loop, or a stalled scrape scheduler via `GET /warmup/heartbeat`). That script is versioned at [`ops/watchdog.sh`](ops/watchdog.sh) for review; deploying a change to it is a manual copy to that host, not part of this repo's automation.

## 🤖 Agent Skills

This repo ships [agent skills](.claude/skills/) so coding agents (Claude Code, opencode, …) can work on it effectively:

- [`add-scraper`](.claude/skills/add-scraper/SKILL.md) — end-to-end recipe for adding a new job board
- [`debug-scrapers`](.claude/skills/debug-scrapers/SKILL.md) — diagnosing broken selectors, anti-bot blocks, and empty results
- [`cache-ops`](.claude/skills/cache-ops/SKILL.md) — inspecting, flushing, and reasoning about the Redis cache
- [`run-and-verify`](.claude/skills/run-and-verify/SKILL.md) — start the stack and verify changes end-to-end

## 🧩 Third-party: career-ops

[career-ops](https://github.com/vnk8071/career-ops/tree/feat/goodjobs-provider) is vendored as a git submodule at `thirdparty/career-ops` (branch `feat/goodjobs-provider`). It consumes Good Jobs as a source via `providers/goodjobs.mjs`, a thin client that calls the agent `POST /scrape` endpoint and normalizes the results.

Since `/scrape` now requires authentication, career-ops needs an [API key](#-api-keys): set
`GOODJOBS_API_KEY` (or the provider's equivalent setting) to a `gj_…` token, and the provider must
send it as the `X-API-Key` header. Without it every request fails with `401`.

```bash
# Clone with the submodule
git clone --recurse-submodules https://github.com/vnk8071/goodjobs.git

# Or, in an existing clone
git submodule update --init --recursive
```

Test the provider:

```bash
cd thirdparty/career-ops
node --test tests/providers/goodjobs.test.mjs
```

The unit tests cover URL resolution, request building, normalization, and retry behavior. They do not hit the network. The provider's default backend is `https://api.goodjobs.io.vn`. Because career-ops' SSRF guard blocks loopback addresses, a local `docker compose up` instance cannot be used as a source; point `api:` at a publicly reachable HTTPS deployment instead.

To update the submodule to the latest branch commit:

```bash
git submodule update --remote thirdparty/career-ops
```

## 🔌 MCP Server

Good Jobs speaks MCP two ways; both need an [API key](#-api-keys).

**Remote (recommended — nothing to install):**

```bash
claude mcp add --transport http goodjobs https://api.goodjobs.io.vn/mcp --header "X-API-Key: gj_your_key_here"
```

The endpoint is `/mcp` (streamable HTTP, stateless). Each tool call is verified against the key's
daily quotas before it runs.

**Local stdio** (`mcp/goodjobs_mcp.py`) — for clients without HTTP transport support:

```bash
cd mcp
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export GOODJOBS_API_KEY=gj_your_key_here
python -m pytest -q test_goodjobs_mcp.py   # unit tests, no network
```

Register it with Claude Code:

```bash
claude mcp add goodjobs -- /absolute/path/to/goodjobs/mcp/.venv/bin/python /absolute/path/to/goodjobs/mcp/goodjobs_mcp.py
```

| Tool | Calls | Purpose |
| ---- | ----- | ------- |
| `search_jobs` | `GET /v1/jobs/search` | Cache-first search by keyword and location; `live=true` scrapes on a cache miss and spends one daily live unit |
| `recent_jobs` | `GET /v1/jobs/recent` | Latest postings across cached searches, optionally one board |
| `semantic_search` | `GET /v1/jobs/semantic` | Match by meaning, e.g. a pasted CV summary |
| `classify_keyword` | `POST /v1/classify` | Turn free text or a CV into a canonical job title |
| `normalize_city` | `POST /v1/normalize-city` | Fix city typos and abbreviations ("hcmc" → Ho Chi Minh City) |

Admin and submission routes are deliberately not exposed. Set `GOODJOBS_API_URL` to point the stdio
server at another deployment (`http://localhost:8000` for a local `docker compose up`). The first
`search_jobs` call with `live=true` on an uncached keyword can take a minute, because it runs a full
multi-board scrape.

## 🤝 Contributing

PRs are welcome! The most valuable contribution is a **new job board for your country** — see [CONTRIBUTING.md](CONTRIBUTING.md) and the [`add-scraper`](.claude/skills/add-scraper/SKILL.md) guide.

## 📄 License

[MIT](LICENSE) — each job posting links back to its original source. See also our [privacy](frontend/privacy/) and [terms](frontend/terms/) pages.
