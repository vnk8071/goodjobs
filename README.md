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

> **One search box → every job board.** Good Jobs is a self-hosted job search aggregator that scrapes recently posted jobs from **18 sources** and streams results to your browser in real time over SSE — with AI-powered intent detection, skill extraction, vector-based related-job supplements, and a permanent Redis cache warmed every 2 hours.

🇻🇳 **Vietnam is the core market** — LinkedIn, ITViec, TopCV, VietnamWorks, CareerViet, TopDev, JobsGo, CareerLink, Glints, ViecOi, Indeed.
🌍 **Opt-in global mode** covers the US, UK, and Singapore — LinkedIn, Indeed, RemoteOK, We Work Remotely, Glassdoor, USAJOBS, Dice. Just type the city; the country is inferred automatically ("nyc" → New York → US sources).

---

## 🔎 The Job Boards

**18 sources, one query.** Every board below is scraped live on each search and merged into a single deduplicated feed.

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
| `/scrape` | POST | Blocking scrape (returns full job list) |
| `/classify-input` | POST | LLM intent detection: free text → canonical keyword |
| `/normalize-city` | POST | Typo correction / abbreviation expansion for cities |
| `/suggest-query` | POST | Query suggestions |
| `/search-semantic` | GET | Vector search across cached jobs |
| `/recent-jobs`, `/stats` | GET | VN-facing views over the cache |
| `/cache/status`, `/cache/overview` | GET | Cache keys with `fetched_ago` and `job_count` |
| `/cache/scrape` | GET | Trigger a scrape for a specific key |
| `/warmup/keywords` | GET / POST | Inspect / extend warmup keyword pairs |
| `/health` | GET | Health probe |

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
- 📝 **Direct submissions**: Headhunters can submit a job via `POST /submissions`; after admin review (`/admin/submissions/*`, gated by `ADMIN_SECRET`) it's merged into search results as source `"Direct"`. Stored in Redis under `submissions:{pending,approved,rejected}`, never written into the keyword/location cache.
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

## 🤖 Agent Skills

This repo ships [agent skills](.claude/skills/) so coding agents (Claude Code, opencode, …) can work on it effectively:

- [`add-scraper`](.claude/skills/add-scraper/SKILL.md) — end-to-end recipe for adding a new job board
- [`debug-scrapers`](.claude/skills/debug-scrapers/SKILL.md) — diagnosing broken selectors, anti-bot blocks, and empty results
- [`cache-ops`](.claude/skills/cache-ops/SKILL.md) — inspecting, flushing, and reasoning about the Redis cache
- [`run-and-verify`](.claude/skills/run-and-verify/SKILL.md) — start the stack and verify changes end-to-end

## 🤝 Contributing

PRs are welcome! The most valuable contribution is a **new job board for your country** — see [CONTRIBUTING.md](CONTRIBUTING.md) and the [`add-scraper`](.claude/skills/add-scraper/SKILL.md) guide.

## 📄 License

[MIT](LICENSE) — each job posting links back to its original source. See also our [privacy](frontend/privacy/) and [terms](frontend/terms/) pages.
