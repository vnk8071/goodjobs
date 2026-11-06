# Good Jobs — D1 job pages

This Worker stores jobs in **Cloudflare D1**, serves permanent job pages at
`https://goodjobs.io.vn/jobs/<slug>`, and runs the Vietnamese **news desk** at
`https://goodjobs.io.vn/news`.

Search is unchanged: `/scrape-stream` and the homepage feed still read the Redis
cache through the FastAPI backend. D1 is the durable store for job pages and
news articles.

## Current deployment

- Cloudflare account: `2ea08cf1cba04b632d819356c96d24fd`.
- Worker: `goodjobs-jobs`, served only on the main domain:
  - `https://goodjobs.io.vn/jobs/*` → server-rendered job pages (HTTP 200).
  - `https://goodjobs.io.vn/api/jobs/*` → JSON API used by the homepage.
  - `https://goodjobs.io.vn/news*` and `/sitemap-news.xml` → news desk pages,
    RSS, sitemap and the generation API.
  - `workers.dev` and preview URLs are disabled.
- D1: `goodjobs-jobs` (`bcf3f823-1866-4131-ba11-3241511b0aba`).
- Public share URL: `https://goodjobs.io.vn/jobs/<slug>`.
- The rest of `goodjobs.io.vn` (homepage, static pages) is still GitHub Pages;
  only the Worker routes above are intercepted.
- Sync: daily at 20:00 ICT (13:00 UTC) via
  `.github/workflows/sync-d1.yml`; only changed rows are written.

## Routes

| Route | Purpose |
| --- | --- |
| `GET /jobs/<slug>` | Server-rendered job page with OG tags + JSON-LD |
| `GET /api/jobs/<slug>` | Job JSON |
| `GET /api/jobs/by-link?link=...` | Existing slug/URL for a job link |
| `GET /api/jobs/ensure?link=...` | Fetch a job from the API, upsert it, return its page URL |
| `POST /api/jobs/sync` | Batch upsert from the sync script (requires `SYNC_SECRET`) |
| `GET /api/jobs/health` | Row count |

### News routes

| Route | Purpose |
| --- | --- |
| `GET /news` | Vietnamese news index (10 per page, category filter) |
| `GET /news/<slug>` | Article page with JSON-LD NewsArticle |
| `GET /news/rss.xml` | RSS feed |
| `GET /sitemap-news.xml` | News sitemap (also declared in `robots.txt`) |
| `GET /api/news/health` | Published/draft counts + last run |
| `GET /api/news/list` | Published articles (`status=draft` requires secret) |
| `POST /api/news/generate` | Generate + publish now (`NEWS_SECRET`; body `{force, perRun, topic}`) |
| `GET /api/news/linkedin-drafts` | LinkedIn copy drafts (`NEWS_SECRET`) |
| `POST /api/news/linkedin-post` | Post an article to LinkedIn (`NEWS_SECRET`) |
| `GET /api/news/debug-sources` / `debug-feed` | Feed diagnostics (`NEWS_SECRET`) |

## News desk

- **Cron:** every day at **20:00 ICT** (`0 13 * * *`). Generates 2 short
  articles (350–500 words, 3–5 sections) so readers can skim.
- **Discovery:** Google News RSS + Bing News RSS + curated feeds from
  VietnamNet, Thanh Niên, Dân trí and Tuổi Trẻ. (VnExpress blocks Workers.)
- **Signals:** D1 `jobs` table — total jobs, new jobs in 7 days, week-over-week
  delta, top skills, job categories, locations and companies.
- **Model:** Workers AI `NEWS_MODEL` (default
  `@cf/meta/llama-4-scout-17b-16e-instruct`) with JSON mode and a Llama 3.1 8B
  fallback.
- **Quality gate:** ≥1,000 body characters, 3–5 sections, ≥2 publishers,
  concrete Good Jobs numbers, LinkedIn draft, no duplicate/near-duplicate title.
  Data paragraphs and the LinkedIn draft are repaired automatically when thin.
- **Publishing:** auto-publish after the gate (`status = 'published'`). Failed
  drafts are stored with `status = 'draft'` and never appear publicly.
- **LinkedIn:** every article stores a ready-to-post `linkedin_text`. To post
  automatically, set `LINKEDIN_ACCESS_TOKEN` and `LINKEDIN_ORG_ID=113563982`
  (Community Management API, scope `w_organization_social`). Without them the
  post stays a draft; copy it from `/api/news/linkedin-drafts`.

Manual run:

```bash
cd worker
NEWS_SECRET=... node scripts/news.mjs
NEWS_SECRET=... node scripts/news.mjs --topic ai-va-nghe-nghiep --per-run 1
```

Required secrets: `NEWS_SECRET` (generation/admin API). Optional LinkedIn:
`LINKEDIN_ACCESS_TOKEN`, `LINKEDIN_ORG_ID`, `LINKEDIN_VERSION`.


## First-time setup

```bash
cd worker
npm install
npx wrangler login                 # or set CLOUDFLARE_API_TOKEN
npx wrangler d1 create goodjobs-jobs
# copy database_id into wrangler.toml
npx wrangler d1 execute goodjobs-jobs --remote --file=./schema.sql
npx wrangler secret put SYNC_SECRET
npx wrangler deploy
```

Required API token permissions:

- Account → D1 → Edit
- Account → Workers Scripts → Edit
- Zone → Workers Routes → Edit (zone `goodjobs.io.vn`)

## Local test

```bash
cd worker
npx wrangler d1 execute goodjobs-jobs --local --file=./schema.sql
npx wrangler dev
# open http://localhost:8787/api/jobs/health
```

The Worker also supports a `?link=` fallback on `/jobs/<slug>`: if the slug is
not in D1 yet, it calls the backend and redirects to the canonical page.

## Sync

The GitHub Actions workflow `.github/workflows/sync-d1.yml` runs **once a day**
at 20:00 ICT (13:00 UTC), after the 17:00 ICT scrape cycle:

- `GET https://api.goodjobs.io.vn/export-jobs` (Redis cache, full descriptions)
- Computes stable `id` / `slug` / `content_hash`
- Sends batches to `POST /api/jobs/sync` with `SYNC_SECRET`

Only changed rows are written, so the D1 free tier (100k writes/day) is used
efficiently.

Required GitHub secrets:

- `JOBS_WORKER_URL` — e.g. `https://goodjobs.io.vn`
- `SYNC_SECRET` — same value as the Worker secret

Manual run:

```bash
cd worker
JOBS_WORKER_URL=https://goodjobs.io.vn SYNC_SECRET=... node scripts/sync.mjs
```

## Frontend share button

`frontend/src/api.ts` calls `/api/jobs/ensure?link=...`; if the Worker is not
available it falls back to the old `/?job=...` link. The Worker page is the
preferred share URL once D1 is deployed.
