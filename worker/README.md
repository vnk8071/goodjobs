# Good Jobs — D1 job pages

This Worker stores jobs in **Cloudflare D1** and serves permanent, shareable job
pages at `https://goodjobs.io.vn/jobs/<slug>`.

Search is unchanged: `/scrape-stream` and the homepage feed still read the Redis
cache through the FastAPI backend. D1 is only the durable store for job pages.

## Routes

| Route | Purpose |
| --- | --- |
| `GET /jobs/<slug>` | Server-rendered job page with OG tags + JSON-LD |
| `GET /api/jobs/<slug>` | Job JSON |
| `GET /api/jobs/by-link?link=...` | Existing slug/URL for a job link |
| `GET /api/jobs/ensure?link=...` | Fetch a job from the API, upsert it, return its page URL |
| `POST /api/jobs/sync` | Batch upsert from the sync script (requires `SYNC_SECRET`) |
| `GET /api/jobs/health` | Row count |

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
