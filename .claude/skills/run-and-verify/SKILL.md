---
name: run-and-verify
description: Start the Good Jobs stack locally and verify changes end-to-end — health checks, live SSE searches, frontend build, lint/typecheck. Use before claiming any change works.
---

# Run & Verify

## Start the stack

```bash
docker compose up -d          # redis + backend + frontend
```

| Service | URL |
| ------- | --- |
| Frontend | http://localhost |
| Backend API | http://localhost:8000 |
| Swagger docs | http://localhost:8000/docs |

If only Python deps changed: `docker compose up -d --build backend`.

## Backend verification

```bash
curl -s localhost:8000/health                       # liveness
curl -s localhost:8000/cache/status | python3 -m json.tool   # cache state

# Live streaming search (the main path) — watch events arrive
curl -N -X POST localhost:8000/scrape-stream \
  -H 'Content-Type: application/json' \
  -d '{"keyword":"python","location":"Ho Chi Minh City"}'
```

Expected event order: `cached` (if hit) → `data` per scraper as each finishes → enrichment updates in background. A global search: use `"location":"New York"` — should hit US sources (`remoteok`, `weworkremotely`, `usajobs`, `dice`, ...).

## Frontend verification

```bash
cd frontend && npm install && npm run build   # tsc + vite build must pass
```

No test framework is configured — verification is build + manual SSE smoke test above.

## Logs

```bash
docker compose logs -f backend    # app logs include per-scraper progress lines
```

## Definition of done

- [ ] `npm run build` passes for frontend changes
- [ ] Backend imports cleanly: `python3 -c "import main"` from `backend/`
- [ ] One VN search + one global search stream results over SSE
- [ ] New/changed scrapers verified via the `debug-scrapers` / `add-scraper` skills
