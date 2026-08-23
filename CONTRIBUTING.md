# Contributing to Good Jobs

Thanks for your interest in contributing! 🎉 Good Jobs is an open-source job search aggregator — every contribution, from a new job board to a one-line fix, is welcome.

## Ways to Contribute

- **Add a job board** for your country (see below) — the most valuable contribution
- **Fix a broken scraper** — sites change their markup often; stale selectors are the #1 issue
- **Improve matching** — skill patterns live in `backend/src/constants.py` (`_SKILL_PATTERNS`)
- **Frontend polish** — vanilla TypeScript, no framework (`frontend/src/`)
- **Report bugs** — open an issue with the board name, keyword, and city you searched

## Getting Started

```bash
git clone https://github.com/vnk8071/goodjobs.git
cd goodjobs
docker compose up
```

- Frontend: <http://localhost>
- Backend API: <http://localhost:8000> (Swagger docs at `/docs`)

Verify your setup: search "python" in "Ho Chi Minh City" and confirm results stream in from multiple boards.

## Adding a New Job Board

This is the most common contribution. Quick version:

1. Create `backend/src/scrapers/<site>.py` exporting `scrape_<site>(keyword, location, max_results) -> list[dict]`
2. Every job must include: `title`, `company`, `location`, `posted`, `posted_ts`, `link`, `description`, `source`, `skills`
3. Export it from `backend/src/scrapers/__init__.py`, register in `_SCRAPERS` (VN) or `_GLOBAL_SCRAPERS` / `_global_scraper_registry()` (global) in `backend/main.py`
4. Never raise — return `[]` on failure so a broken board degrades silently

Full step-by-step guide: [`.claude/skills/add-scraper/SKILL.md`](.claude/skills/add-scraper/SKILL.md)

Pick a scraping method based on the site:

| Site type | Method | Reference |
| --------- | ------ | --------- |
| JS-rendered / anti-bot | Playwright | `linkedin.py`, `glints.py` |
| Static HTML | requests + BeautifulSoup | `itviec.py`, `topcv.py` |
| Public API / RSS | plain requests | `remoteok.py`, `usajobs.py` |

**Scraping etiquette:** identify with a browser-like User-Agent, respect per-site cooldowns, don't hammer detail pages. A board that rate-limits you ruins it for everyone.

## Code Guidelines

- **Python**: follow existing scraper style; no comments explaining obvious code; keep scrapers self-contained
- **TypeScript**: no framework — match the existing vanilla TS + Vite patterns in `frontend/src/`
- Frontend changes must pass: `cd frontend && npm run build`

## Submitting a Pull Request

1. Fork the repo and create a branch from `main`: `feat/<short-name>` or `fix/<short-name>`
2. Make your changes
3. Test end-to-end: run the stack, do one VN search and one global search through `/scrape-stream`
4. Update `README.md` if you added a board (add it to the right table)
5. Open a PR describing what you changed and how you tested it

Keep PRs focused — one board per PR is ideal.

## Reporting a Broken Scraper

Open an issue with:

- Board name
- Keyword + city searched
- What happened vs. what you expected (empty results? missing fields? crash?)
- Output of `curl -s localhost:8000/cache/status` if relevant

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
