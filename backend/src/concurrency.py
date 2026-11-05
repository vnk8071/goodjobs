"""Process-wide Chromium concurrency gate.

Shared between the user-facing /scrape endpoint and the background warmup
scrape/enrich cycles so neither path can launch Chromium processes the other
doesn't know about. Living in its own module (not main.py or warmup.py) lets
both import it without a circular dependency.
"""

import asyncio
import os

# Hard cap on simultaneous headless Chromium across every caller (user search,
# warmup scrape, warmup enrich, background rescrape). The server is 1 CPU / 2GiB:
# more than a handful thrashes the single CPU and pushes RAM toward the OOM ceiling.
CHROMIUM_LIMIT = int(os.getenv("CHROMIUM_LIMIT", "3"))

# Scrapers whose *listing* pass is requests-based (no Chromium) — safe to run
# outside the Chromium gate.
LIGHT_LISTING_SITES = {"viecoi", "linkedin"}
# Scrapers whose *detail* pass is requests-based. LinkedIn's detail pass hits the
# jobs-guest API with plain requests; gating it behind the Chromium semaphore let
# slow Chromium listing scrapes starve it, so no LinkedIn description ever arrived.
LIGHT_DETAIL_SITES = {"viecoi", "linkedin"}

_chromium_sem: asyncio.Semaphore | None = None


def get_chromium_sem() -> asyncio.Semaphore:
    """Return the global Chromium concurrency gate, creating it on first call."""
    global _chromium_sem
    if _chromium_sem is None:
        _chromium_sem = asyncio.Semaphore(CHROMIUM_LIMIT)
    return _chromium_sem
