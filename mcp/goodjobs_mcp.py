"""Good Jobs MCP server.

Exposes the Good Jobs aggregator as read-only MCP tools over stdio, so any
MCP client (Claude Desktop, Claude Code, ...) can search jobs across the 18
supported boards.

Config (env):
    GOODJOBS_API_URL   Backend origin. Defaults to the public instance
                       https://api.goodjobs.io.vn. Use http://localhost:8000
                       to talk to a local `docker compose up`.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

DEFAULT_API_URL = "https://api.goodjobs.io.vn"
# Cold multi-source scrapes can take 30-60s+; cached hits return in under a second.
SCRAPE_TIMEOUT_S = 120.0
DEFAULT_TIMEOUT_S = 30.0

# Fields kept from a /scrape job. The full `description` is dropped to keep
# tool output small; use `link` to read the original posting.
_JOB_FIELDS = ("title", "company", "location", "source", "posted_date", "link", "skills")

mcp = FastMCP(
    "goodjobs",
    instructions=(
        "Search recently posted jobs across Vietnam (and opt-in US/UK/SG) job boards. "
        "Start with search_jobs; use classify_keyword to turn a CV or vague phrase into a job title."
    ),
)


def _api_url() -> str:
    return os.environ.get("GOODJOBS_API_URL", DEFAULT_API_URL).rstrip("/")


def _request(method: str, path: str, *, timeout: float = DEFAULT_TIMEOUT_S, **kwargs: Any) -> Any:
    """Call the Good Jobs backend. Returns parsed JSON or raises RuntimeError with a readable message."""
    url = f"{_api_url()}{path}"
    try:
        with httpx.Client(timeout=timeout, follow_redirects=False) as client:
            resp = client.request(method, url, **kwargs)
    except httpx.TimeoutException as exc:
        raise RuntimeError(f"Good Jobs API timed out after {timeout:.0f}s ({url})") from exc
    except httpx.HTTPError as exc:
        raise RuntimeError(f"Could not reach Good Jobs API at {url}: {exc}") from exc
    if resp.status_code >= 400:
        detail = resp.text[:300]
        raise RuntimeError(f"Good Jobs API returned HTTP {resp.status_code} for {path}: {detail}")
    return resp.json()


def _trim_job(job: dict[str, Any]) -> dict[str, Any]:
    return {k: job.get(k) for k in _JOB_FIELDS}


@mcp.tool()
def search_jobs(
    keyword: str,
    location: str = "Ho Chi Minh City",
    country: str = "VN",
    limit: int = 20,
) -> dict[str, Any]:
    """Search live job listings for a keyword and location.

    Args:
        keyword: Job title or skill, e.g. "Backend Engineer" or "data analyst".
        location: City name, free text is fine ("hcmc", "hanoi"). Defaults to Ho Chi Minh City.
        country: "VN" (default), "US", "UK", or "SG".
        limit: Maximum number of jobs to return (1-100).

    Returns the total match count and up to `limit` jobs, newest first.
    """
    keyword = keyword.strip()
    if not keyword:
        return {"error": "keyword is required"}
    limit = max(1, min(limit, 100))
    body = {"keyword": keyword, "location": location, "country": country}
    try:
        jobs = _request("POST", "/scrape", json=body, timeout=SCRAPE_TIMEOUT_S)
    except RuntimeError as exc:
        return {"error": str(exc)}
    if not isinstance(jobs, list):
        return {"error": f"unexpected /scrape response: {type(jobs).__name__}"}
    jobs.sort(key=lambda j: j.get("posted_ts") or 0, reverse=True)
    return {
        "keyword": keyword,
        "location": location,
        "country": country,
        "total": len(jobs),
        "jobs": [_trim_job(j) for j in jobs[:limit]],
    }


@mcp.tool()
def recent_jobs(n: int = 20, source: str = "") -> dict[str, Any]:
    """List the most recently posted jobs across all cached searches (Vietnam market).

    Args:
        n: How many jobs to return.
        source: Optional board name to restrict to, e.g. "LinkedIn", "TopCV", "ITViec".
    """
    params: dict[str, Any] = {"n": max(1, min(n, 100))}
    if source.strip():
        params["source"] = source.strip()
    try:
        data = _request("GET", "/recent-jobs", params=params)
    except RuntimeError as exc:
        return {"error": str(exc)}
    jobs = data if isinstance(data, list) else []
    return {"count": len(jobs), "jobs": [_trim_job(j) for j in jobs]}


@mcp.tool()
def semantic_search(query: str, top_k: int = 20) -> dict[str, Any]:
    """Find jobs by meaning rather than keyword, e.g. a pasted CV summary or a skill list.

    Args:
        query: Free text to match against indexed job postings.
        top_k: Number of results to return.
    """
    query = query.strip()
    if not query:
        return {"error": "query is required"}
    try:
        data = _request("GET", "/search-semantic", params={"q": query, "top_k": max(1, min(top_k, 100))})
    except RuntimeError as exc:
        return {"error": str(exc)}
    results = data.get("results", []) if isinstance(data, dict) else []
    return {"query": query, "count": len(results), "results": results}


@mcp.tool()
def classify_keyword(text: str) -> dict[str, Any]:
    """Turn free text (a job title, a phrase, or a pasted CV/skills list) into a canonical job keyword.

    Use the returned `keyword` as input to search_jobs.
    """
    text = text.strip()
    if not text:
        return {"error": "text is required"}
    try:
        return _request("POST", "/classify-input", json={"keyword": text})
    except RuntimeError as exc:
        return {"error": str(exc)}


@mcp.tool()
def normalize_city(text: str) -> dict[str, Any]:
    """Correct a city name (typos, abbreviations) to the canonical city Good Jobs uses.

    Example: "hcmc" -> "Ho Chi Minh City".
    """
    text = text.strip()
    if not text:
        return {"error": "text is required"}
    try:
        # The endpoint validates against ScrapeRequest, so `keyword` is required
        # even though only `location` is used.
        return _request("POST", "/normalize-city", json={"keyword": text, "location": text})
    except RuntimeError as exc:
        return {"error": str(exc)}


@mcp.tool()
def cache_status() -> dict[str, Any]:
    """Report which keyword/location searches are cached, how fresh they are, and how many jobs each holds."""
    try:
        return _request("GET", "/cache/status")
    except RuntimeError as exc:
        return {"error": str(exc)}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
