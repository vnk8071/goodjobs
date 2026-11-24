"""Good Jobs MCP server.

Exposes the Good Jobs aggregator as read-only MCP tools over stdio, so any
MCP client (Claude Desktop, Claude Code, ...) can search jobs across the
supported boards. Every call goes through the authenticated /v1 agent API.

Config (env):
    GOODJOBS_API_URL   Backend origin. Defaults to the public instance
                       https://api.goodjobs.io.vn. Use http://localhost:8000
                       to talk to a local `docker compose up`.
    GOODJOBS_API_KEY   Agent API key ("gj_..."). Required — the /v1 endpoints
                       authenticate and quota every caller. Request one:
                       https://github.com/vnk8071/goodjobs#-api-keys
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

DEFAULT_API_URL = "https://api.goodjobs.io.vn"
# A live search can run a full multi-board scrape; cached hits return in under a second.
SCRAPE_TIMEOUT_S = 120.0
DEFAULT_TIMEOUT_S = 30.0
MISSING_KEY_ERROR = (
    "Set GOODJOBS_API_KEY. Request a key: see https://github.com/vnk8071/goodjobs#-api-keys"
)

mcp = FastMCP(
    "goodjobs",
    instructions=(
        "Search recently posted jobs across Vietnam (and opt-in US/UK/SG) job boards. "
        "Start with search_jobs; use classify_keyword to turn a CV or vague phrase into a job title."
    ),
)


def _api_url() -> str:
    return os.environ.get("GOODJOBS_API_URL", DEFAULT_API_URL).rstrip("/")


def _api_key() -> str:
    return os.environ.get("GOODJOBS_API_KEY", "").strip()


def _request(method: str, path: str, *, timeout: float = DEFAULT_TIMEOUT_S, **kwargs: Any) -> Any:
    """Call the Good Jobs /v1 agent API. Returns parsed JSON or raises RuntimeError."""
    key = _api_key()
    if not key:
        # Refuse locally: no point spending a round-trip on a guaranteed 401.
        raise RuntimeError(MISSING_KEY_ERROR)
    url = f"{_api_url()}{path}"
    headers = {"X-API-Key": key}
    try:
        with httpx.Client(timeout=timeout, follow_redirects=False) as client:
            resp = client.request(method, url, headers=headers, **kwargs)
    except httpx.TimeoutException as exc:
        raise RuntimeError(f"Good Jobs API timed out after {timeout:.0f}s ({url})") from exc
    except httpx.HTTPError as exc:
        raise RuntimeError(f"Could not reach Good Jobs API at {url}: {exc}") from exc
    if resp.status_code == 401:
        raise RuntimeError(f"Good Jobs API rejected the API key (HTTP 401). {MISSING_KEY_ERROR}")
    if resp.status_code == 429:
        try:
            detail = resp.json().get("detail", "")
        except Exception:
            detail = resp.text[:300]
        raise RuntimeError(f"Good Jobs API quota reached for this key (HTTP 429): {detail}")
    if resp.status_code >= 400:
        detail = resp.text[:300]
        raise RuntimeError(f"Good Jobs API returned HTTP {resp.status_code} for {path}: {detail}")
    return resp.json()


@mcp.tool()
def search_jobs(
    keyword: str,
    location: str = "Ho Chi Minh City",
    country: str = "VN",
    limit: int = 20,
    live: bool = False,
) -> dict[str, Any]:
    """Search job listings for a keyword and location.

    Cache-first: a cached query answers in under a second. With `live=True` the
    server scrapes the boards when nothing is cached, which spends one of the
    key's daily live scrapes.

    Args:
        keyword: Job title or skill, e.g. "Backend Engineer" or "data analyst".
        location: City name, free text is fine ("hcmc", "hanoi"). Defaults to Ho Chi Minh City.
        country: "VN" (default), "US", "UK", or "SG".
        limit: Maximum number of jobs to return (1-100).
        live: Scrape on a cache miss (spends one daily live unit).

    Returns the total match count and up to `limit` jobs, newest first.
    """
    keyword = keyword.strip()
    if not keyword:
        return {"error": "keyword is required"}
    params: dict[str, Any] = {
        "keyword": keyword,
        "location": location,
        "country": country,
        "limit": max(1, min(limit, 100)),
        "live": "true" if live else "false",
    }
    try:
        return _request(
            "GET",
            "/v1/jobs/search",
            params=params,
            timeout=SCRAPE_TIMEOUT_S if live else DEFAULT_TIMEOUT_S,
        )
    except RuntimeError as exc:
        return {"error": str(exc)}


@mcp.tool()
def recent_jobs(n: int = 20, source: str = "") -> dict[str, Any]:
    """List the most recently posted jobs across all cached searches (Vietnam market).

    Args:
        n: How many jobs to return (1-100).
        source: Optional board name to restrict to, e.g. "LinkedIn", "TopCV", "ITViec".
    """
    params: dict[str, Any] = {"n": max(1, min(n, 100))}
    if source.strip():
        params["source"] = source.strip()
    try:
        return _request("GET", "/v1/jobs/recent", params=params)
    except RuntimeError as exc:
        return {"error": str(exc)}


@mcp.tool()
def semantic_search(query: str, top_k: int = 20) -> dict[str, Any]:
    """Find jobs by meaning rather than keyword, e.g. a pasted CV summary or a skill list.

    Args:
        query: Free text to match against indexed job postings.
        top_k: Number of results to return (1-50).
    """
    query = query.strip()
    if not query:
        return {"error": "query is required"}
    try:
        return _request(
            "GET",
            "/v1/jobs/semantic",
            params={"q": query, "top_k": max(1, min(top_k, 50))},
        )
    except RuntimeError as exc:
        return {"error": str(exc)}


@mcp.tool()
def classify_keyword(text: str) -> dict[str, Any]:
    """Turn free text (a job title, a phrase, or a pasted CV/skills list) into a canonical job keyword.

    Use the returned `keyword` as input to search_jobs.
    """
    text = text.strip()
    if not text:
        return {"error": "text is required"}
    try:
        return _request("POST", "/v1/classify", json={"text": text})
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
        return _request("POST", "/v1/normalize-city", json={"text": text})
    except RuntimeError as exc:
        return {"error": str(exc)}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
