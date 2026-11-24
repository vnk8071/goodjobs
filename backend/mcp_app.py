"""Remote MCP server (streamable HTTP) for agents; main.py mounts it at /mcp behind an API key."""

import os
from typing import Any

from fastapi import HTTPException
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse

# Hosts allowed by the SDK's DNS-rebinding protection. The SDK default is
# localhost-only, which would reject api.goodjobs.io.vn behind the tunnel.
MCP_ALLOWED_HOSTS = [
    host.strip()
    for host in os.getenv(
        "MCP_ALLOWED_HOSTS", "api.goodjobs.io.vn,localhost:*,127.0.0.1:*,testserver"
    ).split(",")
    if host.strip()
]

# Copied from mcp/goodjobs_mcp.py so remote and stdio agents get the same brief.
INSTRUCTIONS = (
    "Search recently posted jobs across Vietnam (and opt-in US/UK/SG) job boards. "
    "Start with search_jobs; use classify_keyword to turn a CV or vague phrase into a job title."
)


def _error(message: str, status: int | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"error": message}
    if status is not None:
        out["status"] = status
    return out


def _key_from_headers(headers) -> str:
    """Read the API key from headers only — never from a query parameter."""
    raw = (headers.get("x-api-key") or "").strip()
    if raw:
        return raw
    authorization = headers.get("authorization") or ""
    if authorization.startswith("Bearer "):
        return authorization[7:].strip()
    return ""


async def _record_for(ctx: Context, services) -> dict | None:
    """Verify the caller's API key from the MCP request's HTTP headers."""
    try:
        request = ctx.request_context.request
    except (AttributeError, ValueError):
        return None
    if request is None:
        return None
    raw = _key_from_headers(request.headers)
    if not raw:
        return None
    return await services.verify_key(raw)


async def _call(service, *args) -> dict[str, Any]:
    """Call one backend service, turning HTTP errors into tool errors."""
    try:
        return await service(*args)
    except HTTPException as exc:
        return _error(str(exc.detail), exc.status_code)


def build_mcp(services) -> FastMCP:
    """Build the remote MCP server. `services` supplies the verified API record's
    search/recent/semantic/classify/normalize_city operations (backed by the /v1
    routes in main.py), so quotas and behaviour match the HTTP API exactly."""
    mcp = FastMCP(
        "goodjobs",
        instructions=INSTRUCTIONS,
        stateless_http=True,
        json_response=True,
        streamable_http_path="/mcp",
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=MCP_ALLOWED_HOSTS,
            allowed_origins=[],
        ),
    )

    @mcp.tool()
    async def search_jobs(
        keyword: str,
        location: str = "Ho Chi Minh City",
        country: str = "VN",
        limit: int = 20,
        live: bool = False,
        ctx: Context = None,
    ) -> dict[str, Any]:
        """Search job listings for a keyword and location.

        Args:
            keyword: Job title or skill, e.g. "Backend Engineer" or "data analyst".
            location: City name, free text is fine ("hcmc", "hanoi"). Defaults to Ho Chi Minh City.
            country: "VN" (default), "US", "UK", or "SG".
            limit: Maximum number of jobs to return (1-100).
            live: When the keyword is not cached, scrape the boards now. A live scrape
                spends one of the key's daily `live` units.

        Returns the total match count and up to `limit` jobs, newest first.
        """
        record = await _record_for(ctx, services)
        if record is None:
            return _error("A valid API key is required.")
        keyword = (keyword or "").strip()
        location = location or ""
        country = country or "VN"
        if not 1 <= len(keyword) <= 120:
            return _error("keyword must be 1-120 characters")
        if len(location) > 80:
            return _error("location must be at most 80 characters")
        if len(country) > 2:
            return _error("country must be at most 2 characters")
        if not 1 <= limit <= 100:
            return _error("limit must be between 1 and 100")
        return await _call(services.search, record, keyword, location, country, limit, live)

    @mcp.tool()
    async def recent_jobs(n: int = 20, source: str = "", ctx: Context = None) -> dict[str, Any]:
        """List the most recently posted jobs across all cached searches (Vietnam market).

        Args:
            n: How many jobs to return (1-100).
            source: Optional board name to restrict to, e.g. "LinkedIn", "TopCV", "ITViec".
        """
        record = await _record_for(ctx, services)
        if record is None:
            return _error("A valid API key is required.")
        source = source or ""
        if not 1 <= n <= 100:
            return _error("n must be between 1 and 100")
        if len(source) > 40:
            return _error("source must be at most 40 characters")
        return await _call(services.recent, record, n, source)

    @mcp.tool()
    async def semantic_search(query: str, top_k: int = 20, ctx: Context = None) -> dict[str, Any]:
        """Find jobs by meaning rather than keyword, e.g. a pasted CV summary or a skill list.

        Args:
            query: Free text to match against indexed job postings (1-2000 characters).
            top_k: Number of results to return (1-50).
        """
        record = await _record_for(ctx, services)
        if record is None:
            return _error("A valid API key is required.")
        query = (query or "").strip()
        if not 1 <= len(query) <= 2000:
            return _error("query must be 1-2000 characters")
        if not 1 <= top_k <= 50:
            return _error("top_k must be between 1 and 50")
        return await _call(services.semantic, record, query, top_k)

    @mcp.tool()
    async def classify_keyword(text: str, ctx: Context = None) -> dict[str, Any]:
        """Turn free text (a job title, a phrase, or a pasted CV/skills list) into a canonical job keyword.

        Use the returned `keyword` as input to search_jobs.
        """
        record = await _record_for(ctx, services)
        if record is None:
            return _error("A valid API key is required.")
        text = (text or "").strip()
        if not 1 <= len(text) <= 4000:
            return _error("text must be 1-4000 characters")
        return await _call(services.classify, record, text)

    @mcp.tool()
    async def normalize_city(text: str, ctx: Context = None) -> dict[str, Any]:
        """Correct a city name (typos, abbreviations) to the canonical city Good Jobs uses.

        Example: "hcmc" -> "Ho Chi Minh City".
        """
        record = await _record_for(ctx, services)
        if record is None:
            return _error("A valid API key is required.")
        text = (text or "").strip()
        if not 1 <= len(text) <= 120:
            return _error("text must be 1-120 characters")
        return await _call(services.normalize_city, record, text)

    return mcp


class ApiKeyGuard:
    """ASGI wrapper for the MCP endpoint: reject requests without a valid API key.

    Checking in raw ASGI (not as a tool) means unauthenticated traffic never reaches
    the MCP session manager or any tool code.
    """

    def __init__(self, app, verify_key):
        self.app = app
        self.verify_key = verify_key

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        raw = (headers.get("x-api-key") or "").strip()
        if not raw and headers.get("authorization", "").startswith("Bearer "):
            raw = headers["authorization"][7:].strip()
        record = None
        if raw:
            try:
                record = await self.verify_key(raw)
            except Exception:
                record = None
        if record is None:
            response = JSONResponse(
                {"detail": "A valid API key is required. See README: Request an API key."},
                status_code=401,
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
