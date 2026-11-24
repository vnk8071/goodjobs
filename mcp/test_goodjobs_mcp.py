import json

import httpx
import pytest

import goodjobs_mcp as gm

API_KEY = "gj_test_key"


def _install(monkeypatch, handler):
    """Route every httpx.Client the server creates through a mock transport."""
    real_client = httpx.Client
    seen_timeouts = []

    def factory(**kwargs):
        seen_timeouts.append(kwargs.get("timeout"))
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(gm.httpx, "Client", factory)
    return seen_timeouts


def _json(body, status=200):
    return httpx.Response(status, json=body)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("GOODJOBS_API_URL", "https://api.example.test/")
    monkeypatch.setenv("GOODJOBS_API_KEY", API_KEY)


def test_api_url_strips_trailing_slash():
    assert gm._api_url() == "https://api.example.test"


def test_search_jobs_gets_v1_and_sends_key(monkeypatch):
    seen = {}

    def handler(request):
        seen["method"] = request.method
        seen["url"] = str(request.url.copy_with(query=None))
        seen["params"] = dict(request.url.params)
        seen["key"] = request.headers.get("X-API-Key")
        return _json({
            "keyword": "Backend Engineer",
            "location": "Ho Chi Minh City",
            "country": "VN",
            "source": "cache",
            "total": 1,
            "jobs": [{"title": "New", "link": "https://x/2"}],
        })

    _install(monkeypatch, handler)
    out = gm.search_jobs("Backend Engineer", limit=1000)

    assert seen["method"] == "GET"
    assert seen["url"] == "https://api.example.test/v1/jobs/search"
    assert seen["key"] == API_KEY
    assert seen["params"] == {
        "keyword": "Backend Engineer",
        "location": "Ho Chi Minh City",
        "country": "VN",
        "limit": "100",
        "live": "false",
    }
    assert out["total"] == 1
    assert out["jobs"][0]["title"] == "New"


def test_search_jobs_live_flag_and_longer_timeout(monkeypatch):
    seen = {}

    def handler(request):
        seen["params"] = dict(request.url.params)
        return _json({"keyword": "x", "source": "live", "total": 0, "jobs": []})

    timeouts = _install(monkeypatch, handler)
    gm.search_jobs("x", live=True)

    assert seen["params"]["live"] == "true"
    assert timeouts[-1] == gm.SCRAPE_TIMEOUT_S

    gm.search_jobs("x")
    assert timeouts[-1] == gm.DEFAULT_TIMEOUT_S


def test_search_jobs_rejects_blank_keyword(monkeypatch):
    _install(monkeypatch, lambda r: pytest.fail("should not call the API"))
    assert gm.search_jobs("   ") == {"error": "keyword is required"}


def test_search_jobs_clamps_limit(monkeypatch):
    seen = []

    def handler(request):
        seen.append(request.url.params["limit"])
        return _json({"keyword": "x", "source": "cache", "total": 0, "jobs": []})

    _install(monkeypatch, handler)
    gm.search_jobs("x", limit=0)
    gm.search_jobs("x", limit=999)
    assert seen == ["1", "100"]


def test_missing_key_returns_error_without_request(monkeypatch):
    monkeypatch.delenv("GOODJOBS_API_KEY")
    _install(monkeypatch, lambda r: pytest.fail("should not call the API"))

    out = gm.search_jobs("x")
    assert out["error"] == gm.MISSING_KEY_ERROR
    assert "GOODJOBS_API_KEY" in out["error"]


def test_unauthorized_mentions_the_key(monkeypatch):
    _install(monkeypatch, lambda r: _json({"detail": "A valid API key is required."}, status=401))
    out = gm.search_jobs("x")
    assert "401" in out["error"] and "GOODJOBS_API_KEY" in out["error"]


def test_quota_error_includes_server_detail(monkeypatch):
    detail = "Daily calls quota reached (500/day). It resets at midnight ICT."
    _install(monkeypatch, lambda r: _json({"detail": detail}, status=429))
    out = gm.search_jobs("x")
    assert "429" in out["error"] and detail in out["error"]


def test_http_error_becomes_error_dict(monkeypatch):
    _install(monkeypatch, lambda r: httpx.Response(500, text="boom"))
    out = gm.search_jobs("x")
    assert "HTTP 500" in out["error"] and "boom" in out["error"]


def test_connection_error_becomes_error_dict(monkeypatch):
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    _install(monkeypatch, handler)
    assert "Could not reach" in gm.search_jobs("x")["error"]


def test_timeout_becomes_error_dict(monkeypatch):
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    _install(monkeypatch, handler)
    assert "timed out" in gm.search_jobs("x")["error"]


def test_recent_jobs_passes_params(monkeypatch):
    seen = {}

    def handler(request):
        seen["url"] = str(request.url.copy_with(query=None))
        seen["params"] = dict(request.url.params)
        return _json({"count": 1, "jobs": [{"title": "t", "link": "https://x"}]})

    _install(monkeypatch, handler)
    out = gm.recent_jobs(n=5, source=" ITViec ")
    assert seen["url"] == "https://api.example.test/v1/jobs/recent"
    assert seen["params"] == {"n": "5", "source": "ITViec"}
    assert out["count"] == 1


def test_semantic_search_passes_query_and_clamps_top_k(monkeypatch):
    seen = []

    def handler(request):
        seen.append((str(request.url.copy_with(query=None)), dict(request.url.params)))
        return _json({"query": "python", "count": 1, "results": [{"title": "t"}]})

    _install(monkeypatch, handler)
    out = gm.semantic_search("python", top_k=3)
    gm.semantic_search("python", top_k=1000)

    assert seen[0] == ("https://api.example.test/v1/jobs/semantic", {"q": "python", "top_k": "3"})
    assert seen[1][1]["top_k"] == "50"
    assert out == {"query": "python", "count": 1, "results": [{"title": "t"}]}


def test_classify_and_normalize_post_expected_bodies(monkeypatch):
    bodies = {}

    def handler(request):
        bodies[request.url.path] = json.loads(request.content)
        return _json({"ok": True})

    _install(monkeypatch, handler)
    gm.classify_keyword("backend developer")
    gm.normalize_city("hcmc")
    assert bodies["/v1/classify"] == {"text": "backend developer"}
    assert bodies["/v1/normalize-city"] == {"text": "hcmc"}


def test_tools_are_registered():
    names = {t.name for t in gm.mcp._tool_manager.list_tools()}
    assert names == {"search_jobs", "recent_jobs", "semantic_search",
                     "classify_keyword", "normalize_city"}
    # cache_status is internal and no longer part of the agent API.
    assert "cache_status" not in names
