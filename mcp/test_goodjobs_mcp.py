import json

import httpx
import pytest

import goodjobs_mcp as gm


def _install(monkeypatch, handler):
    """Route every httpx.Client the server creates through a mock transport."""
    real_client = httpx.Client

    def factory(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(gm.httpx, "Client", factory)


def _json(body, status=200):
    return httpx.Response(status, json=body)


@pytest.fixture(autouse=True)
def _api_url(monkeypatch):
    monkeypatch.setenv("GOODJOBS_API_URL", "https://api.example.test/")


def test_api_url_strips_trailing_slash():
    assert gm._api_url() == "https://api.example.test"


def test_search_jobs_posts_scrape_body_and_sorts_newest_first(monkeypatch):
    seen = {}

    def handler(request):
        seen["method"] = request.method
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return _json([
            {"title": "Old", "company": "A", "location": "HCMC", "source": "TopCV",
             "posted_date": "2026-01-01", "link": "https://x/1", "skills": [], "posted_ts": 1.0,
             "description": "should be dropped"},
            {"title": "New", "company": "B", "location": "HCMC", "source": "LinkedIn",
             "posted_date": "2026-02-01", "link": "https://x/2", "skills": ["Python"], "posted_ts": 2.0},
        ])

    _install(monkeypatch, handler)
    out = gm.search_jobs("Backend Engineer", limit=1)

    assert seen["method"] == "POST"
    assert seen["url"] == "https://api.example.test/scrape"
    assert seen["body"] == {"keyword": "Backend Engineer", "location": "Ho Chi Minh City", "country": "VN"}
    assert out["total"] == 2
    assert [j["title"] for j in out["jobs"]] == ["New"]
    assert set(out["jobs"][0]) == set(gm._JOB_FIELDS)
    assert "description" not in out["jobs"][0]


def test_search_jobs_rejects_blank_keyword(monkeypatch):
    _install(monkeypatch, lambda r: pytest.fail("should not call the API"))
    assert gm.search_jobs("   ") == {"error": "keyword is required"}


def test_search_jobs_clamps_limit(monkeypatch):
    _install(monkeypatch, lambda r: _json([{"title": f"j{i}", "posted_ts": i} for i in range(5)]))
    assert len(gm.search_jobs("x", limit=0)["jobs"]) == 1
    assert len(gm.search_jobs("x", limit=999)["jobs"]) == 5


def test_http_error_becomes_error_dict(monkeypatch):
    _install(monkeypatch, lambda r: httpx.Response(500, text="boom"))
    out = gm.search_jobs("x")
    assert "HTTP 500" in out["error"] and "boom" in out["error"]


def test_connection_error_becomes_error_dict(monkeypatch):
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    _install(monkeypatch, handler)
    assert "Could not reach" in gm.cache_status()["error"]


def test_timeout_becomes_error_dict(monkeypatch):
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    _install(monkeypatch, handler)
    assert "timed out" in gm.search_jobs("x")["error"]


def test_recent_jobs_passes_params(monkeypatch):
    seen = {}

    def handler(request):
        seen["params"] = dict(request.url.params)
        return _json([{"title": "t", "company": "c", "location": "l", "source": "ITViec",
                       "link": "https://x", "description": "drop me"}])

    _install(monkeypatch, handler)
    out = gm.recent_jobs(n=5, source=" ITViec ")
    assert seen["params"] == {"n": "5", "source": "ITViec"}
    assert out["count"] == 1 and "description" not in out["jobs"][0]


def test_semantic_search_passes_query(monkeypatch):
    seen = {}

    def handler(request):
        seen["params"] = dict(request.url.params)
        return _json({"query": "python", "count": 1, "results": [{"title": "t"}]})

    _install(monkeypatch, handler)
    out = gm.semantic_search("python", top_k=3)
    assert seen["params"] == {"q": "python", "top_k": "3"}
    assert out == {"query": "python", "count": 1, "results": [{"title": "t"}]}


def test_classify_and_normalize_post_expected_bodies(monkeypatch):
    bodies = {}

    def handler(request):
        bodies[request.url.path] = json.loads(request.content)
        return _json({"ok": True})

    _install(monkeypatch, handler)
    gm.classify_keyword("backend developer")
    gm.normalize_city("hcmc")
    assert bodies["/classify-input"] == {"keyword": "backend developer"}
    assert bodies["/normalize-city"] == {"keyword": "hcmc", "location": "hcmc"}


def test_cache_status_is_get(monkeypatch):
    _install(monkeypatch, lambda r: _json({"total": 1}) if r.method == "GET" else pytest.fail("not GET"))
    assert gm.cache_status() == {"total": 1}


def test_tools_are_registered():
    names = {t.name for t in gm.mcp._tool_manager.list_tools()}
    assert names == {"search_jobs", "recent_jobs", "semantic_search",
                     "classify_keyword", "normalize_city", "cache_status"}
