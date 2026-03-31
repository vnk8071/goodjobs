import hashlib
import json
import os

import requests

from .logger import log_app

_CF_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
_CF_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN", "")
_EMBED_MODEL = "@cf/google/embeddinggemma-300m"
_INDEX_NAME = "goodjobs"
_EMBED_DIM = 768

_CF_AI_URL = f"https://api.cloudflare.com/client/v4/accounts/{_CF_ACCOUNT_ID}/ai/run/{_EMBED_MODEL}"
_CF_VEC_BASE = f"https://api.cloudflare.com/client/v4/accounts/{_CF_ACCOUNT_ID}/vectorize/v2/indexes/{_INDEX_NAME}"


def _headers() -> dict:
    return {"Authorization": f"Bearer {_CF_API_TOKEN}", "Content-Type": "application/json"}


def ensure_index() -> bool:
    """Create the Vectorize index if it does not already exist. Returns True on success."""
    if not _CF_ACCOUNT_ID or not _CF_API_TOKEN:
        log_app("[vector] CF credentials not set — skipping index creation", "WARN")
        return False
    url = f"https://api.cloudflare.com/client/v4/accounts/{_CF_ACCOUNT_ID}/vectorize/v2/indexes"
    resp = requests.get(url, headers=_headers(), timeout=15)
    if resp.ok:
        existing = {idx["name"] for idx in resp.json().get("result", [])}
        if _INDEX_NAME in existing:
            log_app(f"[vector] index '{_INDEX_NAME}' already exists")
            return True
    body = {"name": _INDEX_NAME, "config": {"dimensions": _EMBED_DIM, "metric": "cosine"}}
    resp = requests.post(url, headers=_headers(), json=body, timeout=15)
    if resp.ok and resp.json().get("success"):
        log_app(f"[vector] index '{_INDEX_NAME}' created")
        return True
    log_app(f"[vector] failed to create index: {resp.text}", "ERROR")
    return False


def embed_texts(texts: list[str]) -> list[list[float]] | None:
    """Call Cloudflare Workers AI to get embeddings for a list of texts (max 100).

    Returns a list of float vectors or None on error.
    """
    if not _CF_ACCOUNT_ID or not _CF_API_TOKEN:
        return None
    if not texts:
        return []
    try:
        resp = requests.post(_CF_AI_URL, headers=_headers(), json={"text": texts}, timeout=(10, 90))
        if resp.ok:
            result = resp.json().get("result", {})
            return result.get("data")
        log_app(f"[vector] embed error {resp.status_code}: {resp.text}", "ERROR")
        return None
    except Exception as e:
        log_app(f"[vector] embed exception: {e}", "ERROR")
        return None


def _job_id(job: dict) -> str:
    """Stable ID from job link (SHA-1 hex, truncated to 40 chars)."""
    return hashlib.sha1(job.get("link", "").encode()).hexdigest()


def _job_text(job: dict) -> str:
    """Concat title + description into a single string for embedding."""
    title = job.get("title", "")
    desc = job.get("description", "")[:2000]
    return f"{title}. {desc}".strip()


def upsert_jobs(jobs: list[dict]) -> bool:
    """Embed job title+description and upsert vectors into Cloudflare Vectorize.

    Processes jobs in batches of 100 (API limit). Returns True if all batches succeed.
    """
    if not _CF_ACCOUNT_ID or not _CF_API_TOKEN:
        return False
    if not jobs:
        return True

    BATCH = 100
    all_ok = True
    for i in range(0, len(jobs), BATCH):
        batch = jobs[i : i + BATCH]
        texts = [_job_text(j) for j in batch]
        vectors = embed_texts(texts)
        if vectors is None:
            log_app(f"[vector] embed failed for batch {i//BATCH}", "ERROR")
            all_ok = False
            continue

        ndjson_lines = []
        for job, vec in zip(batch, vectors):
            record = {
                "id": _job_id(job),
                "values": vec,
                "metadata": {
                    "title": job.get("title", "")[:256],
                    "company": job.get("company", "")[:128],
                    "location": job.get("location", "")[:128],
                    "source": job.get("source", ""),
                    "link": job.get("link", ""),
                    "posted": job.get("posted", ""),
                },
            }
            ndjson_lines.append(json.dumps(record, ensure_ascii=False))

        ndjson_body = "\n".join(ndjson_lines)
        upsert_headers = {
            "Authorization": f"Bearer {_CF_API_TOKEN}",
            "Content-Type": "application/x-ndjson",
        }
        resp = requests.post(
            f"{_CF_VEC_BASE}/upsert",
            headers=upsert_headers,
            data=ndjson_body.encode("utf-8"),
            timeout=(10, 60),
        )
        if not (resp.ok and resp.json().get("success")):
            log_app(f"[vector] upsert failed batch {i//BATCH}: {resp.text}", "ERROR")
            all_ok = False
        else:
            log_app(f"[vector] upserted {len(batch)} vectors (batch {i//BATCH})")

    return all_ok


def rerank_jobs_by_vector(jobs: list[dict], query: str, top_k: int = 50) -> list[dict]:
    """Re-rank a list of job dicts by semantic similarity to the query.

    Calls vector search, builds a link→score map, sorts jobs by score descending,
    and annotates each job with _vector_score. Returns the original list unchanged
    on any error.
    """
    if not jobs:
        return jobs
    try:
        results = search(query, top_k=top_k)
    except Exception:
        return jobs
    score_map = {r["link"]: r["score"] for r in results if "link" in r}
    jobs.sort(key=lambda j: score_map.get(j.get("link", ""), 0.0), reverse=True)
    for j in jobs:
        j["_vector_score"] = score_map.get(j.get("link", ""), 0.0)
    return jobs


def search(query: str, top_k: int = 20) -> list[dict]:
    """Embed the query and query Vectorize for the top-k most similar jobs.

    Returns a list of metadata dicts (title, company, location, source, link, posted),
    sorted by score descending. Returns [] on any error.
    """
    if not _CF_ACCOUNT_ID or not _CF_API_TOKEN:
        return []
    vectors = embed_texts([query])
    if not vectors:
        return []
    query_vec = vectors[0]

    try:
        body = {
            "vector": query_vec,
            "topK": top_k,
            "returnMetadata": "all",
        }
        resp = requests.post(
            f"{_CF_VEC_BASE}/query",
            headers=_headers(),
            json=body,
            timeout=15,
        )
        if not resp.ok:
            log_app(f"[vector] query error {resp.status_code}: {resp.text}", "ERROR")
            return []
        matches = resp.json().get("result", {}).get("matches", [])
        results = []
        for m in matches:
            meta = m.get("metadata") or {}
            meta["score"] = round(m.get("score", 0.0), 4)
            results.append(meta)
        return results
    except Exception as e:
        log_app(f"[vector] search exception: {e}", "ERROR")
        return []


def delete_by_ids(ids: list[str]) -> bool:
    """Delete vectors from Cloudflare Vectorize by their IDs.

    Returns True if the delete call succeeded (or was a no-op).
    IDs are the same SHA-1 hashes used during upsert.
    """
    if not _CF_ACCOUNT_ID or not _CF_API_TOKEN:
        return False
    if not ids:
        return True
    try:
        resp = requests.post(
            f"{_CF_VEC_BASE}/delete_by_ids",
            headers={**_headers(), "Content-Type": "application/json"},
            json={"ids": ids},
            timeout=30,
        )
        if not resp.ok:
            log_app(f"[vector] delete_by_ids error {resp.status_code}: {resp.text}", "ERROR")
            return False
        log_app(f"[vector] deleted {len(ids)} vectors")
        return True
    except Exception as e:
        log_app(f"[vector] delete_by_ids exception: {e}", "ERROR")
        return False
