"""Agent API keys: hashed storage in Redis, verification, revocation and daily quotas."""

import hashlib
import json
import os
import secrets
import time
from datetime import datetime, timedelta, timezone

from .cache import get_redis

KEYS_HASH = "apikeys"          # field: sha256(raw key) -> JSON record
KEY_IDS_HASH = "apikeys:ids"   # field: key id -> sha256(raw key)
KEY_PREFIX = "gj_"
QUOTA_KINDS = ("calls", "live")
DEFAULT_CALLS_PER_DAY = int(os.getenv("AGENT_QUOTA_CALLS_PER_DAY", "500"))
DEFAULT_LIVE_PER_DAY = int(os.getenv("AGENT_QUOTA_LIVE_PER_DAY", "30"))
_TZ_ICT = timezone(timedelta(hours=7))


def _hash(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def _today() -> str:
    return datetime.now(_TZ_ICT).strftime("%Y%m%d")


async def create_key(
    name: str,
    contact: str = "",
    calls_per_day: int | None = None,
    live_per_day: int | None = None,
) -> tuple[str, dict]:
    """Mint a new agent key; only its SHA-256 is stored. Returns (raw key, record)."""
    name = (name or "").strip()
    contact = (contact or "").strip()
    calls_per_day = DEFAULT_CALLS_PER_DAY if calls_per_day is None else calls_per_day
    live_per_day = DEFAULT_LIVE_PER_DAY if live_per_day is None else live_per_day

    if not 1 <= len(name) <= 80:
        raise ValueError("name must be 1-80 characters")
    if len(contact) > 200:
        raise ValueError("contact must be at most 200 characters")
    if not 1 <= calls_per_day <= 100000:
        raise ValueError("calls_per_day must be between 1 and 100000")
    if not 0 <= live_per_day <= 10000:
        raise ValueError("live_per_day must be between 0 and 10000")

    raw = KEY_PREFIX + secrets.token_urlsafe(32)
    key_id = secrets.token_hex(16)
    record = {
        "id": key_id,
        "name": name,
        "contact": contact,
        "created_ts": time.time(),
        "revoked": False,
        "calls_per_day": calls_per_day,
        "live_per_day": live_per_day,
    }

    redis = get_redis()
    hashed = _hash(raw)
    # Claim the id first: hsetnx never overwrites, so two keys can't end up
    # sharing an id (which would share one quota and leave one unrevocable).
    if not await redis.hsetnx(KEY_IDS_HASH, key_id, hashed):
        raise RuntimeError("key id collision, retry")
    try:
        # A record without its id mapping would verify but never revoke, so the
        # record write goes through the pipeline as one unit; on failure the
        # claimed id is removed again rather than left dangling.
        async with redis.pipeline(transaction=True) as pipe:
            pipe.hset(KEYS_HASH, hashed, json.dumps(record))
            await pipe.execute()
    except BaseException:
        await redis.hdel(KEY_IDS_HASH, key_id)
        raise
    return raw, record


async def verify_key(raw: str) -> dict | None:
    """Return the key's record, or None when the key is malformed, unknown or revoked."""
    if not isinstance(raw, str) or not raw.startswith(KEY_PREFIX) or len(raw) > 128:
        return None
    stored = await get_redis().hget(KEYS_HASH, _hash(raw))
    if not stored:
        return None
    try:
        record = json.loads(stored)
    except (TypeError, ValueError):
        return None
    if record.get("revoked"):
        return None
    return record


async def revoke_key(key_id: str) -> bool:
    """Revoke a key by id. Returns False when the id or its record is unknown."""
    redis = get_redis()
    hashed = await redis.hget(KEY_IDS_HASH, key_id)
    if not hashed:
        return False
    stored = await redis.hget(KEYS_HASH, hashed)
    if not stored:
        # Dangling id mapping: nothing to revoke, and nothing safe to log.
        return False
    record = json.loads(stored)
    record["revoked"] = True
    await redis.hset(KEYS_HASH, hashed, json.dumps(record))
    return True


async def usage_today(key_id: str) -> dict:
    """Return today's (ICT) usage counters for one key."""
    redis = get_redis()
    usage: dict[str, int] = {}
    for kind in QUOTA_KINDS:
        raw = await redis.get(f"apikey_usage:{key_id}:{_today()}:{kind}")
        usage[kind] = int(raw or 0)
    return usage


async def list_keys() -> list[dict]:
    """Every key record plus today's usage, newest first. Never includes hashes or raw keys."""
    records = []
    for stored in (await get_redis().hgetall(KEYS_HASH)).values():
        try:
            records.append(json.loads(stored))
        except (TypeError, ValueError):
            continue
    for record in records:
        record["usage_today"] = await usage_today(record.get("id", ""))
    records.sort(key=lambda r: r.get("created_ts", 0), reverse=True)
    return records


async def consume(record: dict, kind: str) -> tuple[bool, int, int]:
    """Count one use of `kind` for this key. Returns (allowed, used_today, limit)."""
    if kind not in QUOTA_KINDS:
        raise ValueError("kind must be one of calls, live")
    counter = f"apikey_usage:{record.get('id', '')}:{_today()}:{kind}"
    redis = get_redis()
    n = await redis.incr(counter)
    if n == 1:
        await redis.expire(counter, 2 * 86400)
    limit = int(record.get(f"{kind}_per_day", 0))
    return n <= limit, n, limit
