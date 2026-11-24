"""Admin account login: scrypt password check, Redis-backed sessions, login-failure limiter."""

import asyncio
import base64
import getpass
import hashlib
import hmac
import json
import os
import secrets
import sys
import time

from starlette.concurrency import run_in_threadpool

from .cache import get_redis

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "").strip()
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH", "").strip()
SESSION_TTL = int(os.getenv("ADMIN_SESSION_TTL", "28800"))
SESSION_PREFIX = "gjs_"
LOGIN_MAX_FAILURES = 5
LOGIN_GLOBAL_MAX_FAILURES = 100
LOGIN_SCRYPT_CONCURRENCY = 2
LOGIN_WINDOW = 900
_N, _R, _P, _DKLEN = 2**14, 8, 1, 32

_LOGIN_FAIL_PREFIX = "admin_login_fail:"
_LOGIN_GLOBAL_KEY = "admin_login_global"
_SESSION_KEY_PREFIX = "admin_session:"

_scrypt_sem: asyncio.Semaphore | None = None


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def hash_password(password: str) -> str:
    """Hash a password for ADMIN_PASSWORD_HASH. Format has no '$' (docker compose interpolates it)."""
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(password.encode(), salt=salt, n=_N, r=_R, p=_P, dklen=_DKLEN)
    return f"scrypt:{_N}:{_R}:{_P}:{_b64(salt)}:{_b64(dk)}"


def verify_password(password: str, stored: str) -> bool:
    """Check a password against a stored hash. Any parse/compute error is just a mismatch."""
    try:
        algo, n, r, p, salt_b64, dk_b64 = (stored or "").split(":")
        if algo != "scrypt":
            return False
        salt = _unb64(salt_b64)
        expected = _unb64(dk_b64)
        dk = hashlib.scrypt(
            password.encode(), salt=salt, n=int(n), r=int(r), p=int(p),
            dklen=len(expected), maxmem=64 * 1024 * 1024,
        )
        return hmac.compare_digest(dk, expected)
    except (AttributeError, MemoryError, OverflowError, TypeError, ValueError):
        return False


def accounts_configured() -> bool:
    """True when an admin account is configured (globals read per call so tests can patch them)."""
    return bool(ADMIN_USERNAME and ADMIN_PASSWORD_HASH)


def _credential_fingerprint() -> str:
    """Fingerprint of the current admin credentials.

    Sessions carry it, so rotating ADMIN_USERNAME/ADMIN_PASSWORD_HASH invalidates
    every outstanding gjs_ token instead of leaving old ones valid forever.
    """
    return hashlib.sha256(f"{ADMIN_USERNAME}\n{ADMIN_PASSWORD_HASH}".encode()).hexdigest()[:32]


def check_credentials(username: str, password: str) -> bool:
    """Check username+password. Always hashes, so a wrong username costs the same as a wrong password."""
    pw_ok = verify_password(password or "", ADMIN_PASSWORD_HASH)
    user_ok = hmac.compare_digest((username or "").encode(), ADMIN_USERNAME.encode())
    return accounts_configured() and user_ok and pw_ok


async def check_credentials_async(username: str, password: str) -> bool:
    """check_credentials off the event loop, capped at LOGIN_SCRYPT_CONCURRENCY at a time.

    scrypt costs 16 MB and real CPU per call, so unbounded parallel logins would
    exhaust the threadpool and the host's RAM for everyone else.
    """
    global _scrypt_sem
    if _scrypt_sem is None:
        _scrypt_sem = asyncio.Semaphore(LOGIN_SCRYPT_CONCURRENCY)
    async with _scrypt_sem:
        return await run_in_threadpool(check_credentials, username, password)


async def create_session(username: str) -> str:
    """Create a session for the admin account. Only the token's SHA-256 is stored."""
    token = SESSION_PREFIX + secrets.token_urlsafe(32)
    key = _SESSION_KEY_PREFIX + hashlib.sha256(token.encode()).hexdigest()
    payload = json.dumps(
        {
            "username": username,
            "created_ts": time.time(),
            "fp": _credential_fingerprint(),
        }
    )
    await get_redis().set(key, payload, ex=SESSION_TTL)
    return token


async def get_session(token: str) -> dict | None:
    """Return the session payload, or None when the token is malformed, unknown,
    expired, or was minted for credentials that have since changed."""
    if not isinstance(token, str) or not token.startswith(SESSION_PREFIX) or len(token) > 128:
        return None
    key = _SESSION_KEY_PREFIX + hashlib.sha256(token.encode()).hexdigest()
    stored = await get_redis().get(key)
    if not stored:
        return None
    try:
        payload = json.loads(stored)
    except (TypeError, ValueError):
        return None
    fingerprint = payload.get("fp")
    if not fingerprint or not hmac.compare_digest(str(fingerprint), _credential_fingerprint()):
        return None
    return payload


async def delete_session(token: str) -> None:
    """Log a session out."""
    key = _SESSION_KEY_PREFIX + hashlib.sha256(token.encode()).hexdigest()
    await get_redis().delete(key)


async def reserve_attempt(ip: str) -> bool:
    """Count this attempt before hashing. False when the IP or the global budget is spent."""
    redis = get_redis()
    ip_key = f"{_LOGIN_FAIL_PREFIX}{ip[:64]}"
    global_key = _LOGIN_GLOBAL_KEY
    # Increment first, then decide: checking a counter before counting lets a
    # burst of parallel requests all pass the check before any increment lands.
    n_ip = await redis.incr(ip_key)
    # NX on every call: a key whose first expire was lost still gets a TTL, so an
    # IP can never be locked out forever.
    await redis.expire(ip_key, LOGIN_WINDOW, nx=True)
    if n_ip > LOGIN_MAX_FAILURES:
        # Already-blocked attempts must not burn the shared budget, or one noisy
        # IP could lock every admin out for the whole window.
        return False
    n_global = await redis.incr(global_key)
    await redis.expire(global_key, LOGIN_WINDOW, nx=True)
    return n_global <= LOGIN_GLOBAL_MAX_FAILURES


async def clear_failures(ip: str) -> None:
    """Forget this IP's failed logins after a successful one.

    Also gives the global budget its slot back: a successful login reserves an
    attempt like any other, and it must not count against other requests.
    """
    redis = get_redis()
    await redis.delete(f"{_LOGIN_FAIL_PREFIX}{ip[:64]}")
    # Only decrement a live counter: decr on an expired key would recreate it at
    # -1 with no TTL, handing the next window an extra slot.
    if int(await redis.get(_LOGIN_GLOBAL_KEY) or 0) > 0:
        await redis.decr(_LOGIN_GLOBAL_KEY)


if __name__ == "__main__":
    if sys.argv[1:] == ["hash"]:
        pw = getpass.getpass("Admin password: ")
        confirm = getpass.getpass("Repeat password: ")
        if pw != confirm:
            print("Error: passwords do not match", file=sys.stderr)
            sys.exit(1)
        if len(pw) < 12:
            print("Error: password must be at least 12 characters", file=sys.stderr)
            sys.exit(1)
        print(hash_password(pw))
    else:
        print("usage: python -m src.admin_auth hash", file=sys.stderr)
        sys.exit(2)
