import json
import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

from fastapi import Request

# ---------------------------------------------------------------------------
# Search logger — writes one JSON line per search request to search.log
# ---------------------------------------------------------------------------

_LOG_DIR  = os.getenv("LOG_DIR", os.path.join(os.path.dirname(__file__), "..", "logs"))
_LOG_FILE = os.path.join(_LOG_DIR, "search.log")
os.makedirs(_LOG_DIR, exist_ok=True)

_search_logger = logging.getLogger("search")
_search_logger.setLevel(logging.INFO)
_search_logger.propagate = False
_log_handler = RotatingFileHandler(
    _LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
_log_handler.setFormatter(logging.Formatter("%(message)s"))
_search_logger.addHandler(_log_handler)


def log_search(request: Request, keyword: str, location: str) -> None:
    ip = (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )
    entry = {
        "ts":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ip":       ip,
        "keyword":  keyword,
        "location": location,
    }
    print(f"[search] ip={ip} keyword={keyword!r} location={location!r}")
    _search_logger.info(json.dumps(entry, ensure_ascii=False))
