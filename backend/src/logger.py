import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

from fastapi import Request

# ---------------------------------------------------------------------------
# Logger setup — app.log for general events, search.log for search requests
# ---------------------------------------------------------------------------

_LOG_DIR   = os.getenv("LOG_DIR", os.path.join(os.path.dirname(__file__), "..", "logs"))
_APP_FILE  = os.path.join(_LOG_DIR, "app.log")
_LOG_FILE  = os.path.join(_LOG_DIR, "search.log")
os.makedirs(_LOG_DIR, exist_ok=True)

# App logger — general application events
_app_logger = logging.getLogger("app")
_app_logger.setLevel(logging.INFO)
_app_logger.propagate = False
_app_handler = RotatingFileHandler(
    _APP_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
_TZ_ICT = timezone(timedelta(hours=7))

logging.Formatter.converter = lambda *_: datetime.now(_TZ_ICT).timetuple()
_app_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s — %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_app_logger.addHandler(_app_handler)

# Also log to console (stderr)
_console_handler = logging.StreamHandler(sys.stderr)
_console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
_app_logger.addHandler(_console_handler)

# Search logger — writes one JSON line per search request to search.log
_search_logger = logging.getLogger("search")
_search_logger.setLevel(logging.INFO)
_search_logger.propagate = False
_search_handler = RotatingFileHandler(
    _LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
_search_handler.setFormatter(logging.Formatter("%(message)s"))
_search_logger.addHandler(_search_handler)


def log_app(message: str, level: str = "INFO") -> None:
    """Log an application event."""
    getattr(_app_logger, level.lower())(message)


def log_search(request: Request, keyword: str, location: str) -> None:
    ip = (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )
    entry = {
        "ts":       datetime.now(_TZ_ICT).strftime("%Y-%m-%dT%H:%M:%S+07:00"),
        "ip":       ip,
        "keyword":  keyword,
        "location": location,
    }
    log_app(f"search: ip={ip} keyword={keyword!r} location={location!r}")
    _search_logger.info(json.dumps(entry, ensure_ascii=False))
