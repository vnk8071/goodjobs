import collections
import time

_RATE_LIMIT_WINDOW  = 90
_RATE_LIMIT_MAX     = 4
_RATE_LIMIT_MAX_CONCURRENT = 1

_ip_timestamps: dict[str, collections.deque] = collections.defaultdict(collections.deque)
_ip_active: dict[str, int] = collections.defaultdict(int)


def check_rate_limit(ip: str) -> str | None:
    """Return an error message if the IP is rate-limited, else None."""
    now = time.time()
    window_start = now - _RATE_LIMIT_WINDOW
    dq = _ip_timestamps[ip]
    while dq and dq[0] < window_start:
        dq.popleft()
    if len(dq) >= _RATE_LIMIT_MAX:
        return "Bạn đã tìm kiếm quá nhiều lần. Vui lòng chờ 1 phút rồi thử lại."
    if _ip_active[ip] >= _RATE_LIMIT_MAX_CONCURRENT:
        return "Bạn đang có một tìm kiếm đang chạy. Vui lòng chờ kết quả trước."
    dq.append(now)
    return None


def ip_active_inc(ip: str) -> None:
    """Increment the active scrape counter for an IP."""
    _ip_active[ip] += 1


def ip_active_dec(ip: str) -> None:
    """Decrement the active scrape counter for an IP (floor at 0)."""
    _ip_active[ip] = max(0, _ip_active[ip] - 1)
