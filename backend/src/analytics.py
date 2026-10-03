"""Admin analytics over search.log.

search.log mixes two kinds of traffic:
  - user searches (real visitors, intent job_title / cv_or_skills / not_job / warmup_job)
  - automated warmup scrapes (ip "warmup", intent "warmup"), written by
    warmup.py once per keyword×location pair per scrape cycle.

Everything user-facing here counts user searches only. Warmup traffic is
reported separately, as per-cycle scrape health: a morning and an evening
cycle per day, each expected to cover every warmup pair.

Pure functions only (no Redis, no FastAPI) so they can be unit-tested directly.
"""

from __future__ import annotations

import glob
import json
import os
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone

TZ_ICT = timezone(timedelta(hours=7))

WARMUP_IP = "warmup"
WARMUP_INTENT = "warmup"

# ICT hour windows for the two scrape cycles (warmup._SCRAPE_HOURS = 10, 17).
# A cycle that starts at 10:00 or 17:00 and runs for ~an hour logs its entries
# inside these windows; the end is exclusive.
CYCLE_WINDOWS = {
    "morning": (9, 14),
    "evening": (16, 21),
}

# A cycle is "ok" once it has logged at least this share of the expected pairs
# (a few pairs can legitimately be skipped by warmup's own dedup/timeouts).
CYCLE_OK_RATIO = 0.9

_LOG_FILE_RE = re.compile(r"search\.log(\.\d+)?$")


def read_search_entries(log_dir: str) -> list[dict]:
    """Read every search.log* file (rotated first, oldest to newest) into dicts."""
    paths = sorted(
        (p for p in glob.glob(os.path.join(log_dir, "search.log*")) if _LOG_FILE_RE.search(p)),
        key=lambda p: (0 if p.endswith("search.log") else -int(p.rsplit(".", 1)[-1])),
    )
    entries: list[dict] = []
    for path in paths:
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entries.append(json.loads(line))
                    except ValueError:
                        continue
        except OSError:
            continue
    return entries


def is_warmup(entry: dict) -> bool:
    """True for automated scrape-cycle entries, not for user searches."""
    return entry.get("ip") == WARMUP_IP or entry.get("intent") == WARMUP_INTENT


def _parse_ts(entry: dict) -> datetime | None:
    ts = entry.get("ts")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return None
    return dt.astimezone(TZ_ICT) if dt.tzinfo else dt.replace(tzinfo=TZ_ICT)


def cycle_status(count: int, expected: int, day: date, window: str, now: datetime) -> str:
    """Classify one scrape cycle on one day.

    ok       - at least CYCLE_OK_RATIO of expected pairs logged
    partial  - some pairs logged, but not enough (cycle ran short or died mid-way)
    missed   - nothing logged for a window that has already closed
    pending  - the window is still open (today only), or nothing is expected yet
    unknown  - the expected pair count is unavailable
    """
    if expected <= 0:
        return "unknown"
    if count >= expected * CYCLE_OK_RATIO:
        return "ok"
    window_end = CYCLE_WINDOWS[window][1]
    still_open = day > now.date() or (day == now.date() and now.hour < window_end)
    if still_open:
        return "pending"
    return "missed" if count == 0 else "partial"


def _empty_day() -> dict:
    return {
        "user": 0,
        "unique_ips": set(),
        "scrape": 0,
        "cycles": {w: 0 for w in CYCLE_WINDOWS},
        "keywords": {w: set() for w in CYCLE_WINDOWS},
    }


def summarize(entries: list[dict], now: datetime, expected_pairs: int, days: int = 30) -> dict:
    """Build the admin analytics payload from raw search.log entries."""
    today = now.date()
    week_start = today - timedelta(days=6)
    month_start = today - timedelta(days=days - 1)

    daily: dict[date, dict] = {}
    week_keywords: Counter = Counter()
    hour_today: Counter = Counter()
    intents: Counter = Counter()
    all_ips: set[str] = set()
    user_total = scrape_total = 0
    user_today = scrape_today = 0
    user_list: list[tuple[datetime, dict]] = []

    for e in entries:
        dt = _parse_ts(e)
        if dt is None:
            continue
        d = dt.date()
        bucket = daily.setdefault(d, _empty_day())

        if is_warmup(e):
            scrape_total += 1
            bucket["scrape"] += 1
            if d == today:
                scrape_today += 1
            for window, (lo, hi) in CYCLE_WINDOWS.items():
                if lo <= dt.hour < hi:
                    bucket["cycles"][window] += 1
                    kw = (e.get("keyword") or "").strip().lower()
                    if kw:
                        bucket["keywords"][window].add(kw)
                    break
            continue

        user_total += 1
        bucket["user"] += 1
        ip = e.get("ip", "")
        if ip:
            bucket["unique_ips"].add(ip)
            all_ips.add(ip)
        user_list.append((dt, e))

        if d == today:
            user_today += 1
            hour_today[dt.hour] += 1
        if d >= week_start:
            kw = (e.get("keyword") or "").strip().lower()
            if kw:
                week_keywords[kw] += 1
        intent = e.get("intent", "")
        if intent in ("job_title", "cv_or_skills", "not_job"):
            intents[intent] += 1

    recent = [
        {
            "ts": e.get("ts", ""),
            "ip": e.get("ip", ""),
            "keyword": e.get("keyword", ""),
            "location": e.get("location", ""),
            "intent": e.get("intent", ""),
        }
        for _, e in sorted(user_list, key=lambda x: x[0], reverse=True)[:100]
    ]

    daily_rows = []
    missed = 0
    for i in range(days):
        d = month_start + timedelta(days=i)
        b = daily.get(d) or _empty_day()
        row = {
            "date": d.isoformat(),
            "user": b["user"],
            "unique_ips": len(b["unique_ips"]),
            "scrape": b["scrape"],
            "morning": _cycle_row(b, "morning", expected_pairs, d, now),
            "evening": _cycle_row(b, "evening", expected_pairs, d, now),
        }
        missed += (row["morning"]["status"] == "missed") + (row["evening"]["status"] == "missed")
        daily_rows.append(row)

    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "expected_cycle_size": expected_pairs,
        # User traffic only. Scrape-job traffic is reported under scrape_* keys.
        "total_requests": user_total,
        "today_requests": user_today,
        "unique_ips": len(all_ips),
        "scrape_requests": scrape_total,
        "today_scrape_requests": scrape_today,
        "top_keywords": [
            {"keyword": kw, "count": cnt} for kw, cnt in week_keywords.most_common(10)
        ],
        "requests_by_hour": {str(h): hour_today.get(h, 0) for h in range(24)},
        "requests_by_week": [
            {"date": r["date"], "count": r["user"]}
            for r in daily_rows
            if date.fromisoformat(r["date"]) >= week_start
        ],
        "intent_breakdown": {
            "job_title": intents.get("job_title", 0),
            "cv_or_skills": intents.get("cv_or_skills", 0),
            "not_job": intents.get("not_job", 0),
        },
        "recent_searches": recent,
        "daily": daily_rows,
        "missed_cycles": missed,
    }


def _cycle_row(bucket: dict, window: str, expected: int, day: date, now: datetime) -> dict:
    count = bucket["cycles"][window]
    return {
        "count": count,
        "expected": expected,
        "status": cycle_status(count, expected, day, window, now),
        "keywords": len(bucket["keywords"][window]),
    }


def day_detail(entries: list[dict], day: date, now: datetime, expected_pairs: int, limit: int = 500) -> dict:
    """Everything the admin saw on one day: user searches (newest first) and scrape cycles."""
    searches: list[tuple[datetime, dict]] = []
    cycles: dict[str, dict] = {
        w: {"count": 0, "first_ts": None, "last_ts": None, "keywords": set()} for w in CYCLE_WINDOWS
    }
    ips: set[str] = set()

    for e in entries:
        dt = _parse_ts(e)
        if dt is None or dt.date() != day:
            continue
        if is_warmup(e):
            for window, (lo, hi) in CYCLE_WINDOWS.items():
                if lo <= dt.hour < hi:
                    c = cycles[window]
                    c["count"] += 1
                    c["keywords"].add((e.get("keyword") or "").strip().lower())
                    # ts strings share the +07:00 offset, so lexical order is time order.
                    ts = e.get("ts", "")
                    if c["first_ts"] is None or ts < c["first_ts"]:
                        c["first_ts"] = ts
                    if c["last_ts"] is None or ts > c["last_ts"]:
                        c["last_ts"] = ts
                    break
            continue
        searches.append((dt, e))
        if e.get("ip"):
            ips.add(e["ip"])

    return {
        "date": day.isoformat(),
        "expected_cycle_size": expected_pairs,
        "user_requests": len(searches),
        "unique_ips": len(ips),
        "cycles": {
            w: {
                "count": c["count"],
                "expected": expected_pairs,
                "status": cycle_status(c["count"], expected_pairs, day, w, now),
                "first_ts": c["first_ts"],
                "last_ts": c["last_ts"],
                "keywords": len(c["keywords"]),
            }
            for w, c in cycles.items()
        },
        "searches": [
            {
                "ts": e.get("ts", ""),
                "ip": e.get("ip", ""),
                "keyword": e.get("keyword", ""),
                "location": e.get("location", ""),
                "intent": e.get("intent", ""),
            }
            for _, e in sorted(searches, key=lambda x: x[0], reverse=True)[:limit]
        ],
    }
