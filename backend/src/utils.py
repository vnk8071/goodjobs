import re
from datetime import date, timedelta
from html import unescape

from bs4 import BeautifulSoup

from .constants import DESC_MAX_CHARS


def _fmt_num(val) -> str:
    """Format a number as a readable string with comma separators."""
    try:
        n = float(val)
        if n == int(n):
            return f"{int(n):,}"
        return f"{n:,.2f}"
    except (ValueError, TypeError):
        return str(val)


def _parse_iso_date(s: str) -> date | None:
    """Parse a YYYY-MM-DD string into a date object, returning None on failure."""
    try:
        parts = s.strip().split("-")
        if len(parts) == 3:
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        pass
    return None


def _relative_display(days_ago: int) -> str:
    """Return a human-readable relative time string (e.g. "3 days ago")."""
    if days_ago <= 0:
        return "Today"
    if days_ago == 1:
        return "1 day ago"
    if days_ago < 7:
        return f"{days_ago} days ago"
    weeks = days_ago // 7
    if weeks == 1:
        return "1 week ago"
    return f"{weeks} weeks ago"


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode common HTML entities from a string."""
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = (
        text.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&#39;", "'")
            .replace("&nbsp;", " ")
            .replace("&#x2F;", "/")
            .replace("&#x27;", "'")
    )
    return text


def _clean_html(html: str) -> str:
    """Sanitize HTML: remove scripts, styles, and noisy attributes, keeping href/src/alt."""
    html = unescape(html)
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    for tag in soup.find_all(True):
        for attr in list(tag.attrs):
            if attr not in ("href", "src", "alt"):
                del tag.attrs[attr]
    return str(soup).strip()


def _extract_html(element) -> str:
    """Extract sanitized inner HTML from a BeautifulSoup element."""
    return _clean_html(element.decode_contents())


def _truncate(text: str, max_chars: int = DESC_MAX_CHARS) -> str:
    """Truncate HTML at max_chars characters, cutting at a tag boundary."""
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars]
    last_close = cut.rfind(">")
    if last_close != -1:
        cut = cut[: last_close + 1]
    return cut
