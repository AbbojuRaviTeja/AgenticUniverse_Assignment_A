"""Pure normalisers for the lead and event rules. No database imports."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

IST = timezone(timedelta(hours=5, minutes=30))

_HEADER_RUN = re.compile(r"[\s_]+")
_MONTHS = {
    name.lower(): index
    for index, name in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        start=1,
    )
}
_MONEY_PREFIX = re.compile(r"^(?:₹|rs\.?|inr)\s*", re.IGNORECASE)
_MONEY_BODY = re.compile(r"\d+(?:\.\d+)?")
_ISO_DATE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_DMY_SLASH = re.compile(r"(\d{2})/(\d{2})/(\d{4})")
_DMY_DASH = re.compile(r"(\d{2})-(\d{2})-(\d{4})")
_DMON_DATE = re.compile(r"(\d{2})-([A-Za-z]{3})-(\d{4})")


def normalize_header(header: str) -> str:
    """Lower-case, trim, and collapse '_' or whitespace into a single space."""
    return _HEADER_RUN.sub(" ", header.strip().lower()).strip()


def canonical_phone(value: str) -> str | None:
    """Return +91XXXXXXXXXX, or None when the number is not an Indian mobile."""
    digits = re.sub(r"\D", "", value)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in "6789":
        return "+91" + digits
    return None


def _ymd(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_date(value: str) -> date | None:
    """Day-first dates. Impossible calendar dates are invalid."""
    text = value.strip()
    match = _ISO_DATE.fullmatch(text)
    if match:
        return _ymd(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    match = _DMY_SLASH.fullmatch(text)
    if match:
        return _ymd(int(match.group(3)), int(match.group(2)), int(match.group(1)))
    match = _DMY_DASH.fullmatch(text)
    if match:
        return _ymd(int(match.group(3)), int(match.group(2)), int(match.group(1)))
    match = _DMON_DATE.fullmatch(text)
    if match:
        month = _MONTHS.get(match.group(2).lower())
        if month is None:
            return None
        return _ymd(int(match.group(3)), month, int(match.group(1)))
    return None


def parse_decimal(value: str) -> Decimal | None:
    """Strip a currency prefix and thousands separators. Reject negatives."""
    text = _MONEY_PREFIX.sub("", value.strip())
    text = text.replace(",", "").strip()
    if not _MONEY_BODY.fullmatch(text):
        return None
    amount = Decimal(text)
    if amount < 0:
        return None
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def canonical_enum(value: str, allowed: frozenset[str]) -> str | None:
    lowered = value.strip().lower()
    if lowered in allowed:
        return lowered
    return None


def parse_occurred_at(value: str) -> datetime | None:
    """ISO-8601, including fractional seconds. Naive means IST. None if missing or not a time."""
    text = value.strip()
    if not text or ":" not in text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=IST)
    return parsed


def format_ist(value: datetime) -> str:
    """ISO-8601 in IST with seconds, for example 2026-10-14T17:55:24+05:30."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=IST)
    local = value.astimezone(IST)
    return local.strftime("%Y-%m-%dT%H:%M:%S+05:30")
