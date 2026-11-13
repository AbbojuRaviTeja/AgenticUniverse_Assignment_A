from datetime import date, datetime, timezone
from decimal import Decimal

from au_pipeline.normalize import (
    IST,
    canonical_phone,
    normalize_header,
    parse_date,
    parse_decimal,
    parse_occurred_at,
)


def test_phone_formats():
    assert canonical_phone("9876543210") == "+919876543210"
    assert canonical_phone("+91 98765 43210") == "+919876543210"
    assert canonical_phone("(+91) 9876543210") == "+919876543210"
    assert canonical_phone("919876543210") == "+919876543210"
    assert canonical_phone("91-98765-43210") == "+919876543210"
    assert canonical_phone("09876543210") == "+919876543210"
    assert canonical_phone("12345") is None
    assert canonical_phone("5123456789") is None
    assert canonical_phone("022-2345-6789") is None
    assert canonical_phone("+1 415 555 0134") is None
    assert canonical_phone("N/A") is None


def test_dates_are_day_first_and_reject_impossible_days():
    assert parse_date("2026-12-01") == date(2026, 12, 1)
    assert parse_date("15/12/2026") == date(2026, 12, 15)
    assert parse_date("15-12-2026") == date(2026, 12, 15)
    assert parse_date("05-Jan-2026") == date(2026, 1, 5)
    assert parse_date("01-dec-2026") == date(2026, 12, 1)
    assert parse_date("31/02/2026") is None
    assert parse_date("2026-13-01") is None
    assert parse_date("29/02/2024") == date(2024, 2, 29)
    assert parse_date("29/02/2026") is None
    assert parse_date("soon") is None


def test_decimals_and_headers():
    assert parse_decimal("1000") == Decimal("1000.00")
    assert parse_decimal("₹1,234.50") == Decimal("1234.50")
    assert parse_decimal("Rs. 2000") == Decimal("2000.00")
    assert parse_decimal("INR 10.5") == Decimal("10.50")
    assert parse_decimal("1.005") == Decimal("1.01")
    assert parse_decimal("N/A") is None
    assert parse_decimal("-5") is None
    assert normalize_header(" Policy_No ") == "policy no"
    assert normalize_header("Insured   Name") == "insured name"


def test_naive_timestamp_is_ist():
    naive = parse_occurred_at("2026-10-03 10:00:00")
    assert naive == datetime(2026, 10, 3, 10, 0, tzinfo=IST)
    utc = parse_occurred_at("2026-10-03T20:30:00Z")
    assert utc == datetime(2026, 10, 3, 20, 30, tzinfo=timezone.utc)
    assert utc.astimezone(IST) == datetime(2026, 10, 4, 2, 0, tzinfo=IST)
    offset = parse_occurred_at("2026-10-05T09:00:00+05:30")
    assert offset == datetime(2026, 10, 5, 9, 0, tzinfo=IST)


def test_timestamp_offsets_and_fractional_seconds():
    fractional = parse_occurred_at("2026-10-05T13:20:21.5Z")
    assert fractional == datetime(2026, 10, 5, 13, 20, 21, 500000, tzinfo=timezone.utc)
    micros = parse_occurred_at("2026-10-05T13:20:21.123456+05:30")
    assert micros == datetime(2026, 10, 5, 13, 20, 21, 123456, tzinfo=IST)
    western = parse_occurred_at("2026-10-03T20:30:00-04:00")
    assert western is not None
    assert western.astimezone(IST) == datetime(2026, 10, 4, 6, 0, tzinfo=IST)
    spaced = parse_occurred_at("2026-10-03 10:01:00.25")
    assert spaced == datetime(2026, 10, 3, 10, 1, 0, 250000, tzinfo=IST)
    assert parse_occurred_at("") is None
    assert parse_occurred_at("2026-10-05") is None
