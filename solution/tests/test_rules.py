"""Rules that do not need PostgreSQL."""

import base64
import json
from decimal import Decimal

import pytest

from au_pipeline.db import assert_reset_allowed, split_sql
from au_pipeline.errors import PipelineError
from au_pipeline.ingest import classify_events, column_map, file_decision, lead_row, parse_lead
from au_pipeline.pii import decode_key, decrypt, derived_keys, encrypt
from au_pipeline.reports import _rate
from au_pipeline.schemas import Field, WorkflowSchema, load_schemas

KEY = bytes(range(32))
AES_KEY, MAC_KEY = derived_keys(KEY)


def test_connectivity_rounding_is_half_up():
    assert _rate(0, 0) == "0.0000"
    assert _rate(9, 16) == "0.5625"
    assert _rate(1, 6) == "0.1667"
    assert _rate(1, 160) == "0.0063"
    assert _rate(2, 160) == "0.0125"


def test_rejection_order_follows_the_schema():
    schema = load_schemas()["motor_renewal"]
    columns = column_map(["customer_name", "phone", "policy_number"])
    missing_name = parse_lead(
        schema,
        {"customer_name": " ", "phone": "12345", "policy_number": "MOT/1"},
        columns,
    )
    assert missing_name.reason == "missing_required:customer_name"
    assert missing_name.warnings == 0
    bad_phone = parse_lead(
        schema,
        {"customer_name": "Asha", "phone": "12345", "policy_number": ""},
        columns,
    )
    assert bad_phone.reason == "invalid_phone"


def test_duplicate_event_id_is_checked_before_the_timestamp():
    valid = json.dumps(
        {
            "event_id": "e1",
            "event_type": "call.initiated",
            "workflow_id": "motor_renewal",
            "call_id": "c1",
            "occurred_at": "2026-10-03 10:00:00",
            "phone": "9876543210",
            "payload": {},
        }
    )
    broken = '{"event_id": "e1", "occurred_at": "not-a-timestamp"}'
    total, malformed, duplicates, fresh = classify_events([valid, broken], MAC_KEY, set())
    assert (total, malformed, duplicates, len(fresh)) == (2, 0, 1, 1)

    total, malformed, duplicates, fresh = classify_events([broken], MAC_KEY, {"e1"})
    assert (total, malformed, duplicates, len(fresh)) == (1, 0, 1, 0)

    missing = '{"event_id": "e2"}'
    total, malformed, duplicates, fresh = classify_events([missing, valid.replace("e1", "e2")], MAC_KEY, set())
    assert malformed == 1
    assert duplicates == 0
    assert len(fresh) == 1
    assert fresh[0][0] == "e2"


def test_missing_timestamp_is_malformed_and_does_not_reserve_the_id():
    first = '{"event_id": "e9", "occurred_at": ""}'
    second = json.dumps(
        {
            "event_id": "e9",
            "event_type": "call.initiated",
            "occurred_at": "2026-10-05T13:20:21.5Z",
            "phone": "9876543210",
            "payload": {},
        }
    )
    total, malformed, duplicates, fresh = classify_events([first, second], MAC_KEY, set())
    assert total == 2
    assert malformed == 1
    assert duplicates == 0
    assert len(fresh) == 1
    assert fresh[0][4].microsecond == 500000


def test_key_must_be_32_bytes_and_is_not_invented():
    assert decode_key(KEY.hex()) == KEY
    encoded = __import__("base64").b64encode(KEY).decode("ascii")
    assert decode_key(encoded) == KEY
    with pytest.raises(PipelineError):
        decode_key("replace-with-a-long-random-secret")
    with pytest.raises(PipelineError):
        decode_key("abcd")


def test_changed_file_hash_is_rejected_and_same_hash_skips():
    digest = "a" * 64
    assert file_decision(None, digest) == "ingest"
    assert file_decision(digest, digest) == "skip"
    with pytest.raises(PipelineError, match="different contents"):
        file_decision("b" * 64, digest)


def test_reset_refuses_an_unexpected_database(monkeypatch):
    monkeypatch.delenv("AU_ALLOW_RESET", raising=False)
    assert_reset_allowed("au_assignment")
    assert_reset_allowed("au_assignment_test")
    with pytest.raises(PipelineError, match="Refusing to reset"):
        assert_reset_allowed("postgres")
    monkeypatch.setenv("AU_ALLOW_RESET", "1")
    assert_reset_allowed("postgres")


def test_sql_splitter_keeps_function_bodies():
    script = """
CREATE TABLE t (id INT);
CREATE FUNCTION f() RETURNS trigger AS $fn$
BEGIN
    RAISE EXCEPTION 'no; updates';
END;
$fn$ LANGUAGE plpgsql;
CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION f();
"""
    parts = split_sql(script)
    assert len(parts) == 3
    assert "RAISE EXCEPTION 'no; updates';" in parts[1]
    assert parts[1].strip().endswith("LANGUAGE plpgsql")


def test_pii_fields_follow_the_schema_flag():
    schema = WorkflowSchema(
        "demo",
        (
            Field("customer_name", "string", True, True, (), frozenset(), None, False),
            Field("phone", "phone", True, True, (), frozenset(), None, False),
            Field("policy_number", "string", True, False, (), frozenset(), None, True),
            Field("city", "string", False, False, (), frozenset(), None, False),
            Field("email", "string", False, True, (), frozenset(), None, False),
            Field("premium_amount", "decimal", False, False, (), frozenset(), None, False),
        ),
    )
    built = lead_row(
        schema,
        {
            "customer_name": "Asha",
            "phone": "+919876543210",
            "policy_number": "MOT/1",
            "city": "Pune",
            "email": "asha@example.com",
            "premium_amount": Decimal("10.50"),
        },
        AES_KEY,
        MAC_KEY,
    )
    assert built["external_ref"] == "MOT/1"
    assert built["attributes"]["city"] == "Pune"
    assert built["attributes"]["premium_amount"] == "10.50"
    assert "Asha" not in json.dumps(built["attributes"]["customer_name"])
    email = built["attributes"]["email"]
    assert decrypt(AES_KEY, base64.b64decode(email["n"]), base64.b64decode(email["c"])) == "asha@example.com"
    nonce, ciphertext = encrypt(AES_KEY, "+919876543210")
    assert decrypt(AES_KEY, nonce, ciphertext) == "+919876543210"
    assert built["phone_hmac"]
    assert b"9876543210" not in built["phone_ciphertext"]
