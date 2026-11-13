"""Database behaviour. These tests fail if PostgreSQL is not reachable."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import psycopg
import pytest

from au_pipeline.errors import PipelineError
from au_pipeline.ingest import ingest_drop
from au_pipeline.pii import decrypt, derived_keys
from au_pipeline.reports import write_reports
from au_pipeline.schemas import Field, WorkflowSchema, load_schemas
from tests.constants import TEST_KEY_BYTES, TEST_PII_KEY

REPO = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).parent / "fixtures"
AES_KEY, MAC_KEY = derived_keys(TEST_KEY_BYTES)


def _lead(name: str, phone: str, ref: str) -> str:
    return f"customer_name,phone,policy_number\n{name},{phone},{ref}\n"


def _event(event_id, event_type, call_id, when, phone="9876543210", **payload):
    body = {
        "event_id": event_id,
        "event_type": event_type,
        "workflow_id": "motor_renewal",
        "call_id": call_id,
        "occurred_at": when,
        "payload": payload,
    }
    if phone is not None:
        body["phone"] = phone
    return json.dumps(body)


def _drop(root: Path, name: str, csv_text: str, events: list[str] | None, batch: str = "batch_001"):
    drop = root / name
    folder = drop / "leads" / "motor_renewal"
    folder.mkdir(parents=True)
    (folder / f"{batch}.csv").write_text(csv_text, encoding="utf-8", newline="\n")
    if events is not None:
        (drop / "events").mkdir()
        (drop / "events" / "events_0001.jsonl").write_text("\n".join(events) + "\n", encoding="utf-8")
    return drop


def _ingest(connection, drop: Path, schemas=None) -> None:
    ingest_drop(connection, drop, AES_KEY, MAC_KEY, schemas or load_schemas())


def _bucket(connection, ref: str):
    row = connection.execute(
        "SELECT bucket, current_disposition, ever_success FROM lead_status WHERE external_ref = %s",
        (ref,),
    ).fetchone()
    assert row is not None
    return row


def _disposition(connection, call_id: str):
    return connection.execute(
        """
        SELECT disposition_code, is_terminal, is_success, call_date
        FROM call_facts WHERE call_id = %s
        """,
        (call_id,),
    ).fetchone()


def test_multiple_overrides_use_the_latest(ready, tmp_path):
    csv_text = _lead("Asha", "9876543210", "MOT/A")
    events = [
        _event("i", "call.initiated", "c1", "2026-10-05 10:00:00"),
        _event("s", "disposition.set", "c1", "2026-10-05T10:05:00+05:30", disposition_code="renewed"),
        _event(
            "o1",
            "disposition.override",
            "c1",
            "2026-10-05T11:00:00+05:30",
            phone=None,
            disposition_code="callback_requested",
        ),
        _event(
            "o2",
            "disposition.override",
            "c1",
            "2026-10-05T12:00:00+05:30",
            phone=None,
            disposition_code="not_interested",
        ),
    ]
    _ingest(ready, _drop(tmp_path, "drop_a", csv_text, events))
    fact = _disposition(ready, "c1")
    assert fact["disposition_code"] == "not_interested"
    assert fact["is_terminal"] is True
    assert fact["is_success"] is False


def test_unknown_override_falls_back_to_the_latest_valid_set(ready, tmp_path):
    csv_text = _lead("Asha", "9876543210", "MOT/A")
    events = [
        _event("i", "call.initiated", "c1", "2026-10-05 10:00:00"),
        _event("s", "disposition.set", "c1", "2026-10-05T10:05:00+05:30", disposition_code="renewed"),
        _event(
            "o",
            "disposition.override",
            "c1",
            "2026-10-05T12:00:00+05:30",
            phone=None,
            disposition_code="NOT_A_CODE",
        ),
    ]
    _ingest(ready, _drop(tmp_path, "drop_a", csv_text, events))
    fact = _disposition(ready, "c1")
    assert fact["disposition_code"] == "renewed"
    assert fact["is_success"] is True
    status = ready.execute(
        "SELECT link_status FROM event_links WHERE event_id = 'o'"
    ).fetchone()
    assert status["link_status"] == "ignored_unknown_disposition"


def test_latest_unknown_set_does_not_erase_the_previous_valid_set(ready, tmp_path):
    csv_text = _lead("Asha", "9876543210", "MOT/A")
    events = [
        _event("i", "call.initiated", "c1", "2026-10-05 10:00:00"),
        _event("s1", "disposition.set", "c1", "2026-10-05T10:05:00+05:30", disposition_code="payment_link_sent"),
        _event("s2", "disposition.set", "c1", "2026-10-05T10:06:00+05:30", disposition_code="RENEWD"),
    ]
    _ingest(ready, _drop(tmp_path, "drop_a", csv_text, events))
    fact = _disposition(ready, "c1")
    assert fact["disposition_code"] == "payment_link_sent"
    assert fact["is_success"] is False


def test_orphan_event_links_when_the_lead_arrives(ready, tmp_path):
    events = [_event("i", "call.initiated", "c1", "2026-10-05 10:00:00")]
    _ingest(ready, _drop(tmp_path, "drop_1", _lead("nobody", "9000000000", "MOT/X"), events))
    orphan = ready.execute("SELECT link_status FROM event_links WHERE event_id = 'i'").fetchone()
    assert orphan["link_status"] == "orphan"
    _ingest(ready, _drop(tmp_path, "drop_2", _lead("Asha", "9876543210", "MOT/A"), []))
    linked = ready.execute(
        "SELECT link_status, lead_id FROM event_links WHERE event_id = 'i'"
    ).fetchone()
    assert linked["link_status"] == "linked"
    assert linked["lead_id"] is not None


def test_unlinked_override_stays_an_orphan(ready, tmp_path):
    events = [
        _event(
            "o",
            "disposition.override",
            "missing_call",
            "2026-10-05T12:00:00+05:30",
            phone=None,
            disposition_code="renewed",
        )
    ]
    _ingest(ready, _drop(tmp_path, "drop_a", _lead("Asha", "9876543210", "MOT/A"), events))
    row = ready.execute("SELECT link_status, lead_id FROM event_links WHERE event_id = 'o'").fetchone()
    assert row["link_status"] == "orphan"
    assert row["lead_id"] is None
    quality = ready.execute(
        """
        SELECT COUNT(*) FILTER (WHERE link_status = 'orphan') AS orphans,
               COUNT(*) FILTER (WHERE link_status = 'ignored_unknown_disposition') AS unknown
        FROM event_links
        """
    ).fetchone()
    assert quality["orphans"] == 1
    assert quality["unknown"] == 0


def test_completed_beats_telephony_failure(ready, tmp_path):
    csv_text = _lead("Asha", "9876543210", "MOT/A")
    events = [
        _event("i1", "call.initiated", "failed", "2026-10-05 09:00:00"),
        _event("f1", "call.telephony_failed", "failed", "2026-10-05T09:00:05+05:30"),
        _event("i2", "call.initiated", "done", "2026-10-05 11:00:00"),
        _event("c2", "call.connected", "done", "2026-10-05T11:00:10+05:30"),
        _event("s2", "disposition.set", "done", "2026-10-05T11:05:00+05:30", disposition_code="renewed"),
    ]
    _ingest(ready, _drop(tmp_path, "drop_a", csv_text, events))
    row = _bucket(ready, "MOT/A")
    assert row["bucket"] == "completed"
    assert row["ever_success"] is True


def test_mixed_failed_and_normal_calls_are_in_progress(ready, tmp_path):
    csv_text = _lead("Asha", "9876543210", "MOT/A")
    events = [
        _event("i1", "call.initiated", "failed", "2026-10-05 09:00:00"),
        _event("f1", "call.telephony_failed", "failed", "2026-10-05T09:00:05+05:30"),
        _event("i2", "call.initiated", "rang", "2026-10-05 11:00:00"),
        _event("c2", "call.connected", "rang", "2026-10-05T11:00:10+05:30"),
    ]
    _ingest(ready, _drop(tmp_path, "drop_a", csv_text, events))
    assert _bucket(ready, "MOT/A")["bucket"] == "in_progress_dialed"


def test_success_leads_count_distinct_leads_on_a_date(ready, tmp_path):
    csv_text = (
        "customer_name,phone,policy_number\n"
        "Asha,9876543210,MOT/A\n"
        "Diya,9123456789,MOT/B\n"
    )
    events = [
        _event("a1", "call.initiated", "a1", "2026-10-05 10:00:00", phone="9876543210"),
        _event("a1s", "disposition.set", "a1", "2026-10-05T10:05:00+05:30", phone="9876543210", disposition_code="renewed"),
        _event("a2", "call.initiated", "a2", "2026-10-05 15:00:00", phone="9876543210"),
        _event("a2s", "disposition.set", "a2", "2026-10-05T15:05:00+05:30", phone="9876543210", disposition_code="renewed"),
        _event("b1", "call.initiated", "b1", "2026-10-05 12:00:00", phone="9123456789"),
        _event("b1s", "disposition.set", "b1", "2026-10-05T12:05:00+05:30", phone="9123456789", disposition_code="renewed"),
    ]
    _ingest(ready, _drop(tmp_path, "drop_a", csv_text, events))
    row = ready.execute(
        """
        SELECT COUNT(*) AS total_calls,
               COUNT(DISTINCT lead_id) FILTER (WHERE COALESCE(is_success, FALSE)) AS success_leads
        FROM call_facts
        WHERE call_date = %s
        """,
        (date(2026, 10, 5),),
    ).fetchone()
    assert row["total_calls"] == 3
    assert row["success_leads"] == 2


def test_lead_report_sort_uses_c_collation(ready, tmp_path):
    csv_text = (
        "customer_name,phone,policy_number\n"
        "Asha,9876543210,a1\n"
        "Diya,9123456789,A/1\n"
        "Esha,9234567890,B\n"
        "Farhan,9345678901,A-2\n"
    )
    _ingest(ready, _drop(tmp_path, "drop_a", csv_text, []))
    out = tmp_path / "reports"
    write_reports(ready, out, AES_KEY)
    lines = (out / "lead_report.csv").read_text(encoding="utf-8").splitlines()
    refs = [line.split(",")[1] for line in lines[1:]]
    assert refs == ["A-2", "A/1", "B", "a1"]
    blob = (out / "lead_report.csv").read_text(encoding="utf-8")
    for secret in ("Asha", "Diya", "Esha", "Farhan", "9876543210"):
        assert secret not in blob


def test_offset_timestamp_lands_on_the_ist_date(ready, tmp_path):
    csv_text = _lead("Asha", "9876543210", "MOT/A")
    events = [
        _event("i", "call.initiated", "c1", "2026-10-03T20:30:00-04:00"),
        _event("f", "call.initiated", "c2", "2026-10-05T13:20:21.500Z", phone="9876543210"),
    ]
    _ingest(ready, _drop(tmp_path, "drop_a", csv_text, events))
    dates = {
        row["call_id"]: row["call_date"]
        for row in ready.execute("SELECT call_id, call_date FROM call_facts").fetchall()
    }
    assert dates["c1"] == date(2026, 10, 4)
    assert dates["c2"] == date(2026, 10, 5)


def test_changed_file_content_is_rejected(ready, tmp_path):
    drop = _drop(tmp_path, "drop_1", _lead("Asha", "9876543210", "MOT/A"), [])
    _ingest(ready, drop)
    path = drop / "leads" / "motor_renewal" / "batch_001.csv"
    path.write_text(_lead("Diya", "9876543210", "MOT/A"), encoding="utf-8", newline="\n")
    with pytest.raises(PipelineError, match="different contents"):
        _ingest(ready, drop)
    count = ready.execute("SELECT COUNT(*) AS n FROM leads").fetchone()["n"]
    assert count == 1
    stored = ready.execute("SELECT external_ref FROM leads").fetchone()["external_ref"]
    assert stored == "MOT/A"
    ready.rollback()


def test_key_mismatch_cli(ready, database_url, tmp_path, monkeypatch):
    _ingest(ready, _drop(tmp_path, "drop_1", _lead("Asha", "9876543210", "MOT/A"), []))
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("AU_PII_KEY", "cd" * 32)
    from au_pipeline.cli import main

    out = tmp_path / "reports"
    assert main(["ingest", str(tmp_path / "drop_1")]) == 1
    assert main(["report", "--out", str(out)]) == 1
    assert not out.exists()
    assert ready.execute("SELECT COUNT(*) AS n FROM leads").fetchone()["n"] == 1


def test_append_only_ledger_rejects_update_and_delete(ready, tmp_path):
    _ingest(
        ready,
        _drop(
            tmp_path,
            "drop_a",
            _lead("Asha", "9876543210", "MOT/A"),
            [_event("i", "call.initiated", "c1", "2026-10-05 10:00:00")],
        ),
    )
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        ready.execute("UPDATE events SET event_type = 'call.ended' WHERE event_id = 'i'")
    ready.rollback()
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        ready.execute("DELETE FROM events WHERE event_id = 'i'")
    ready.rollback()
    ready.execute(
        "UPDATE event_links SET link_status = 'orphan', lead_id = NULL WHERE event_id = 'i'"
    )
    ready.commit()
    row = ready.execute("SELECT event_type FROM events WHERE event_id = 'i'").fetchone()
    assert row["event_type"] == "call.initiated"


def test_same_filename_in_two_drops_does_not_collide(ready, tmp_path):
    first_csv = (
        "customer_name,phone,policy_number\n"
        ",9876543210,MOT/A\n"
        "Asha,9876543210,MOT/A\n"
    )
    second_csv = (
        "customer_name,phone,policy_number\n"
        ",9123456789,MOT/B\n"
        "Diya,9123456789,MOT/B\n"
    )
    first = _drop(tmp_path, "drop_1", first_csv, [], batch="batch_001")
    second = _drop(tmp_path, "drop_2", second_csv, [], batch="batch_001")
    _ingest(ready, first)
    _ingest(ready, second)
    rows = ready.execute(
        """
        SELECT batch_id, total_leads
        FROM (
            SELECT batch.batch_id, COUNT(lead.lead_id) AS total_leads, batch.file_key
            FROM batches AS batch
            LEFT JOIN leads AS lead ON lead.source_file_key = batch.file_key
            GROUP BY batch.file_key, batch.batch_id
        ) AS counted
        ORDER BY file_key
        """
    ).fetchall()
    assert [row["batch_id"] for row in rows] == ["batch_001", "batch_001"]
    assert [row["total_leads"] for row in rows] == [1, 1]
    reasons = ready.execute(
        "SELECT reason, row_number FROM rejections ORDER BY file_key"
    ).fetchall()
    assert len(reasons) == 2
    assert {row["reason"] for row in reasons} == {"missing_required:customer_name"}
    assert [row["row_number"] for row in reasons] == [1, 1]


def test_new_schema_field_is_kept_and_pii_is_encrypted(ready, tmp_path):
    base = load_schemas()["motor_renewal"]
    extra = base.fields + (
        Field("city", "string", False, False, (), frozenset(), None, False),
        Field("email", "string", False, True, (), frozenset(), None, False),
    )
    schemas = dict(load_schemas())
    schemas["motor_renewal"] = WorkflowSchema(base.workflow_id, extra)
    csv_text = (
        "customer_name,phone,policy_number,city,email\n"
        "Asha,9876543210,MOT/A,Pune,asha@example.com\n"
        "Asha,9876543210,MOT/A2,Mumbai,\n"
    )
    _ingest(ready, _drop(tmp_path, "drop_a", csv_text, []), schemas)
    row = ready.execute("SELECT external_ref, attributes FROM leads").fetchone()
    assert row["external_ref"] == "MOT/A2"
    assert row["attributes"]["city"] == "Mumbai"
    email = row["attributes"]["email"]
    assert isinstance(email, dict)
    assert decrypt(AES_KEY, base64.b64decode(email["n"]), base64.b64decode(email["c"])) == "asha@example.com"
    blob = json.dumps(row["attributes"])
    assert "Asha" not in blob
    assert "asha@example.com" not in blob


def test_empty_directory_is_an_error(database_url, ready, tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("AU_PII_KEY", TEST_PII_KEY)
    from au_pipeline.cli import main

    empty = tmp_path / "empty"
    empty.mkdir()
    assert main(["ingest", str(empty)]) == 1


def test_ingesting_both_drops_together_matches_one_then_the_other(database_url, tmp_path):
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["AU_PII_KEY"] = TEST_PII_KEY
    env["PYTHONPATH"] = str(REPO / "solution")
    env.pop("AU_ALLOW_RESET", None)

    def run(args):
        result = subprocess.run(
            [sys.executable, "-m", "au_pipeline", *args],
            cwd=REPO,
            env=env,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            raise AssertionError(f"{result.stdout}\n{result.stderr}")

    separate = tmp_path / "separate"
    together = tmp_path / "together"
    run(["reset"])
    run(["ingest", str(FIXTURES / "drop_1")])
    run(["ingest", str(FIXTURES / "drop_2")])
    run(["report", "--out", str(separate)])
    run(["reset"])
    run(["ingest", str(FIXTURES / "drop_1"), str(FIXTURES / "drop_2")])
    run(["report", "--out", str(together)])
    for name in (
        "lead_report.csv",
        "batch_summary.csv",
        "daily_overview.csv",
        "rejections.csv",
        "data_quality.json",
    ):
        assert (together / name).read_bytes() == (separate / name).read_bytes()
