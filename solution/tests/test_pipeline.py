import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from au_pipeline.db import connect

from tests.constants import TEST_PII_KEY

REPO = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).parent / "fixtures"

LEAD_REPORT = """\
workflow_id,external_ref,batch_id,phone_masked,bucket,total_calls,connected_calls,last_call_at,current_disposition,ever_success
motor_renewal,MOT/1B,batch_001,+91******3210,completed,2,1,2026-10-04T02:00:00+05:30,not_interested,false
motor_renewal,MOT/4,batch_001,+91******6789,in_progress_telephony_failed,1,0,2026-10-05T09:00:00+05:30,,false
motor_renewal,MOT/5B,batch_001,+91******6655,yet_to_reach_out,0,0,,,false
motor_renewal,MOT/6,batch_002,+91******1111,in_progress_dialed,1,1,2026-10-05T11:00:00+05:30,payment_link_sent,false
motor_renewal,MOT/7,batch_003,+91******0001,completed,1,1,2026-10-06T10:00:00+05:30,renewed,true
"""

BATCH_SUMMARY = """\
workflow_id,batch_id,total_leads,yet_to_reach_out,in_progress_telephony_failed,in_progress_dialed,completed
motor_renewal,batch_001,3,1,1,0,1
motor_renewal,batch_002,1,0,0,1,0
motor_renewal,batch_003,1,0,0,0,1
"""

DAILY = """\
workflow_id,date,total_calls,connected_calls,connectivity_rate,success_leads
motor_renewal,2026-10-03,1,1,1.0000,0
motor_renewal,2026-10-04,1,0,0.0000,0
motor_renewal,2026-10-05,2,1,0.5000,0
motor_renewal,2026-10-06,1,1,1.0000,1
"""

REJECTIONS = """\
workflow_id,batch_id,row_number,reason
motor_renewal,batch_001,2,missing_required:customer_name
motor_renewal,batch_001,3,invalid_phone
"""

QUALITY = {
    "lead_rows_total": 9,
    "lead_rows_rejected": 2,
    "rejections_by_reason": {
        "invalid_phone": 1,
        "missing_required:customer_name": 1,
    },
    "duplicate_lead_rows_merged": 2,
    "field_warnings": 3,
    "events_total": 22,
    "malformed_events": 2,
    "duplicate_events_dropped": 2,
    "orphan_events": 1,
    "unknown_disposition_events": 1,
}


def _env(url: str) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = url
    env["AU_PII_KEY"] = TEST_PII_KEY
    env["PYTHONPATH"] = str(REPO / "solution")
    env.pop("AU_ALLOW_RESET", None)
    return env


def _run(args: list[str], env: dict[str, str]) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "au_pipeline", *args],
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise AssertionError(f"{args}\n{result.stdout}\n{result.stderr}")


def test_fixture_drop_matches_the_contract(database_url, tmp_path):
    env = _env(database_url)
    _run(["reset"], env)
    _run(["ingest", str(FIXTURES / "drop_1")], env)

    early = tmp_path / "early"
    _run(["report", "--out", str(early)], env)
    early_report = (early / "lead_report.csv").read_text(encoding="utf-8")
    assert "MOT/7" not in early_report
    assert "2026-10-06" not in (early / "daily_overview.csv").read_text(encoding="utf-8")
    early_quality = json.loads((early / "data_quality.json").read_text(encoding="utf-8"))
    assert early_quality["orphan_events"] == 5
    assert early_quality["unknown_disposition_events"] == 0
    assert early_quality["duplicate_lead_rows_merged"] == 2

    _run(["ingest", str(FIXTURES / "drop_2")], env)
    out = tmp_path / "reports"
    _run(["report", "--out", str(out)], env)

    assert (out / "lead_report.csv").read_text(encoding="utf-8") == LEAD_REPORT
    assert (out / "batch_summary.csv").read_text(encoding="utf-8") == BATCH_SUMMARY
    assert (out / "daily_overview.csv").read_text(encoding="utf-8") == DAILY
    assert (out / "rejections.csv").read_text(encoding="utf-8") == REJECTIONS
    quality = json.loads((out / "data_quality.json").read_text(encoding="utf-8"))
    assert quality == QUALITY

    _run(["ingest", str(FIXTURES / "drop_1")], env)
    _run(["ingest", str(FIXTURES / "drop_2")], env)
    again = tmp_path / "again"
    _run(["report", "--out", str(again)], env)
    for name in (
        "lead_report.csv",
        "batch_summary.csv",
        "daily_overview.csv",
        "rejections.csv",
        "data_quality.json",
    ):
        assert (again / name).read_bytes() == (out / name).read_bytes()

    with connect(database_url) as connection:
        asha = connection.execute(
            """
            SELECT batch_id, attributes
            FROM leads WHERE external_ref = 'MOT/1B'
            """
        ).fetchone()
        assert asha["batch_id"] == "batch_001"
        assert asha["attributes"]["preferred_language"] == "ta"
        assert Decimal(asha["attributes"]["premium_amount"]) == Decimal("2000.00")
        assert asha["attributes"]["policy_expiry"] == "2026-12-15"
        assert isinstance(asha["attributes"]["customer_name"], dict)

        esha = connection.execute(
            """
            SELECT attributes FROM leads WHERE external_ref = 'MOT/5B'
            """
        ).fetchone()
        assert Decimal(esha["attributes"]["premium_amount"]) == Decimal("99.00")
        assert esha["attributes"]["policy_expiry"] == "2026-01-05"
        assert esha["attributes"]["preferred_language"] == "hi"

        diya = connection.execute(
            """
            SELECT attributes_resolved->>'preferred_language' AS language
            FROM leads_resolved WHERE external_ref = 'MOT/4'
            """
        ).fetchone()
        assert diya["language"] == "en"

        farhan = connection.execute(
            "SELECT attributes FROM leads WHERE external_ref = 'MOT/6'"
        ).fetchone()
        assert Decimal(farhan["attributes"]["premium_amount"]) == Decimal("2000.00")

        statuses = {
            row["event_id"]: row["link_status"]
            for row in connection.execute(
                """
                SELECT event.event_id, link.link_status
                FROM events AS event
                JOIN event_links AS link ON link.event_id = event.event_id
                WHERE event.event_id IN ('evt_g_bad', 'evt_z_init', 'evt_a_over', 'evt_f_over', 'evt_g_set')
                """
            ).fetchall()
        }
        assert statuses == {
            "evt_g_bad": "ignored_unknown_disposition",
            "evt_z_init": "orphan",
            "evt_a_over": "linked",
            "evt_f_over": "linked",
            "evt_g_set": "linked",
        }

        text_bits = []
        for row in connection.execute(
            """
            SELECT external_ref, batch_id, attributes::text
            FROM leads
            """
        ).fetchall():
            text_bits.extend(str(value) for value in row.values())
        for row in connection.execute("SELECT event_id, event_type, payload::text FROM events").fetchall():
            text_bits.extend(str(value) for value in row.values())
        blob = "\n".join(text_bits)
        for secret in ("Asha", "Diya", "Esha", "Farhan", "Geeta", "9876543210"):
            assert secret not in blob
