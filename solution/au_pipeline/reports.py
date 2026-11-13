"""The five report files. Aggregation stays in SQL; Python only formats."""

from __future__ import annotations

import csv
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from au_pipeline.normalize import format_ist
from au_pipeline.pii import decrypt, mask_phone


def write_reports(connection, out_dir: Path, aes_key: bytes) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_batch_summary(connection, out_dir / "batch_summary.csv")
    _write_daily_overview(connection, out_dir / "daily_overview.csv")
    _write_lead_report(connection, out_dir / "lead_report.csv", aes_key)
    _write_rejections(connection, out_dir / "rejections.csv")
    _write_quality(connection, out_dir / "data_quality.json")


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def _write_batch_summary(connection, path: Path) -> None:
    rows = connection.execute(
        """
        SELECT
            batch.workflow_id,
            batch.batch_id,
            COUNT(lead.lead_id) AS total_leads,
            COUNT(lead.lead_id) FILTER (WHERE status.bucket = 'yet_to_reach_out') AS yet_to_reach_out,
            COUNT(lead.lead_id) FILTER (WHERE status.bucket = 'in_progress_telephony_failed')
                AS in_progress_telephony_failed,
            COUNT(lead.lead_id) FILTER (WHERE status.bucket = 'in_progress_dialed') AS in_progress_dialed,
            COUNT(lead.lead_id) FILTER (WHERE status.bucket = 'completed') AS completed
        FROM batches AS batch
        LEFT JOIN leads AS lead ON lead.source_file_key = batch.file_key
        LEFT JOIN lead_status AS status ON status.lead_id = lead.lead_id
        GROUP BY batch.file_key, batch.workflow_id, batch.batch_id
        ORDER BY batch.workflow_id COLLATE "C", batch.batch_id COLLATE "C", batch.file_key COLLATE "C"
        """
    ).fetchall()
    _write_csv(
        path,
        [
            "workflow_id",
            "batch_id",
            "total_leads",
            "yet_to_reach_out",
            "in_progress_telephony_failed",
            "in_progress_dialed",
            "completed",
        ],
        [
            [
                row["workflow_id"],
                row["batch_id"],
                row["total_leads"],
                row["yet_to_reach_out"],
                row["in_progress_telephony_failed"],
                row["in_progress_dialed"],
                row["completed"],
            ]
            for row in rows
        ],
    )


def _rate(connected: int, total: int) -> str:
    if total == 0:
        return "0.0000"
    quantized = (Decimal(connected) / Decimal(total)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return f"{quantized:.4f}"


def _write_daily_overview(connection, path: Path) -> None:
    rows = connection.execute(
        """
        SELECT
            workflow_id,
            call_date,
            COUNT(*) AS total_calls,
            COUNT(*) FILTER (WHERE connected) AS connected_calls,
            COUNT(DISTINCT lead_id) FILTER (WHERE COALESCE(is_success, FALSE)) AS success_leads
        FROM call_facts
        GROUP BY workflow_id, call_date
        ORDER BY workflow_id COLLATE "C", call_date
        """
    ).fetchall()
    _write_csv(
        path,
        ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"],
        [
            [
                row["workflow_id"],
                row["call_date"].isoformat(),
                row["total_calls"],
                row["connected_calls"],
                _rate(row["connected_calls"], row["total_calls"]),
                row["success_leads"],
            ]
            for row in rows
        ],
    )


def _write_lead_report(connection, path: Path, aes_key: bytes) -> None:
    rows = connection.execute(
        """
        SELECT
            workflow_id, external_ref, batch_id, phone_ciphertext, phone_nonce,
            bucket, total_calls, connected_calls, last_call_at,
            current_disposition, ever_success
        FROM lead_status
        ORDER BY workflow_id COLLATE "C", external_ref COLLATE "C", lead_id
        """
    ).fetchall()
    written = []
    for row in rows:
        canonical = decrypt(aes_key, row["phone_nonce"], row["phone_ciphertext"])
        written.append(
            [
                row["workflow_id"],
                row["external_ref"],
                row["batch_id"],
                mask_phone(canonical),
                row["bucket"],
                row["total_calls"],
                row["connected_calls"],
                "" if row["last_call_at"] is None else format_ist(row["last_call_at"]),
                row["current_disposition"] or "",
                "true" if row["ever_success"] else "false",
            ]
        )
    _write_csv(
        path,
        [
            "workflow_id",
            "external_ref",
            "batch_id",
            "phone_masked",
            "bucket",
            "total_calls",
            "connected_calls",
            "last_call_at",
            "current_disposition",
            "ever_success",
        ],
        written,
    )


def _write_rejections(connection, path: Path) -> None:
    rows = connection.execute(
        """
        SELECT workflow_id, batch_id, row_number, reason
        FROM rejections
        ORDER BY workflow_id COLLATE "C", batch_id COLLATE "C", row_number, file_key COLLATE "C"
        """
    ).fetchall()
    _write_csv(
        path,
        ["workflow_id", "batch_id", "row_number", "reason"],
        [[row["workflow_id"], row["batch_id"], row["row_number"], row["reason"]] for row in rows],
    )


def _write_quality(connection, path: Path) -> None:
    totals = connection.execute("SELECT * FROM quality_totals WHERE id = 1").fetchone()
    reasons = connection.execute(
        "SELECT reason, COUNT(*) AS n FROM rejections GROUP BY reason"
    ).fetchall()
    statuses = {
        row["link_status"]: row["n"]
        for row in connection.execute(
            "SELECT link_status, COUNT(*) AS n FROM event_links GROUP BY link_status"
        ).fetchall()
    }
    document = {
        "lead_rows_total": totals["lead_rows_total"],
        "lead_rows_rejected": totals["lead_rows_rejected"],
        "rejections_by_reason": dict(sorted((row["reason"], row["n"]) for row in reasons)),
        "duplicate_lead_rows_merged": totals["duplicate_lead_rows_merged"],
        "field_warnings": totals["field_warnings"],
        "events_total": totals["events_total"],
        "malformed_events": totals["malformed_events"],
        "duplicate_events_dropped": totals["duplicate_events_dropped"],
        "orphan_events": statuses.get("orphan", 0),
        "unknown_disposition_events": statuses.get("ignored_unknown_disposition", 0),
    }
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
