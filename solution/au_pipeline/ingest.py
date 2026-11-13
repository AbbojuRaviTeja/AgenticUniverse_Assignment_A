"""Lead and event ingest. Files are applied in name order, one transaction each."""

from __future__ import annotations

import base64
import csv
import hashlib
import hmac
import json
from dataclasses import dataclass
from pathlib import Path

from psycopg.types.json import Jsonb

from au_pipeline.errors import PipelineError
from au_pipeline.normalize import (
    canonical_enum,
    canonical_phone,
    normalize_header,
    parse_date,
    parse_decimal,
    parse_occurred_at,
)
from au_pipeline.pii import encrypt, phone_hmac
from au_pipeline.schemas import Field, WorkflowSchema


@dataclass
class ParsedLead:
    reason: str | None
    values: dict
    warnings: int


def column_map(fieldnames: list[str] | None) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for name in fieldnames or []:
        if not name:
            continue
        mapping.setdefault(normalize_header(name), name)
    return mapping


def is_blank(row: dict) -> bool:
    for value in row.values():
        if isinstance(value, list):
            if any(str(item).strip() for item in value if item is not None):
                return False
        elif value is not None and str(value).strip():
            return False
    return True


def _lookup(field: Field, row: dict, columns: dict[str, str]) -> str | None:
    for candidate in (field.name, *field.aliases):
        key = columns.get(normalize_header(candidate))
        if key is None:
            continue
        raw = row.get(key)
        text = "" if raw is None else str(raw).strip()
        return text or None
    return None


def _parse_field(field: Field, raw: str):
    if field.type == "string":
        return raw
    if field.type == "phone":
        return canonical_phone(raw)
    if field.type == "date":
        return parse_date(raw)
    if field.type == "decimal":
        return parse_decimal(raw)
    if field.type == "enum":
        return canonical_enum(raw, field.values)
    raise ValueError(f"unsupported field type {field.type}")


def parse_lead(schema: WorkflowSchema, row: dict, columns: dict[str, str]) -> ParsedLead:
    """First rejection wins. Warnings on a rejected row are discarded."""
    values: dict = {}
    reason = None
    warnings = 0
    for field in schema.fields:
        raw = _lookup(field, row, columns)
        if raw is None:
            if field.required and reason is None:
                reason = f"missing_required:{field.name}"
            values[field.name] = None
            continue
        parsed = _parse_field(field, raw)
        if parsed is None:
            if reason is None:
                if field.type == "phone":
                    reason = "invalid_phone"
                elif field.required:
                    reason = f"invalid_{field.name}"
                else:
                    warnings += 1
            values[field.name] = None
        else:
            values[field.name] = parsed
    if reason:
        warnings = 0
    return ParsedLead(reason, values, warnings)


def _file_key(drop: Path, path: Path) -> str:
    """Drop directory name plus the path inside it. Same name means the same identity."""
    return f"{drop.name}/{path.relative_to(drop).as_posix()}"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def file_decision(stored_hash: str | None, new_hash: str) -> str:
    """Return ``skip`` or ``ingest``. A changed file is an error, never a skip."""
    if stored_hash is None:
        return "ingest"
    if len(stored_hash) == len(new_hash) and hmac.compare_digest(stored_hash, new_hash):
        return "skip"
    raise PipelineError(
        "A file already ingested under this identity has different contents. "
        f"Stored content hash {stored_hash}, new content hash {new_hash}. "
        "Changed input is not skipped."
    )


def _attribute_value(field: Field, parsed, aes_key: bytes):
    if field.type == "date":
        plain = parsed.isoformat()
    elif field.type == "decimal":
        plain = f"{parsed:.2f}"
    else:
        plain = str(parsed)
    if not field.pii:
        return plain
    nonce, ciphertext = encrypt(aes_key, plain)
    return {
        "n": base64.b64encode(nonce).decode("ascii"),
        "c": base64.b64encode(ciphertext).decode("ascii"),
    }


def lead_row(schema: WorkflowSchema, values: dict, aes_key: bytes, mac_key: bytes) -> dict:
    """Phone HMAC plus ciphertext, the reference as external_ref, other fields as attributes."""
    phone_fields = [field for field in schema.fields if field.type == "phone"]
    if len(phone_fields) != 1:
        raise PipelineError(f"{schema.workflow_id} must declare exactly one phone field")
    canonical = values.get(phone_fields[0].name)
    if not canonical:
        raise PipelineError(f"{schema.workflow_id} accepted a row without a canonical phone")
    external = values.get(schema.reference_field)
    if not external:
        raise PipelineError(
            f"{schema.workflow_id} accepted a row without reference field {schema.reference_field}"
        )
    nonce, ciphertext = encrypt(aes_key, canonical)
    attributes = {}
    for field in schema.fields:
        if field.type == "phone" or field.reference:
            continue
        parsed = values.get(field.name)
        if parsed is None:
            continue
        attributes[field.name] = _attribute_value(field, parsed, aes_key)
    return {
        "phone_hmac": phone_hmac(mac_key, canonical),
        "phone_ciphertext": ciphertext,
        "phone_nonce": nonce,
        "external_ref": external,
        "attributes": attributes,
    }


def ingest_drop(
    connection,
    drop: Path,
    aes_key: bytes,
    mac_key: bytes,
    schemas: dict[str, WorkflowSchema],
) -> None:
    drop = drop.resolve()
    if not drop.is_dir():
        raise PipelineError(f"drop not found: {drop}")
    lead_files = sorted(drop.glob("leads/*/*.csv"), key=lambda path: path.relative_to(drop).as_posix())
    event_files = sorted(drop.glob("events/*.jsonl"), key=lambda path: path.name)
    if not lead_files and not event_files:
        raise PipelineError(
            f"no lead CSV or event JSONL files in {drop}. "
            "Expected leads/<workflow>/*.csv and/or events/*.jsonl."
        )
    for path in lead_files:
        _ingest_leads(connection, drop, path, aes_key, mac_key, schemas)
    for path in event_files:
        _ingest_events(connection, drop, path, mac_key)
    _relink(connection)


def _stored_hash(connection, file_key: str) -> str | None:
    row = connection.execute(
        "SELECT content_sha256 FROM ingest_files WHERE file_key = %s",
        (file_key,),
    ).fetchone()
    if row is None:
        return None
    return row["content_sha256"]


def _ingest_leads(connection, drop: Path, path: Path, aes_key: bytes, mac_key: bytes, schemas) -> None:
    workflow_id = path.parent.name
    schema = schemas.get(workflow_id)
    if schema is None:
        raise PipelineError(f"no schema for workflow {workflow_id} ({path})")
    batch_id = path.stem
    key = _file_key(drop, path)
    digest = _sha256(path)
    with connection.transaction():
        if file_decision(_stored_hash(connection, key), digest) == "skip":
            print(f"skip {key}")
            return
        connection.execute(
            """
            INSERT INTO batches (file_key, workflow_id, batch_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (file_key) DO NOTHING
            """,
            (key, workflow_id, batch_id),
        )
        accepted, rejected, warnings, total = _read_lead_file(path, schema, aes_key, mac_key)
        if rejected:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO rejections (file_key, workflow_id, batch_id, row_number, reason)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [(key, workflow_id, batch_id, number, reason) for number, reason in rejected],
                )
        merged = 0
        if accepted:
            merged = _merge_leads(connection, workflow_id, batch_id, key, accepted)
        connection.execute(
            """
            UPDATE quality_totals SET
                lead_rows_total = lead_rows_total + %s,
                lead_rows_rejected = lead_rows_rejected + %s,
                duplicate_lead_rows_merged = duplicate_lead_rows_merged + %s,
                field_warnings = field_warnings + %s
            WHERE id = 1
            """,
            (total, len(rejected), merged, warnings),
        )
        connection.execute(
            "INSERT INTO ingest_files (file_key, content_sha256) VALUES (%s, %s)",
            (key, digest),
        )
    print(f"ingested {key}")


def _read_lead_file(path: Path, schema: WorkflowSchema, aes_key: bytes, mac_key: bytes):
    accepted = []
    rejected = []
    warnings = 0
    total = 0
    sequence = 0
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = column_map(reader.fieldnames)
        for row in reader:
            if is_blank(row):
                continue
            total += 1
            parsed = parse_lead(schema, row, columns)
            if parsed.reason:
                rejected.append((total, parsed.reason))
                continue
            warnings += parsed.warnings
            sequence += 1
            built = lead_row(schema, parsed.values, aes_key, mac_key)
            accepted.append(
                (
                    sequence,
                    built["phone_hmac"],
                    built["phone_ciphertext"],
                    built["phone_nonce"],
                    built["external_ref"],
                    Jsonb(built["attributes"]),
                )
            )
    return accepted, rejected, warnings, total


def _merge_leads(connection, workflow_id: str, batch_id: str, file_key: str, rows: list[tuple]) -> int:
    """Last non-null attribute in file order wins. batch_id stays with the creating row."""
    connection.execute(
        """
        CREATE TEMP TABLE incoming (
            seq INTEGER PRIMARY KEY,
            phone_hmac TEXT NOT NULL,
            phone_ciphertext BYTEA NOT NULL,
            phone_nonce BYTEA NOT NULL,
            external_ref TEXT NOT NULL,
            attributes JSONB NOT NULL
        ) ON COMMIT DROP
        """
    )
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO incoming (
                seq, phone_hmac, phone_ciphertext, phone_nonce, external_ref, attributes
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    counts = connection.execute(
        """
        WITH ranked AS (
            SELECT phone_hmac, ROW_NUMBER() OVER (PARTITION BY phone_hmac ORDER BY seq) AS rn
            FROM incoming
        )
        SELECT
            (SELECT COUNT(*) FROM ranked WHERE rn > 1) AS extra_in_file,
            (SELECT COUNT(*) FROM ranked r
              WHERE r.rn = 1
                AND EXISTS (
                    SELECT 1 FROM leads l
                    WHERE l.workflow_id = %s AND l.phone_hmac = r.phone_hmac
                )) AS already_there
        """,
        (workflow_id,),
    ).fetchone()
    connection.execute(
        """
        INSERT INTO leads (
            workflow_id, phone_hmac, phone_ciphertext, phone_nonce,
            external_ref, batch_id, source_file_key, attributes
        )
        SELECT
            %s,
            ids.phone_hmac,
            phones.phone_ciphertext,
            phones.phone_nonce,
            refs.external_ref,
            %s,
            %s,
            COALESCE(attrs.attributes, '{}'::jsonb)
        FROM (SELECT DISTINCT phone_hmac FROM incoming) AS ids
        JOIN (
            SELECT DISTINCT ON (phone_hmac) phone_hmac, phone_ciphertext, phone_nonce
            FROM incoming ORDER BY phone_hmac, seq DESC
        ) AS phones USING (phone_hmac)
        JOIN (
            SELECT DISTINCT ON (phone_hmac) phone_hmac, external_ref
            FROM incoming ORDER BY phone_hmac, seq DESC
        ) AS refs USING (phone_hmac)
        LEFT JOIN (
            SELECT phone_hmac, jsonb_object_agg(key, value) AS attributes
            FROM (
                SELECT DISTINCT ON (incoming.phone_hmac, item.key)
                    incoming.phone_hmac, item.key, item.value
                FROM incoming
                CROSS JOIN LATERAL jsonb_each(incoming.attributes) AS item
                ORDER BY incoming.phone_hmac, item.key, incoming.seq DESC
            ) AS latest
            GROUP BY phone_hmac
        ) AS attrs USING (phone_hmac)
        ON CONFLICT (workflow_id, phone_hmac) DO UPDATE SET
            phone_ciphertext = EXCLUDED.phone_ciphertext,
            phone_nonce = EXCLUDED.phone_nonce,
            external_ref = EXCLUDED.external_ref,
            attributes = leads.attributes || EXCLUDED.attributes
        """,
        (workflow_id, batch_id, file_key),
    )
    return int(counts["extra_in_file"]) + int(counts["already_there"])


def _ingest_events(connection, drop: Path, path: Path, mac_key: bytes) -> None:
    key = _file_key(drop, path)
    digest = _sha256(path)
    with connection.transaction():
        if file_decision(_stored_hash(connection, key), digest) == "skip":
            print(f"skip {key}")
            return
        existing = _existing_event_ids(connection, _event_ids(path))
        with path.open(encoding="utf-8") as handle:
            total, malformed, duplicates, fresh = classify_events(handle, mac_key, existing)
        if fresh:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO events (
                        event_id, event_type, workflow_id, call_id,
                        occurred_at, phone_hmac, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    fresh,
                )
                cursor.executemany(
                    """
                    INSERT INTO event_links (event_id, link_status)
                    VALUES (%s, 'orphan')
                    """,
                    [(row[0],) for row in fresh],
                )
        connection.execute(
            """
            UPDATE quality_totals SET
                events_total = events_total + %s,
                malformed_events = malformed_events + %s,
                duplicate_events_dropped = duplicate_events_dropped + %s
            WHERE id = 1
            """,
            (total, malformed, duplicates),
        )
        connection.execute(
            "INSERT INTO ingest_files (file_key, content_sha256) VALUES (%s, %s)",
            (key, digest),
        )
    print(f"ingested {key}")


def _event_ids(path: Path) -> list[str]:
    found = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            event_id = payload.get("event_id")
            if event_id is None or str(event_id).strip() == "":
                continue
            found.append(str(event_id).strip())
    return found


def classify_events(lines, mac_key: bytes, existing_ids: set[str] | None = None):
    """Duplicate ids are counted before the timestamp. A bad timestamp does not reserve a new id."""
    existing = set(existing_ids or ())
    total = 0
    malformed = 0
    duplicates = 0
    fresh = []
    seen: set[str] = set()
    for line in lines:
        if not str(line).strip():
            continue
        total += 1
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            malformed += 1
            continue
        if not isinstance(payload, dict):
            malformed += 1
            continue
        event_id = payload.get("event_id")
        if event_id is None or str(event_id).strip() == "":
            malformed += 1
            continue
        event_id = str(event_id).strip()
        if event_id in seen or event_id in existing:
            duplicates += 1
            continue
        occurred_at = parse_occurred_at(str(payload.get("occurred_at") or ""))
        if occurred_at is None:
            malformed += 1
            continue
        seen.add(event_id)
        raw_phone = payload.get("phone")
        digest = None
        if isinstance(raw_phone, str) and raw_phone.strip():
            canonical = canonical_phone(raw_phone)
            if canonical:
                digest = phone_hmac(mac_key, canonical)
        body = payload.get("payload")
        if not isinstance(body, dict):
            body = {}
        call_id = payload.get("call_id")
        fresh.append(
            (
                event_id,
                str(payload.get("event_type") or ""),
                str(payload.get("workflow_id") or ""),
                None if call_id is None else str(call_id),
                occurred_at,
                digest,
                Jsonb(body),
            )
        )
    return total, malformed, duplicates, fresh


def _existing_event_ids(connection, event_ids: list[str]) -> set[str]:
    found: set[str] = set()
    unique = list(dict.fromkeys(event_ids))
    for start in range(0, len(unique), 5000):
        chunk = unique[start : start + 5000]
        rows = connection.execute(
            "SELECT event_id FROM events WHERE event_id = ANY(%s)",
            (chunk,),
        ).fetchall()
        found.update(row["event_id"] for row in rows)
    return found


def _relink(connection) -> None:
    """Attach orphans once their lead exists. Unknown codes are ignored, not reported."""
    with connection.transaction():
        connection.execute(
            """
            UPDATE event_links AS link
            SET lead_id = lead.lead_id,
                link_status = CASE
                    WHEN event.event_type IN ('disposition.set', 'disposition.override')
                         AND NOT EXISTS (
                             SELECT 1 FROM dispositions AS catalogue
                             WHERE catalogue.workflow_id = event.workflow_id
                               AND catalogue.code = event.payload->>'disposition_code'
                         )
                    THEN 'ignored_unknown_disposition'
                    ELSE 'linked'
                END
            FROM events AS event
            JOIN leads AS lead
              ON lead.workflow_id = event.workflow_id
             AND lead.phone_hmac = event.phone_hmac
            WHERE link.event_id = event.event_id
              AND link.link_status = 'orphan'
              AND event.phone_hmac IS NOT NULL
            """
        )
        connection.execute(
            """
            UPDATE event_links AS link
            SET lead_id = source.lead_id,
                link_status = CASE
                    WHEN NOT EXISTS (
                        SELECT 1 FROM dispositions AS catalogue
                        WHERE catalogue.workflow_id = event.workflow_id
                          AND catalogue.code = event.payload->>'disposition_code'
                    )
                    THEN 'ignored_unknown_disposition'
                    ELSE 'linked'
                END
            FROM events AS event
            JOIN (
                SELECT DISTINCT ON (event.workflow_id COLLATE "C", event.call_id COLLATE "C")
                    event.workflow_id, event.call_id, link.lead_id
                FROM events AS event
                JOIN event_links AS link ON link.event_id = event.event_id
                WHERE link.lead_id IS NOT NULL AND event.call_id IS NOT NULL
                ORDER BY event.workflow_id COLLATE "C", event.call_id COLLATE "C", event.event_seq
            ) AS source
              ON event.workflow_id = source.workflow_id
             AND event.call_id = source.call_id
            WHERE link.event_id = event.event_id
              AND link.link_status = 'orphan'
              AND event.event_type = 'disposition.override'
              AND event.call_id IS NOT NULL
            """
        )
