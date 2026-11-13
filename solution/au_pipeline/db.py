from __future__ import annotations

import hashlib
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from au_pipeline.errors import PipelineError
from au_pipeline.schemas import load_disposition_rows, load_schemas

DEFAULT_URL = "postgresql://au:au_local_only@localhost:5432/au_assignment"
_MIGRATIONS = Path(__file__).resolve().parent / "sql" / "migrations"
RESET_ALLOWED = frozenset({"au_assignment", "au_assignment_test"})


def connect(url: str | None = None) -> psycopg.Connection:
    chosen = url or os.environ.get("DATABASE_URL", DEFAULT_URL)
    connection = psycopg.connect(chosen, row_factory=dict_row)
    connection.execute("SET TIME ZONE 'UTC'")
    # Docker's default /dev/shm is 64MB. Parallel aggregates on the large
    # ledger fail with "could not resize shared memory segment".
    connection.execute("SET max_parallel_workers_per_gather = 0")
    connection.commit()
    return connection


def split_sql(script: str) -> list[str]:
    """Split a script on semicolons that are not inside quotes or dollar-quotes.

    Function and trigger bodies use dollar-quoting, so a line-ending semicolon
    is not a safe delimiter.
    """
    statements: list[str] = []
    buffer: list[str] = []
    index = 0
    length = len(script)
    dollar: str | None = None
    in_single = False
    while index < length:
        if dollar is not None:
            if script.startswith(dollar, index):
                buffer.append(dollar)
                index += len(dollar)
                dollar = None
                continue
            buffer.append(script[index])
            index += 1
            continue
        if in_single:
            buffer.append(script[index])
            if script[index] == "'":
                if index + 1 < length and script[index + 1] == "'":
                    buffer.append("'")
                    index += 2
                    continue
                in_single = False
            index += 1
            continue
        if script.startswith("--", index):
            end = script.find("\n", index)
            if end < 0:
                break
            buffer.append(script[index : end + 1])
            index = end + 1
            continue
        if script[index] == "'":
            in_single = True
            buffer.append("'")
            index += 1
            continue
        if script[index] == "$":
            tag_end = index + 1
            while tag_end < length and (script[tag_end].isalnum() or script[tag_end] == "_"):
                tag_end += 1
            if tag_end < length and script[tag_end] == "$":
                dollar = script[index : tag_end + 1]
                buffer.append(dollar)
                index = tag_end + 1
                continue
        if script[index] == ";":
            statement = "".join(buffer).strip()
            buffer = []
            if _meaningful(statement):
                statements.append(statement)
            index += 1
            continue
        buffer.append(script[index])
        index += 1
    trailing = "".join(buffer).strip()
    if _meaningful(trailing):
        statements.append(trailing)
    return statements


def _meaningful(statement: str) -> bool:
    kept = []
    for line in statement.splitlines():
        if line.strip().startswith("--"):
            continue
        kept.append(line)
    return bool("".join(kept).strip())


def execute_script(connection: psycopg.Connection, script: str) -> None:
    for statement in split_sql(script):
        connection.execute(statement)


def assert_reset_allowed(database_name: str) -> None:
    """Refuse DROP SCHEMA unless the database is one of the assignment databases."""
    if database_name in RESET_ALLOWED:
        return
    if os.environ.get("AU_ALLOW_RESET") == "1":
        return
    raise PipelineError(
        f"Refusing to reset database {database_name!r}. "
        "Destructive reset is only allowed for au_assignment and au_assignment_test. "
        "Set AU_ALLOW_RESET=1 if you really intend to reset a different database."
    )


def _legacy_schema(connection: psycopg.Connection) -> bool:
    row = connection.execute(
        """
        SELECT 1 AS present
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'events'
          AND column_name = 'lead_id'
        """
    ).fetchone()
    return row is not None


def migrate(connection: psycopg.Connection, directory: Path | None = None) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    connection.commit()
    applied = {
        row["version"]
        for row in connection.execute("SELECT version FROM schema_migrations").fetchall()
    }
    files = sorted(_MIGRATIONS.glob("*.sql"))
    if not files:
        raise PipelineError(f"no SQL migrations in {_MIGRATIONS}")
    pending = []
    for path in files:
        version = int(path.name.split("_", 1)[0])
        if version not in applied:
            pending.append((version, path))
    if pending and _legacy_schema(connection):
        raise PipelineError(
            "This database has the previous ledger schema (events.lead_id) and no matching "
            "migration history. Refusing to layer a new schema on top of it. "
            "On a disposable assignment database, run: python -m au_pipeline reset"
        )
    for version, path in pending:
        with connection.transaction():
            execute_script(connection, path.read_text(encoding="utf-8"))
            connection.execute(
                "INSERT INTO schema_migrations (version, name) VALUES (%s, %s)",
                (version, path.name),
            )
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO dispositions (workflow_id, code, label, is_terminal, is_success)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (workflow_id, code) DO UPDATE SET
                label = EXCLUDED.label,
                is_terminal = EXCLUDED.is_terminal,
                is_success = EXCLUDED.is_success
            """,
            load_disposition_rows(directory),
        )
        defaults = [
            (schema.workflow_id, field.name, field.default)
            for schema in load_schemas(directory).values()
            for field in schema.fields
            if field.default is not None
        ]
        cursor.executemany(
            """
            INSERT INTO field_defaults (workflow_id, field_name, default_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (workflow_id, field_name) DO UPDATE SET
                default_value = EXCLUDED.default_value
            """,
            defaults,
        )
    connection.commit()


def reset_database(connection: psycopg.Connection, directory: Path | None = None) -> None:
    name = connection.execute("SELECT current_database() AS name").fetchone()["name"]
    assert_reset_allowed(name)
    connection.execute("DROP SCHEMA IF EXISTS public CASCADE")
    connection.execute("CREATE SCHEMA public")
    connection.commit()
    migrate(connection, directory)


def key_fingerprint(raw_key: bytes) -> str:
    return hashlib.sha256(raw_key).hexdigest()


def bind_key(connection: psycopg.Connection, raw_key: bytes) -> None:
    """Record the key fingerprint, or refuse when it does not match the database."""
    fingerprint = key_fingerprint(raw_key)
    row = connection.execute("SELECT fingerprint FROM pii_key WHERE id = 1").fetchone()
    if row is None:
        occupied = connection.execute(
            """
            SELECT
                EXISTS (SELECT 1 FROM leads)
                OR EXISTS (SELECT 1 FROM events)
                OR EXISTS (SELECT 1 FROM ingest_files) AS present
            """
        ).fetchone()
        if occupied["present"]:
            raise PipelineError(
                "This database already has ingested data but no key fingerprint. "
                "Refusing to attach a new key. Restore the original AU_PII_KEY on a "
                "database that already has a fingerprint, or reset a disposable database."
            )
        connection.execute(
            "INSERT INTO pii_key (id, fingerprint) VALUES (1, %s)",
            (fingerprint,),
        )
        connection.commit()
        return
    if row["fingerprint"] != fingerprint:
        connection.rollback()
        raise PipelineError(
            "AU_PII_KEY does not match the key already recorded for this database. "
            "Refusing to ingest or report. The stored fingerprint was not replaced."
        )
    # The SELECT above opens a transaction. Commit it so later ingest
    # transactions are not savepoints that disappear when the process exits.
    connection.commit()
