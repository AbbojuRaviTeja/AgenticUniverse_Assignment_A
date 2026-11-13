from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg

from au_pipeline.db import DEFAULT_URL, bind_key, connect, migrate, reset_database
from au_pipeline.errors import PipelineError
from au_pipeline.ingest import ingest_drop
from au_pipeline.paths import schemas_dir
from au_pipeline.pii import derived_keys, load_raw_key
from au_pipeline.reports import write_reports
from au_pipeline.schemas import load_schemas


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="au_pipeline")
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser("ingest", help="Ingest one or more drop directories, in the order given")
    ingest.add_argument("drops", type=Path, nargs="+")

    report = commands.add_parser("report", help="Write the five report files from the database")
    report.add_argument("--out", type=Path, required=True)

    commands.add_parser("reset", help="Drop and recreate the schema on an assignment database")

    args = parser.parse_args(argv)
    try:
        connection = connect(os.environ.get("DATABASE_URL", DEFAULT_URL))
    except psycopg.OperationalError as exc:
        print(
            "Could not connect to Postgres. Set DATABASE_URL "
            "(default postgresql://au:au_local_only@localhost:5432/au_assignment) "
            f"and start the server.\n{exc}",
            file=sys.stderr,
        )
        return 1
    try:
        directory = schemas_dir()
        if args.command == "reset":
            reset_database(connection, directory)
            print("database reset")
            return 0
        migrate(connection, directory)
        raw_key = load_raw_key()
        bind_key(connection, raw_key)
        aes_key, mac_key = derived_keys(raw_key)
        if args.command == "ingest":
            schemas = load_schemas(directory)
            for drop in args.drops:
                ingest_drop(connection, drop, aes_key, mac_key, schemas)
            return 0
        write_reports(connection, args.out, aes_key)
        print(f"wrote {args.out}")
        return 0
    except PipelineError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        connection.close()
