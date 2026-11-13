"""Integration tests require PostgreSQL. They fail, rather than skip, when it is down."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import psycopg
import pytest

ADMIN_URL = os.environ.get(
    "ADMIN_DATABASE_URL",
    "postgresql://au:au_local_only@localhost:5432/postgres",
)
TEST_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://au:au_local_only@localhost:5432/au_assignment_test",
)


def _database_name(url: str) -> str:
    return urlparse(url).path.lstrip("/").split("?")[0]


@pytest.fixture(scope="module")
def database_url():
    name = _database_name(TEST_URL)
    if name != "au_assignment_test":
        pytest.fail(
            "TEST_DATABASE_URL must point at a dedicated database named au_assignment_test "
            f"(got {name!r}). Refusing to run destructive tests against another database."
        )
    try:
        admin = psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=5)
    except psycopg.OperationalError as exc:
        pytest.fail(
            "PostgreSQL is required for integration tests. They are not skipped when it is unavailable. "
            f"Could not connect with ADMIN_DATABASE_URL={ADMIN_URL!r}: {exc}. "
            "Start Postgres (`docker compose up -d`) and set ADMIN_DATABASE_URL and TEST_DATABASE_URL. "
            "See solution/README.md."
        )
    with admin:
        row = admin.execute(
            "SELECT 1 FROM pg_database WHERE datname = 'au_assignment_test'"
        ).fetchone()
        if row is None:
            admin.execute("CREATE DATABASE au_assignment_test")
    return TEST_URL


@pytest.fixture
def ready(database_url):
    from au_pipeline.db import bind_key, connect, reset_database

    from tests.constants import TEST_KEY_BYTES

    connection = connect(database_url)
    reset_database(connection)
    bind_key(connection, TEST_KEY_BYTES)
    try:
        yield connection
    finally:
        connection.close()
