# Assignment A — interaction ledger

Postgres pipeline for lead files and call events. Reports are SQL views over an append-only event ledger. Lead-to-event links live in a separate table and may change when a late lead arrives.

## Requirements

- Python 3.11+
- PostgreSQL 16 (the repo `docker-compose.yml` is the intended server)
- Environment variables below. Nothing is generated for you.

| Variable | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | for ingest, report, reset | Pipeline database. Default `postgresql://au:au_local_only@localhost:5432/au_assignment` |
| `AU_PII_KEY` | for ingest and report | 32-byte key as 64 hex characters or standard base64. Never generated automatically |
| `PYTHONPATH` | yes | `solution`, so `python -m au_pipeline` imports the package |
| `TEST_DATABASE_URL` | for pytest | Dedicated test database. Must be named `au_assignment_test` |
| `ADMIN_DATABASE_URL` | for pytest | A database the role can use to `CREATE DATABASE` (usually `postgres` on the same server) |
| `AU_ALLOW_RESET` | no | Set to `1` only to allow `reset` against a database other than `au_assignment` or `au_assignment_test` |

`reset` runs `DROP SCHEMA public CASCADE`. It refuses any database name other than `au_assignment` and `au_assignment_test` unless `AU_ALLOW_RESET=1`.

## Windows (PowerShell)

If port 5432 is already taken, create `docker-compose.override.yml` (it is gitignored) with:

```yaml
services:
  postgres:
    ports: !override
      - "5433:5432"
```

Then use that host port in the URLs below. The commands here assume the compose default, port 5432.

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r solution\requirements.txt
docker compose up -d
$env:DATABASE_URL = "postgresql://au:au_local_only@localhost:5432/au_assignment"
$env:AU_PII_KEY = (py -3.11 -c "import os; print(os.urandom(32).hex())")
$env:PYTHONPATH = "solution"
python assignment-a-data\tools\generate_data.py --seed 42 --scale small --out data
python -m au_pipeline reset
python -m au_pipeline ingest data\drop_1 data\drop_2
python -m au_pipeline report --out reports
$env:TEST_DATABASE_URL = "postgresql://au:au_local_only@localhost:5432/au_assignment_test"
$env:ADMIN_DATABASE_URL = "postgresql://au:au_local_only@localhost:5432/postgres"
pytest
```

## Bash

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r solution/requirements.txt
docker compose up -d
export DATABASE_URL=postgresql://au:au_local_only@localhost:5432/au_assignment
export AU_PII_KEY="$(python -c 'import os; print(os.urandom(32).hex())')"
export PYTHONPATH=solution
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python -m au_pipeline reset
python -m au_pipeline ingest data/drop_1 data/drop_2
python -m au_pipeline report --out reports
export TEST_DATABASE_URL=postgresql://au:au_local_only@localhost:5432/au_assignment_test
export ADMIN_DATABASE_URL=postgresql://au:au_local_only@localhost:5432/postgres
pytest
```

## Commands

- `python -m au_pipeline ingest <drop> [<drop> ...]` reads each drop in order. A drop must contain `leads/<workflow>/*.csv` and/or `events/*.jsonl`. An empty directory is an error.
- `python -m au_pipeline report --out <dir>` writes the five report files.
- `python -m au_pipeline reset` drops and recreates the schema on an allowed database.

Re-running ingest on the same files does nothing. The same file identity with different bytes is an error. Keep `AU_PII_KEY` stable for a database; a different key is refused. Do not commit generated `data/`, `data_large/`, `reports/`, or the key.

Each connection sets `max_parallel_workers_per_gather = 0`. Docker's default shared memory is too small for a parallel aggregate over the large ledger, and that setting keeps `report` from failing there. It does not change the numbers.

`--scale large` (seed 42, this machine, Postgres 16): ingest 90.1s + 55.6s, report 8.5s. See `SUBMISSION.md`.
