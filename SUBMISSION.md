# Submission

## Candidate
- **Name:** Abboju Ravi Teja
- **Assignment chosen:** A (Data)
- **Time spent (honest estimate):** about 8 hours.

## How to run

PostgreSQL 16 is required. `docker compose up -d` starts it with user `au`, password `au_local_only`, and database `au_assignment`. Python 3.11+.

### Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | ingest, report, reset | Default `postgresql://au:au_local_only@localhost:5432/au_assignment` |
| `AU_PII_KEY` | ingest and report | A 32-byte key, 64 hex characters or standard base64. The program never generates one |
| `PYTHONPATH` | yes | `solution` |
| `TEST_DATABASE_URL` | pytest | Must name the database `au_assignment_test` |
| `ADMIN_DATABASE_URL` | pytest | Connection used to create that database, usually the `postgres` database on the same server |
| `AU_ALLOW_RESET` | no | `1` allows `reset` on a database other than `au_assignment` or `au_assignment_test` |

If port 5432 is already in use, add a gitignored `docker-compose.override.yml` that publishes another host port and point the URLs at it.

### Windows PowerShell

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

### Bash

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

`pytest` runs the unit tests with no database and the integration tests against `TEST_DATABASE_URL`. If Postgres is down, the integration tests fail. They do not skip.

`report` does not read the drop files. It does write: it applies any pending migration, refreshes the disposition catalogue and field defaults, and records the key fingerprint on the first run. It then reads the ledger, decrypts phone numbers only long enough to mask them, and writes the five files.

## Design

A drop is a directory. `ingest` can take several drops in one command and applies them in the order given. Inside a drop, every lead CSV is applied in path order, then every event file in name order, one transaction per file. `relink` runs after each drop.

File identity is `<drop directory name>/<relative path>`. `ingest_files.content_sha256` is the SHA-256 of the bytes. The same identity and the same hash is a skip. The same identity and a different hash is an error. A changed file is never skipped.

Leads are keyed by `(workflow_id, HMAC-SHA256 of the canonical phone)`. The phone is also stored as AES-GCM ciphertext. Every other non-reference field is stored in `leads.attributes` from the workflow schema. A field with `"pii": true` is AES-GCM ciphertext inside that JSON. The reference field is `external_ref` because the lead report has to print it. Reports decrypt the phone only long enough to write `+91******` plus the last four digits. `rejections.csv` has no PII.

`AU_PII_KEY` must be 32 random bytes. The database stores SHA-256 of that key in `pii_key`, not the key. A later run with a different key stops before ingest or report. A key is never invented, and an existing fingerprint is never replaced.

The event ledger is append-only. `events` has a trigger that rejects `UPDATE`, `DELETE`, and `TRUNCATE`. Link state lives in `event_links` (`lead_id`, `link_status`). An orphan can become linked when its lead arrives later, or ignored when the disposition code is not in the catalogue. A `disposition.override` does not edit the original `disposition.set`. The effective disposition is computed in the `call_facts` view: the latest linked override if any linked override exists, otherwise the latest linked set. Same timestamp, later `event_seq` wins. Text ordering in the views and in the report queries uses `COLLATE "C"`.

A batch row is one file (`batches.file_key`). The reported `batch_id` is still the file stem. Two drops can both contain `batch_001.csv` without sharing a primary key. Leads keep the stem of the file that created them.

Defaults such as `preferred_language → en` are applied in `leads_resolved` when the attribute is absent, not written onto the row. A later batch can still fill it. The five graded files do not print language.

Schema changes are versioned SQL files in `solution/au_pipeline/sql/migrations`. The runner records `schema_migrations`. It does not try to `CREATE TABLE IF NOT EXISTS` over an older ledger that still has `events.lead_id`; that database has to be reset.

```text
lead file -> validate -> reject | merge by phone HMAC
event file -> duplicate check -> timestamp -> ledger row + orphan link
relink -> event_links: linked | ignored unknown code | still orphan
views -> batch_summary, daily_overview, lead_report, rejections, data_quality
```

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| Postgres 16 from the repo compose file | The role is a Postgres backend, and the schema is part of what gets graded | SQLite, which would run with no Docker |
| HMAC for the join, AES-GCM for PII fields | A raw hash of a 10-digit mobile can be brute-forced. The report still needs the last four digits | Store only the last four and lose the ability to show a masked value from one stored secret |
| 32-byte `AU_PII_KEY`, fingerprint stored, no auto-generation | A passphrase and a silently written `.pii_key` made a second run able to fork the lead set | Keep accepting any string and hashing it |
| Idempotency key is `<drop name>/<relative path>`, checked against `content_sha256` | Re-running the same bytes must change nothing. Different bytes under that name must not be skipped | Content hash alone, which would treat a replaced file as a new file and could double-count |
| Reported `batch_id` stays the file stem; the batch row is the file key | The contract says the stem is the batch id. The same stem in two drops must not share a primary key | Put the drop name in the report column |
| Leads in a drop are loaded before events in that drop | Same-day events should link immediately. Events that arrive in an earlier drop stay orphans until `relink` | Interleaving by filename |
| A batch file with zero created leads still gets a `batch_summary` row of zeros | The question is how that upload did, and a file of rejections is part of the answer | Omit empty batches |
| Unknown disposition codes stay in the ledger with link status `ignored_unknown_disposition` | The count has to be reconstructable, and the row must stay out of every report | Delete the row after counting it |
| A JSON object with an `event_id` but no usable timestamp is malformed and is not stored | The spec's malformed rule is only "bad JSON or no event_id". A ledger row still needs a time, and inventing one would put the call on the wrong IST date. This choice changes `malformed_events` if a file contains such lines. The generator does not emit them | Store the row with a null time |
| Duplicate `event_id` is decided before the timestamp | A second copy of an id already accepted or already stored counts as a duplicate even when that copy has a bad timestamp. A first copy with a bad timestamp does not reserve the id | Count every bad timestamp as malformed, including duplicates |
| ISO-8601 with fractional seconds is accepted. A date with no time is not | `2026-10-05T13:20:21.500Z` and offsets such as `-04:00` are normal timestamps. Naive still means IST | Reject anything the three examples in the spec do not spell out |
| Disposition codes are matched exactly against `dispositions.csv` | The catalogue is the canonical code. `RENEWD` is a different code, not a case variant | Case-fold, which would hide bad agent output |
| Header normalisation strips again after collapsing spaces | `Policy_No_` becomes `policy no`, which matches the alias. A literal reading would leave a trailing space | Leave the trailing space |
| `rejections_by_reason` contains only reasons that occurred, keys sorted | The example in the spec is the shape, and a textual diff of zero-valued keys would depend on schemas we have not seen | Emit a zero for every possible reason |
| Money rounds half up to 2 places | "Round to 2 places" on a currency | Banker's rounding |
| Two `call.initiated` rows for one `call_id` are one call, dated from the earliest | A call is a `call_id`, not an event row | Count each initiated row |
| Connectivity uses round-half-up to 4 decimal places | The contract shows `0.5625`. A tie at the 5th decimal, such as 1/160, becomes `0.0063` | Python's default half-even formatting |
| Reference field is plaintext `external_ref` even if a future schema marked it `pii` | The lead report has to print it. Other `pii` fields are encrypted | Encrypt it and omit it from the report |
| Optional attributes are JSON, nulls omitted | A new schema field is stored without a migration. Last non-null value wins | A column per known field |
| `reset` only on the two assignment database names | `DROP SCHEMA` must not run against whatever `DATABASE_URL` happens to be | Ask for confirmation in the terminal |

## Failure modes considered

- Bad phones, missing required fields, blank rows, renamed headers, duplicate leads, and invalid optional dates or amounts are counted and do not abort the file.
- Re-running `ingest` on a file already stored with the same hash does not read it again. A different hash aborts that file before any write. `event_id` and `(workflow_id, phone_hmac)` are unique, so a replay that skipped the file log would still not double-insert.
- A crash mid-file rolls the transaction back, including the file log, so the next run processes that file once.
- An event that arrives before its lead is stored as an orphan link. `relink` runs at the end of every ingest, including a re-run, so the orphan count in `data_quality.json` is the final state.
- An override with no phone links through `call_id` after any other event for that call has a lead. Until then it stays an orphan. It does not become an unknown-disposition event while it is still unlinked, because the spec checks "orphan" before "unknown code".
- Duplicate `event_id`s, including the same line delivered in a later drop, are counted and dropped. That check happens before timestamp validation.
- Malformed lines are counted and not stored.
- A directory with no lead CSV and no event JSONL is an error.
- An encryption key that does not match `pii_key` stops the command. The fingerprint is not overwritten.
- `UPDATE` or `DELETE` on `events` is rejected by a trigger. Linking rows can still change.
- What is not handled: two processes ingesting the same drop at the same time, key rotation for existing ciphertext, and a lead file whose workflow has no schema (the command exits).

## Testing

`pytest` covers the pure rules with no database, and the ledger with Postgres. The integration tests fail if Postgres is unreachable.

Unit tests, no database:

- phone formats, day-first dates, impossible dates, currency prefixes, header normalisation
- fractional seconds and timezone offsets, including a negative offset
- missing timestamps counted as malformed, and a duplicate id counted before the timestamp check
- rejection order (missing name wins over a bad phone)
- connectivity half-up, including 1/160
- PII versus non-PII attribute encoding
- file-hash decision and the reset database-name guard
- SQL splitting that keeps a trigger function intact

Integration tests, Postgres:

- the hand-built drops and a second ingest that leaves all five files byte-identical
- ingesting both fixture drops in one command matches ingesting them separately
- several overrides, an unknown override falling back to the set, and a later unknown set leaving the earlier valid set
- an orphan linked after its lead arrives, and an override that never links
- `completed` ahead of a telephony failure, and a mix of failed and normal calls
- two successful calls for one lead on one date still count as one success lead
- `COLLATE "C"` order for `A-2`, `A/1`, `B`, `a1`
- the same file stem in two drops
- a new optional field, with the `pii` one encrypted
- changed file bytes rejected, key mismatch rejected, and `UPDATE`/`DELETE` on `events` rejected
- names and raw phones absent from stored text and from the report

`pytest` on the final code: **32 passed** in 27.62s, with Postgres 16 on localhost port 5433.

Checked outside pytest, against an independent reading of the spec, on fresh `--scale small` data: seeds 42, 7, and 2026. All five files matched. Re-ingesting matched byte for byte. One command with both drops matched two commands. Seed 42 quality totals:

```json
{
  "lead_rows_total": 2057,
  "lead_rows_rejected": 108,
  "rejections_by_reason": {
    "invalid_phone": 42,
    "missing_required:customer_name": 25,
    "missing_required:order_id": 13,
    "missing_required:phone": 14,
    "missing_required:policy_number": 14
  },
  "duplicate_lead_rows_merged": 57,
  "field_warnings": 122,
  "events_total": 8745,
  "malformed_events": 8,
  "duplicate_events_dropped": 237,
  "orphan_events": 462,
  "unknown_disposition_events": 18
}
```

1892 leads. 2057 data rows minus 108 rejections minus 57 merges is 1892, which matches `lead_report.csv`.

### Large dataset, measured on this build

`--scale large`, seed 42, generated in this session (18.7s, 632,602 event lines). Postgres 16 from `docker compose`, published on host port 5433 because 5432 was already taken. Timings are wall clock around the commands, after `reset`:

- ingest `drop_1`: 90.1s
- ingest `drop_2`: 55.6s
- `report`: 8.5s
- ingest plus report: 154.2s

That is over the two-minute stretch goal. The first `report` attempt failed with `could not resize shared memory segment` because Docker's `/dev/shm` is 64MB and Postgres tried a parallel aggregate. The connection now sets `max_parallel_workers_per_gather = 0`. The 8.5s figure is the successful run after that change. The ingest timings above are from the run before it; ingest does not use that parallel plan.

`data_quality.json` from that report: 143,990 lead rows, 7,097 rejected, 3,990 merged, 132,903 leads (the six batch totals add up to the same 132,903). Every `phone_masked` value matched `+91******` plus four digits (132,903 rows, none otherwise). An independent checker matched all five files, including all 132,903 lead rows.

## Production readiness

What is in place: one transaction per file, a content hash on the file identity, an append-only event table, a separate link table, a key fingerprint, schema migrations, and reports computed from views so a late event cannot leave a stored bucket behind.

What I would not call production yet:

- Two workers ingesting the same drop can race. The unique keys prevent double inserts, but the quality counters and the file row are not a distributed lock.
- `relink` and the report views recompute across the whole ledger. Skipping an unchanged file is incremental; the read side is not.
- There is no key rotation. Ciphertext is tied to the fingerprint recorded on first use.
- AES-GCM is used without associated data, so ciphertext swapped between two PII fields of the same type would still decrypt.
- The key derivation is domain-separated SHA-256 of the 32-byte key, not a slow KDF. That is enough for a random key and the wrong tool for a passphrase, which is why passphrases are rejected.
- A lead file for an unknown workflow aborts the command.
- `reset` is a full schema drop. It is guarded by database name, and it is still the wrong tool for a database that already holds a day of production data.
- `--scale large` ingest plus report took 154.2s on this laptop, past the two-minute stretch. The cost is the per-row lead merge and the second pass over event ids, not the report.

For 100k leads a day I would run one worker per drop, keep the file log as the checkpoint, run `relink` after the drop, and treat `report` as a separate job that is the only process allowed to decrypt. The key would come from a secret manager. Backfill of a late lead is `relink` plus `report`, not a replay of the event files.

## If I had another day

1. Move event and lead insert onto `COPY`. The large report is already 8.5s; the 154s is almost all ingest.
2. Add associated data to AES-GCM so a ciphertext cannot be moved to another row.
3. The dispatcher from Assignment B should emit this same ledger, not a second store.
