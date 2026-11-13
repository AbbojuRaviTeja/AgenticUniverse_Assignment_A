# AI Usage Disclosure

## Tools used

Cursor and its AI model, used during the initial implementation and subsequent hardening of the pipeline.

## What I used them for

I used AI assistance to understand the assignment, draft the initial pipeline (normalizers, PostgreSQL schema, ingestion logic, SQL views, report writers, and tests), and address findings from a later audit.

The hardening work included file-hash checks, test failures instead of silent skips, deterministic sorting with `COLLATE "C"`, timestamp parsing, PII key fingerprinting, the append-only event ledger, schema-driven lead fields, migrations, and additional tests.

I also used AI assistance to run pytest and compare results for seeds 42, 7, and 2026 against an independent interpretation of the specification.

## What I decided myself

- Chose Assignment A over Assignment B.
- Chose PostgreSQL rather than SQLite.
- Used an HMAC of the canonical phone number for identity and AES-GCM for PII fields.
- Kept reports as SQL views.
- Chose drop-relative path plus stored content hash for idempotency, rather than using the hash alone.
- Decided that a missing or unusable timestamp is malformed and should not be stored.
- Kept the reference field plaintext because the lead report must display it.
- Restricted `reset` to the two permitted assignment database names.

## Where the AI was wrong or unhelpful

The initial plan used SQLite to avoid requiring Docker for reviewers. I rejected that approach because PostgreSQL was more appropriate for the assignment and repository.

A later bug left the `bind_key` fingerprint read inside an open transaction. As a result, a second ingestion process rolled back its writes when it exited. The fixture report was missing the second drop until the transaction issue was fixed.

The model also initially needed correction on key handling: a short passphrase is not an acceptable `AU_PII_KEY`.

## How I verified AI-generated code

I reviewed the ingestion path, SQL views, and append-only trigger against the assignment specification.

The final pytest run reported **32 passed in 27.62 seconds**, with PostgreSQL running.

For seeds 42, 7, and 2026 at `--scale small`, all five report files matched an independent implementation. Re-ingesting the same files left the outputs byte-identical. Ingesting both drops in one command produced the same result as ingesting them separately. A changed file under an existing identity was rejected.

I also verified that a mismatched `AU_PII_KEY` was rejected and that the error message did not expose the key. A sample customer name and phone from seed 42 were absent from lead attributes, event payloads, and report files.

For `--scale large` with seed 42, ingestion took 90.1 seconds for the first drop and 55.6 seconds for the second. The initial report attempt failed because of a Docker shared-memory limitation. After disabling parallel workers in the database connection settings, report generation completed in 8.5 seconds.

The total measured runtime was approximately **154.2 seconds**, which is above the two-minute stretch target. All five large-scale outputs were checked. The dataset contained 132,903 leads, and all masked phone numbers checked had the required format. Batch totals also matched the data-quality counts.  