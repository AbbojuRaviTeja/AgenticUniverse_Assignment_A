-- Version 1. Ledger rows are immutable. Lead linkage lives in event_links.
-- A batch is one file (file_key), even when two drops reuse the same file stem.
-- The reported batch_id is still that stem.

CREATE TABLE ingest_files (
    file_key TEXT PRIMARY KEY,
    content_sha256 TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE batches (
    file_key TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    batch_id TEXT NOT NULL
);

CREATE INDEX batches_report_idx ON batches (workflow_id, batch_id);

CREATE TABLE leads (
    lead_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    phone_hmac TEXT NOT NULL,
    phone_ciphertext BYTEA NOT NULL,
    phone_nonce BYTEA NOT NULL,
    external_ref TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    source_file_key TEXT NOT NULL REFERENCES batches (file_key),
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (workflow_id, phone_hmac)
);

CREATE INDEX leads_batch_idx ON leads (workflow_id, batch_id);
CREATE INDEX leads_source_idx ON leads (source_file_key);

CREATE TABLE rejections (
    file_key TEXT NOT NULL REFERENCES batches (file_key),
    workflow_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (file_key, row_number)
);

CREATE INDEX rejections_report_idx ON rejections (workflow_id, batch_id, row_number);

CREATE TABLE events (
    event_seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    call_id TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    phone_hmac TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX events_phone_idx ON events (workflow_id, phone_hmac);
CREATE INDEX events_call_idx ON events (workflow_id, call_id);
CREATE INDEX events_type_idx ON events (event_type);

CREATE TABLE event_links (
    event_id TEXT PRIMARY KEY REFERENCES events (event_id),
    lead_id BIGINT REFERENCES leads (lead_id),
    link_status TEXT NOT NULL CHECK (
        link_status IN ('linked', 'orphan', 'ignored_unknown_disposition')
    )
);

CREATE INDEX event_links_lead_idx ON event_links (lead_id);
CREATE INDEX event_links_status_idx ON event_links (link_status);

CREATE OR REPLACE FUNCTION reject_event_mutation() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    RAISE EXCEPTION 'events is append-only; updates and deletes are not allowed';
END;
$fn$;

CREATE TRIGGER events_append_only
BEFORE UPDATE OR DELETE ON events
FOR EACH ROW
EXECUTE FUNCTION reject_event_mutation();

CREATE TRIGGER events_no_truncate
BEFORE TRUNCATE ON events
FOR EACH STATEMENT
EXECUTE FUNCTION reject_event_mutation();

CREATE TABLE dispositions (
    workflow_id TEXT NOT NULL,
    code TEXT NOT NULL,
    label TEXT NOT NULL,
    is_terminal BOOLEAN NOT NULL,
    is_success BOOLEAN NOT NULL,
    PRIMARY KEY (workflow_id, code)
);

CREATE TABLE field_defaults (
    workflow_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    default_value TEXT NOT NULL,
    PRIMARY KEY (workflow_id, field_name)
);

CREATE TABLE quality_totals (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    lead_rows_total INTEGER NOT NULL DEFAULT 0,
    lead_rows_rejected INTEGER NOT NULL DEFAULT 0,
    duplicate_lead_rows_merged INTEGER NOT NULL DEFAULT 0,
    field_warnings INTEGER NOT NULL DEFAULT 0,
    events_total INTEGER NOT NULL DEFAULT 0,
    malformed_events INTEGER NOT NULL DEFAULT 0,
    duplicate_events_dropped INTEGER NOT NULL DEFAULT 0
);

INSERT INTO quality_totals (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

CREATE TABLE pii_key (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    fingerprint TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A call exists only when a linked call.initiated event exists.
-- Effective disposition prefers the latest linked override over any linked set.
-- Text ordering inside the view uses COLLATE "C" so tie-breaks ignore database locale.
CREATE OR REPLACE VIEW call_facts AS
WITH initiated AS (
    SELECT DISTINCT ON (event.workflow_id COLLATE "C", event.call_id COLLATE "C")
        event.workflow_id,
        event.call_id,
        link.lead_id,
        event.occurred_at,
        (event.occurred_at AT TIME ZONE 'Asia/Kolkata')::date AS call_date
    FROM events AS event
    JOIN event_links AS link ON link.event_id = event.event_id
    WHERE event.event_type = 'call.initiated'
      AND link.link_status = 'linked'
      AND event.call_id IS NOT NULL
      AND link.lead_id IS NOT NULL
    ORDER BY event.workflow_id COLLATE "C", event.call_id COLLATE "C", event.occurred_at, event.event_seq
),
connected AS (
    SELECT DISTINCT event.workflow_id, event.call_id
    FROM events AS event
    JOIN event_links AS link ON link.event_id = event.event_id
    WHERE event.event_type = 'call.connected'
      AND link.link_status = 'linked'
),
tel_failed AS (
    SELECT DISTINCT event.workflow_id, event.call_id
    FROM events AS event
    JOIN event_links AS link ON link.event_id = event.event_id
    WHERE event.event_type = 'call.telephony_failed'
      AND link.link_status = 'linked'
),
overrides AS (
    SELECT DISTINCT ON (event.workflow_id COLLATE "C", event.call_id COLLATE "C")
        event.workflow_id,
        event.call_id,
        event.payload->>'disposition_code' AS code
    FROM events AS event
    JOIN event_links AS link ON link.event_id = event.event_id
    WHERE event.event_type = 'disposition.override'
      AND link.link_status = 'linked'
    ORDER BY event.workflow_id COLLATE "C", event.call_id COLLATE "C", event.occurred_at DESC, event.event_seq DESC
),
sets AS (
    SELECT DISTINCT ON (event.workflow_id COLLATE "C", event.call_id COLLATE "C")
        event.workflow_id,
        event.call_id,
        event.payload->>'disposition_code' AS code
    FROM events AS event
    JOIN event_links AS link ON link.event_id = event.event_id
    WHERE event.event_type = 'disposition.set'
      AND link.link_status = 'linked'
    ORDER BY event.workflow_id COLLATE "C", event.call_id COLLATE "C", event.occurred_at DESC, event.event_seq DESC
)
SELECT
    i.workflow_id,
    i.call_id,
    i.lead_id,
    i.occurred_at,
    i.call_date,
    (c.call_id IS NOT NULL) AS connected,
    (t.call_id IS NOT NULL) AS telephony_failed,
    COALESCE(o.code, s.code) AS disposition_code,
    d.is_terminal,
    d.is_success
FROM initiated i
LEFT JOIN connected c USING (workflow_id, call_id)
LEFT JOIN tel_failed t USING (workflow_id, call_id)
LEFT JOIN overrides o USING (workflow_id, call_id)
LEFT JOIN sets s USING (workflow_id, call_id)
LEFT JOIN dispositions d
    ON d.workflow_id = i.workflow_id
   AND d.code = COALESCE(o.code, s.code);

CREATE OR REPLACE VIEW lead_status AS
SELECT
    l.lead_id,
    l.workflow_id,
    l.external_ref,
    l.batch_id,
    l.phone_ciphertext,
    l.phone_nonce,
    COALESCE(cs.total_calls, 0) AS total_calls,
    COALESCE(cs.connected_calls, 0) AS connected_calls,
    cs.last_call_at,
    cur.current_disposition,
    COALESCE(cs.ever_success, FALSE) AS ever_success,
    CASE
        WHEN COALESCE(cs.total_calls, 0) = 0 THEN 'yet_to_reach_out'
        WHEN COALESCE(cs.any_terminal, FALSE) THEN 'completed'
        WHEN COALESCE(cs.all_telephony_failed, FALSE) THEN 'in_progress_telephony_failed'
        ELSE 'in_progress_dialed'
    END AS bucket
FROM leads l
LEFT JOIN (
    SELECT
        lead_id,
        COUNT(*) AS total_calls,
        COUNT(*) FILTER (WHERE connected) AS connected_calls,
        MAX(occurred_at) AS last_call_at,
        BOOL_OR(COALESCE(is_success, FALSE)) AS ever_success,
        BOOL_OR(COALESCE(is_terminal, FALSE)) AS any_terminal,
        BOOL_AND(telephony_failed) AS all_telephony_failed
    FROM call_facts
    GROUP BY lead_id
) cs ON cs.lead_id = l.lead_id
LEFT JOIN (
    SELECT DISTINCT ON (lead_id)
        lead_id,
        disposition_code AS current_disposition
    FROM call_facts
    WHERE disposition_code IS NOT NULL
    ORDER BY lead_id, occurred_at DESC, call_id COLLATE "C" DESC
) cur ON cur.lead_id = l.lead_id;

-- Defaults stay out of the stored attributes so a later batch can still fill a null.
CREATE OR REPLACE VIEW leads_resolved AS
SELECT
    l.*,
    l.attributes || COALESCE((
        SELECT jsonb_object_agg(d.field_name, to_jsonb(d.default_value))
        FROM field_defaults d
        WHERE d.workflow_id = l.workflow_id
          AND NOT (l.attributes ? d.field_name)
    ), '{}'::jsonb) AS attributes_resolved
FROM leads l;
