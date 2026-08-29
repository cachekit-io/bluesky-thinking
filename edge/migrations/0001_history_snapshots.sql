-- LAB-1616: aggregate-only snapshot history (docs/history.md).
-- One row per (operation, tier, bucket). The primary key IS the idempotency
-- guarantee: a re-fired capture for an already-recorded bucket is an
-- INSERT OR IGNORE no-op, so duplicate publishes cannot create duplicate
-- history. Absent rows are gaps — never zero-filled.
-- Payload-shape versioning is the migration history itself; a future shape
-- change adds a schema_version column (with a DEFAULT backfill) when — and
-- only when — a second shape exists. normalization_version is the version
-- that varies today, and it is stored per row.
CREATE TABLE snapshots (
  operation TEXT NOT NULL,
  tier TEXT NOT NULL CHECK (tier IN ('hourly', 'daily')),
  bucket_ts INTEGER NOT NULL, -- UTC epoch seconds, end of the covered bucket
  generated_at INTEGER NOT NULL, -- the ingester's aggregate generation time
  normalization_version TEXT NOT NULL, -- signal-policy.md semantics version
  payload TEXT NOT NULL, -- trimmed aggregate JSON (top-20, allowlisted keys)
  captured_at INTEGER NOT NULL, -- when the edge cron persisted the row
  PRIMARY KEY (operation, tier, bucket_ts)
);

-- Retention sweeps delete by (tier, bucket_ts) across all operations.
CREATE INDEX idx_snapshots_tier_bucket ON snapshots (tier, bucket_ts);
