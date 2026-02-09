-- Migration for ActivityTest entity (new pure-RPC + standalone workflow API)
-- Creates a table to store activity log records directly in PG

CREATE TABLE IF NOT EXISTS activity_test_logs (
    id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_id, id)
);

CREATE INDEX IF NOT EXISTS idx_activity_test_logs_entity_id
    ON activity_test_logs (entity_id);
