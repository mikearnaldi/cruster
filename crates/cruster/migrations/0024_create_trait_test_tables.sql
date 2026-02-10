-- Migration for TraitTest (new pure-RPC API with RPC groups)
-- Creates tables to store trait test data, audit log, and versions directly in PG

CREATE TABLE IF NOT EXISTS trait_test_data (
    entity_id TEXT NOT NULL PRIMARY KEY,
    data TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS trait_test_audit_log (
    id BIGSERIAL NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details TEXT,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_trait_test_audit_log_entity_id
    ON trait_test_audit_log (entity_id);

CREATE TABLE IF NOT EXISTS trait_test_versions (
    entity_id TEXT NOT NULL PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 0
);
