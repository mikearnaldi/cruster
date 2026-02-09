-- Migration for Counter entity (new pure-RPC API)
-- Creates a table for the counter entity to store values directly in PG

CREATE TABLE IF NOT EXISTS counter_values (
    entity_id TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);
