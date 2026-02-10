-- Migration for StatelessCounter entity testing
-- Creates a table for the pure-RPC stateless counter to store values directly in PG

CREATE TABLE IF NOT EXISTS stateless_counter_values (
    entity_id TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);
