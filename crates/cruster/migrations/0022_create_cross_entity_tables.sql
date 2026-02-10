-- Migration for CrossEntity (new pure-RPC API)
-- Creates tables to store cross-entity messages and ping counts directly in PG

CREATE TABLE IF NOT EXISTS cross_entity_messages (
    id BIGSERIAL NOT NULL,
    entity_id TEXT NOT NULL,
    from_entity TEXT NOT NULL,
    content TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_cross_entity_messages_entity_id
    ON cross_entity_messages (entity_id);

CREATE TABLE IF NOT EXISTS cross_entity_ping_counts (
    entity_id TEXT NOT NULL PRIMARY KEY,
    ping_count INTEGER NOT NULL DEFAULT 0
);
