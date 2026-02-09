-- Migration for SqlActivityTest entity (new stateless API)
-- Creates a state table to replace framework-managed entity state

CREATE TABLE IF NOT EXISTS sql_activity_test_state (
    entity_id TEXT PRIMARY KEY,
    transfer_count BIGINT NOT NULL DEFAULT 0,
    total_transferred BIGINT NOT NULL DEFAULT 0
);
