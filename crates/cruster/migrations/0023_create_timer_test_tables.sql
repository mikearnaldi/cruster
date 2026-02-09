-- Migration for TimerTest entity (new pure-RPC + standalone workflow API)
-- Creates tables to store timer state directly in PG

CREATE TABLE IF NOT EXISTS timer_test_pending (
    entity_id TEXT NOT NULL,
    timer_id TEXT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    delay_ms BIGINT NOT NULL,
    cancelled BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (entity_id, timer_id)
);

CREATE TABLE IF NOT EXISTS timer_test_fires (
    entity_id TEXT NOT NULL,
    timer_id TEXT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    fired_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (entity_id, timer_id)
);

CREATE INDEX IF NOT EXISTS idx_timer_test_fires_entity_id
    ON timer_test_fires (entity_id);
