-- Migration for WorkflowTest entity (new pure-RPC + standalone workflow API)
-- Creates a table to store workflow execution records directly in PG

CREATE TABLE IF NOT EXISTS workflow_test_executions (
    id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    steps_completed TEXT[] NOT NULL DEFAULT '{}',
    result TEXT,
    PRIMARY KEY (entity_id, id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_test_executions_entity_id
    ON workflow_test_executions (entity_id);
