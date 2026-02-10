-- KVStore entity state table (used by examples/cluster-tests KVStore entity).
-- Each row is a key-value pair scoped to an entity_id.

CREATE TABLE IF NOT EXISTS kv_store_entries (
    entity_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value BYTEA NOT NULL,
    PRIMARY KEY (entity_id, key)
);
