-- Activity group integration test: order processing steps
CREATE TABLE IF NOT EXISTS activity_group_test_orders (
    order_id TEXT NOT NULL,
    step TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, step)
);
