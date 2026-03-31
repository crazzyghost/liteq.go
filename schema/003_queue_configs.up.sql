CREATE TABLE IF NOT EXISTS {{qualified_queue_configs_name}} (
    queue_name TEXT PRIMARY KEY,
    retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
