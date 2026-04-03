CREATE TABLE IF NOT EXISTS {{qualified_queue_name}} (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'PENDING',
    is_retry BOOLEAN NOT NULL DEFAULT false,
    retries INTEGER NOT NULL DEFAULT 0,
    retry_policy JSONB,
    next_run_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dequeued_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_{{queue_name}}_created_at
    ON {{qualified_queue_name}} (created_at);

CREATE INDEX IF NOT EXISTS idx_{{queue_name}}_deleted_at
    ON {{qualified_queue_name}} (deleted_at);

CREATE INDEX IF NOT EXISTS idx_{{queue_name}}_dequeue
    ON {{qualified_queue_name}} (status, is_retry, deleted_at, created_at)
    WHERE deleted_at IS NULL;

WITH inserted_queue_meta AS (
    INSERT INTO {{qualified_queue_meta_name}} (queue_name)
    VALUES ('{{queue_name}}')
    ON CONFLICT (queue_name) DO NOTHING
    RETURNING queue_name
)
INSERT INTO {{qualified_queue_states_name}} (queue_name, state, reason)
SELECT queue_name, 'active', 'queue created'
FROM inserted_queue_meta;
