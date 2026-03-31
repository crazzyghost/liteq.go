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
