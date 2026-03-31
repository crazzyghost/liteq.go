CREATE INDEX IF NOT EXISTS idx_{{queue_name}}_created_at
    ON {{qualified_queue_name}} (created_at);

CREATE INDEX IF NOT EXISTS idx_{{queue_name}}_deleted_at
    ON {{qualified_queue_name}} (deleted_at);

CREATE INDEX IF NOT EXISTS idx_{{queue_name}}_dequeue
    ON {{qualified_queue_name}} (status, is_retry, deleted_at, created_at)
    WHERE deleted_at IS NULL;
