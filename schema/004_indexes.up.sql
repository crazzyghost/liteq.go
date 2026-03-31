CREATE INDEX IF NOT EXISTS idx_{{queue_name}}_created_at
    ON {{qualified_queue_name}} (created_at);

CREATE INDEX IF NOT EXISTS idx_{{queue_name}}_deleted_at
    ON {{qualified_queue_name}} (deleted_at);

CREATE INDEX IF NOT EXISTS idx_{{queue_name}}_dequeue_pending
    ON {{qualified_queue_name}} (((meta->>'status')), ((meta->>'isRetry')), ((meta->>'nextRunAt')), created_at)
    WHERE deleted_at IS NULL;
