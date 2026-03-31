CREATE TABLE IF NOT EXISTS {{qualified_schema_versions_name}} (
    version TEXT NOT NULL,
    queue_name TEXT NOT NULL DEFAULT '',
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (version, queue_name)
);
