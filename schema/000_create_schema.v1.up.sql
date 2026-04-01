CREATE SCHEMA IF NOT EXISTS {{schema_name}};

CREATE TABLE IF NOT EXISTS {{qualified_migrations_name}} (
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    queue_name TEXT NOT NULL DEFAULT '',
    batch INTEGER NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (name, version, queue_name)
);

CREATE TABLE IF NOT EXISTS {{qualified_schema_versions_name}} (
    queue_name TEXT NOT NULL DEFAULT '' PRIMARY KEY,
    version TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {{qualified_queue_configs_name}} (
    queue_name TEXT PRIMARY KEY,
    retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
