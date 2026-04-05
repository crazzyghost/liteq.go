CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

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

CREATE TABLE IF NOT EXISTS {{qualified_queue_meta_name}} (
    queue_name TEXT PRIMARY KEY,
    retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'paused', 'draining')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {{qualified_queue_states_name}} (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue_name TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'paused', 'draining')),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_queue_states_queue
    ON {{qualified_queue_states_name}} (queue_name, changed_at DESC);
