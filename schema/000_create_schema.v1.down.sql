DROP TABLE IF EXISTS {{qualified_queue_states_name}};

DROP TABLE IF EXISTS {{qualified_queue_meta_name}};

DROP TABLE IF EXISTS {{qualified_schema_versions_name}};

DROP TABLE IF EXISTS {{qualified_migrations_name}};

DROP SCHEMA IF EXISTS {{schema_name}} CASCADE;
