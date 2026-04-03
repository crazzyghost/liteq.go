DELETE FROM {{qualified_queue_states_name}} WHERE queue_name = '{{queue_name}}';

DELETE FROM {{qualified_queue_meta_name}} WHERE queue_name = '{{queue_name}}';

DROP INDEX IF EXISTS {{qualified_index_created_at_name}};
DROP INDEX IF EXISTS {{qualified_index_deleted_at_name}};
DROP INDEX IF EXISTS {{qualified_index_dequeue_name}};

DROP TABLE IF EXISTS {{qualified_queue_name}};
