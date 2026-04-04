# SQL Operation Contracts

This document captures the canonical SQL operations that make up the liteq
queue protocol. The Go `PgQueue` type is the reference implementation, but
any compliant client can implement the same operations with its own Postgres
driver.

## Shared Conventions

- Queue tables use the schema from `schema/001_create_queue_table.v1.up.sql`.
- Foundation schema (migrations tracking, queue_meta, queue_states) is defined in 
  `schema/000_create_schema.v1.up.sql`.
- `{schema}` and `{queue}` are validated identifiers, not user-provided SQL.
- Parameter placeholders are shown in Postgres `$1`, `$2`, ... form.
- `READ COMMITTED` is sufficient for queue claims because `FOR UPDATE SKIP LOCKED`
  prevents double-claiming.
- The canonical row shape returned by read operations is:

```text
id TEXT
data JSONB
status TEXT
is_retry BOOLEAN
retries INTEGER
retry_policy JSONB
next_run_at TIMESTAMPTZ
last_run_at TIMESTAMPTZ
processed_at TIMESTAMPTZ
enqueued_at TIMESTAMPTZ
dequeued_at TIMESTAMPTZ
created_at TIMESTAMPTZ
updated_at TIMESTAMPTZ
deleted_at TIMESTAMPTZ
```

## Operation Summary

| Operation | Transaction required | Purpose |
| --- | --- | --- |
| `enqueue` | Yes | Insert one queue row. |
| `enqueue_many` | Yes | Insert many queue rows atomically. |
| `dequeue` | Yes | Claim and return the next runnable batch. |
| `update_status` | Optional | Update only `status` and `updated_at`. |
| `update_entry` | Optional | Update lifecycle and retry fields for a row. |
| `check_condition` | No | Check whether any row satisfies a predicate. |
| `select` | No | Query rows with arbitrary filters, ordering, and limit. |
| `get_retry_policy` | No | Fetch queue-level retry policy from `queue_meta`. |
| `pause_queue` | Yes | Set queue state to `paused` and audit change. |
| `resume_queue` | Yes | Set queue state to `active` and audit change. |
| `is_paused` | No | Check current queue state. |
| `drain_queue` | Yes | Soft-delete all entries and set state to `draining`. |

## 1. `enqueue`

```sql
INSERT INTO {schema}.{queue} (
    id,
    data,
    status,
    is_retry,
    retries,
    retry_policy,
    next_run_at,
    last_run_at,
    processed_at,
    enqueued_at,
    updated_at
) VALUES (
    $1,  -- text
    $2,  -- jsonb
    $3,  -- text
    $4,  -- boolean
    $5,  -- integer
    $6,  -- jsonb|null
    $7,  -- timestamptz|null
    $8,  -- timestamptz|null
    $9,  -- timestamptz|null
    NOW(),
    NOW()
);
```

- Result shape: no rows; success is the affected-row count.
- Errors: duplicate primary key, invalid JSONB, transaction/connection failure.
- Go reference: `PgQueue.Enqueue`.

## 2. `enqueue_many`

`enqueue_many` is the batch form of `enqueue`. The Go `PgQueue` implementation does 
not currently expose a public `EnqueueMany` method—applications must loop over 
`Enqueue` calls within a transaction. However, the protocol contract supports batching 
so other language clients can implement batch inserts to avoid N round trips.

```sql
INSERT INTO {schema}.{queue} (
    id, data, status, is_retry, retries, retry_policy,
    next_run_at, last_run_at, processed_at, enqueued_at, updated_at
) VALUES
    ($1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,  $9,  NOW(), NOW()),
    ($10, $11, $12, $13, $14, $15, $16, $17, $18, NOW(), NOW());
```

- Parameter types repeat the `enqueue` tuple shape per row.
- Transaction requirement: all rows must succeed or the batch rolls back.
- Errors: same as `enqueue`, plus partial-batch failure if any row violates a
  constraint.

## 3. `dequeue`

The Go implementation uses a single `WITH ... UPDATE ... RETURNING` statement to
claim work atomically.

```sql
WITH claimed_tasks AS (
    SELECT *
      FROM {schema}.{queue}
     WHERE status = 'PENDING'
       AND deleted_at IS NULL
       AND (is_retry = FALSE OR (is_retry = TRUE AND next_run_at <= NOW()))
     ORDER BY created_at ASC
     FOR UPDATE SKIP LOCKED
     LIMIT $1  -- integer batch size
)
UPDATE {schema}.{queue}
   SET status = 'RUNNING',
       dequeued_at = NOW(),
       updated_at = NOW()
  FROM claimed_tasks
 WHERE {schema}.{queue}.id = claimed_tasks.id
RETURNING {schema}.{queue}.id,
          {schema}.{queue}.data,
          {schema}.{queue}.status,
          {schema}.{queue}.is_retry,
          {schema}.{queue}.retries,
          {schema}.{queue}.retry_policy,
          {schema}.{queue}.next_run_at,
          {schema}.{queue}.last_run_at,
          {schema}.{queue}.processed_at,
          {schema}.{queue}.enqueued_at,
          {schema}.{queue}.dequeued_at,
          {schema}.{queue}.created_at,
          {schema}.{queue}.updated_at,
          {schema}.{queue}.deleted_at;
```

- Result shape: zero or more queue rows.
- Transaction requirement: required.
- Errors: lock/transaction failure, scan/mapping failure, connection failure.
- Go reference: `PgQueue.Dequeue`.

## 4. `update_status`

```sql
UPDATE {schema}.{queue}
   SET status = $1,      -- text
       updated_at = NOW()
 WHERE id = $2;          -- text
```

- Additional predicates may be appended for guarded updates.
- Result shape: no rows; caller may inspect affected-row count.
- Transaction requirement: optional, but recommended when coordinated with other
  writes.
- Errors: invalid status value by client convention, transaction failure.
- Go reference: `PgQueue.UpdateStatus`.

## 5. `update_entry`

```sql
UPDATE {schema}.{queue}
   SET status = $1,          -- text
       is_retry = $2,        -- boolean
       retries = $3,         -- integer
       retry_policy = $4,    -- jsonb|null
       next_run_at = $5,     -- timestamptz|null
       last_run_at = $6,     -- timestamptz|null
       processed_at = $7,    -- timestamptz|null
       updated_at = NOW()
 WHERE id = $8;              -- text
```

- Additional conditions may be appended to the `WHERE` clause.
- Result shape: no rows.
- Transaction requirement: optional, but required when coordinated with status
  changes, retries, or DLQ writes.
- Errors: invalid JSONB, connection failure, guarded update affecting zero rows.
- Go reference: `PgQueue.UpdateEntry`.

## 6. `check_condition`

```sql
SELECT EXISTS (
    SELECT 1
      FROM {schema}.{queue}
     WHERE /* caller-supplied predicate */
     LIMIT 1
);
```

- Parameter types depend on the predicate.
- Result shape: one boolean.
- Transaction requirement: not required. The Go reference can run this inside an
  existing transaction when the caller needs a consistent write flow.
- Errors: invalid predicate construction, connection failure.
- Go reference: `PgQueue.CheckCondition`.

## 7. `select`

```sql
SELECT id, data, status, is_retry, retries, retry_policy,
       next_run_at, last_run_at, processed_at, enqueued_at,
       dequeued_at, created_at, updated_at, deleted_at
  FROM {schema}.{queue}
 WHERE /* caller-supplied predicate */
 ORDER BY created_at ASC
 LIMIT $1;  -- integer
```

- `ORDER BY` is a validated SQL fragment or query-builder construct, not a bound
  parameter.
- Result shape: zero or more queue rows.
- Transaction requirement: not required.
- Errors: invalid query construction, scan failure, connection failure.
- Go reference: `PgQueue.Select` and `PgQueue.SelectOne`.

## 8. `get_retry_policy`

```sql
SELECT retry_policy
  FROM {schema}.queue_meta
 WHERE queue_name = $1   -- text
 LIMIT 1;
```

- Result shape: one JSONB document or no rows.
- Missing-row behavior: the Go reference treats it as an empty retry policy.
- Transaction requirement: not required.
- Errors: invalid JSON payload in storage, connection failure.
- Go reference: `PgQueue.GetRetryPolicy`.

## 9. Queue State Operations

Queue state operations manage the operational status of a queue (`active`, `paused`, 
or `draining`). These operations interact with the `queue_meta` and `queue_states` 
tables defined in `schema/000_create_schema.v1.up.sql`.

For queue state semantics, worker behavior, and lifecycle hooks, see 
[`queue-lifecycle.md`](./queue-lifecycle.md).

### `pause_queue`

```sql
-- Update queue state
UPDATE {schema}.queue_meta
   SET state = 'paused',
       updated_at = NOW()
 WHERE queue_name = $1;  -- text

-- Record state change
INSERT INTO {schema}.queue_states (queue_name, state, reason)
VALUES ($1, 'paused', $2);  -- text, text|null
```

- Transaction requirement: required (ensures atomic state change + audit log).
- Result shape: no rows; caller may inspect affected-row count.
- Go reference: `PgQueue.Pause`.

### `resume_queue`

```sql
-- Update queue state
UPDATE {schema}.queue_meta
   SET state = 'active',
       updated_at = NOW()
 WHERE queue_name = $1;  -- text

-- Record state change
INSERT INTO {schema}.queue_states (queue_name, state, reason)
VALUES ($1, 'active', $2);  -- text, text|null
```

- Transaction requirement: required.
- Go reference: `PgQueue.Resume`.

### `is_paused`

```sql
SELECT state
  FROM {schema}.queue_meta
 WHERE queue_name = $1  -- text
 LIMIT 1;
```

- Result shape: one TEXT value (`active`, `paused`, or `draining`) or no rows.
- Transaction requirement: not required.
- Go reference: `PgQueue.IsPaused` (returns boolean).

### `drain_queue`

```sql
-- Delete all entries
UPDATE {schema}.{queue}
   SET deleted_at = NOW(),
       updated_at = NOW()
 WHERE deleted_at IS NULL;

-- Pause the queue
UPDATE {schema}.queue_meta
   SET state = 'draining',
       updated_at = NOW()
 WHERE queue_name = $1;  -- text

-- Record state change
INSERT INTO {schema}.queue_states (queue_name, state, reason)
VALUES ($1, 'draining', $2);  -- text, text|null
```

- Transaction requirement: required (ensures atomic soft-delete + state change).
- Result shape: no rows.
- Go reference: `PgQueue.Drain`.

### Queue State Schema

The `queue_meta` table tracks queue-level configuration and operational state:

```sql
CREATE TABLE {schema}.queue_meta (
    queue_name TEXT PRIMARY KEY,
    retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL DEFAULT 'active' 
        CHECK (state IN ('active', 'paused', 'draining')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

The `queue_states` table provides an audit log of state transitions:

```sql
CREATE TABLE {schema}.queue_states (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue_name TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'paused', 'draining')),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason TEXT
);
```

## Error-Handling Contract

Clients should preserve the distinction between:

- SQL construction errors
- execution/connection errors
- scan/deserialization errors
- business-state errors handled at the worker layer, such as max retries

The Go reference wraps these failures with operation-specific context (for
example, `could not build enqueue query` or `dequeue commit`), and other client
libraries should provide similarly specific error boundaries.
