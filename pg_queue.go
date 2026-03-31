package liteq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/im"
	"github.com/stephenafamo/bob/dialect/psql/sm"
	"github.com/stephenafamo/bob/dialect/psql/um"
)

type PgQueue[T interface{}] struct {
	BaseQueue
	Pool            *pgxpool.Pool
	retryPolicyOnce sync.Once
	retryPolicyVal  *RetryPolicy
	retryPolicyErr  error
}

// rollback executes a transaction rollback, suppressing pgx.ErrTxClosed which
// is expected after a successful Commit, and logging any other error.
func rollback(tx pgx.Tx) {
	tx.Rollback(context.Background()) //nolint:errcheck
}

// beginTx creates a context-scoped transaction with the queue's TxTimeout.
// The caller must defer the returned cancel to avoid a context leak.
func (q *PgQueue[T]) beginTx(ctx context.Context) (pgx.Tx, context.Context, context.CancelFunc, error) {
	txCtx, cancel := context.WithTimeout(ctx, q.TxTimeout)
	tx, err := q.Pool.Begin(txCtx)
	if err != nil {
		cancel()
		return nil, nil, nil, fmt.Errorf("begin tx: %w", err)
	}
	return tx, txCtx, cancel, nil
}

func (q *PgQueue[T]) queueTable() string {
	return qualifyIdentifier(q.Schema, q.QueueName)
}

func (q *PgQueue[T]) queueConfigsTable() string {
	return qualifyIdentifier(q.Schema, "queue_configs")
}

func (q *PgQueue[T]) Enqueue(item T, tx pgx.Tx) error {
	entry, ok := interface{}(&item).(IQueueEntry)
	if !ok {
		return fmt.Errorf("item does not implement BaseQueueEntry")
	}

	query := psql.Insert(
		im.Into(q.queueTable(), "id", "data", "meta", "updated_at"),
		im.Values(psql.Arg(
			entry.GetBaseQueueEntry().Id,
			entry.GetBaseQueueEntry().Data,
			entry.GetBaseQueueEntry().Meta,
			psql.Raw("NOW()"),
		)),
	)

	sql, args, err := query.Build(q.Ctx)
	if err != nil {
		return fmt.Errorf("could not build enqueue query: %w", err)
	}

	_, err = tx.Exec(q.Ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("could not enqueue entry: %w", err)
	}

	return nil
}

func (q *PgQueue[T]) Dequeue(batchSize int) (tasks []T, err error) {
	tx, txCtx, cancel, err := q.beginTx(q.Ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start transaction: %w", err)
	}
	defer cancel()
	defer rollback(tx)

	table := q.queueTable()
	rawQuery := `
		WITH claimed_tasks AS(
			SELECT *
			FROM %s
			WHERE (
				((meta->>'status')::text = 'PENDING' AND (meta->>'isRetry')::boolean = FALSE AND deleted_at IS NULL) OR
				((meta->>'status')::text = 'PENDING' AND (meta->>'isRetry')::boolean = TRUE  AND (meta->>'nextRunAt')::timestamp <= NOW() AND deleted_at IS NULL)
			)
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED LIMIT $1
		)
		UPDATE %s
        SET meta = jsonb_set(%s.meta, '{status}','"RUNNING"', true), dequeued_at = NOW(), updated_at = NOW()
		FROM claimed_tasks
		WHERE %s.id = claimed_tasks.id
		RETURNING %s.*
	`
	sql := fmt.Sprintf(rawQuery, table, table, table, table, table)

	rows, err := tx.Query(txCtx, sql, batchSize)
	if err != nil {
		return nil, fmt.Errorf("unable to claim tasks: %w", err)
	}
	defer rows.Close()

	tasks, err = pgx.CollectRows(rows, pgx.RowToStructByName[T])
	if err != nil {
		return nil, fmt.Errorf("unable to collect rows: %w", err)
	}

	if err := tx.Commit(txCtx); err != nil {
		return nil, fmt.Errorf("dequeue commit: %w", err)
	}
	return tasks, nil
}

func (q *PgQueue[T]) UpdateQueueEntryMeta(item T, tx pgx.Tx, conditions ...Condition) error {
	entry, ok := interface{}(&item).(IQueueEntry)
	if !ok {
		return fmt.Errorf("item does not implement GetBaseQueueEntry")
	}

	query := psql.Update(
		um.Table(q.queueTable()),
		um.SetCol("meta").ToArg(entry.GetBaseQueueEntry().Meta),
		um.SetCol("updated_at").To(psql.Raw("NOW()")),
	)

	IDEquals(entry.GetBaseQueueEntry().Id).ApplyToUpdate(query)

	for _, cond := range conditions {
		cond.ApplyToUpdate(query)
	}

	sql, args, err := query.Build(q.Ctx)
	if err != nil {
		return fmt.Errorf("could not build update query: %w", err)
	}

	_, err = tx.Exec(q.Ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("could not update entry status: %w", err)
	}
	return nil
}

func (q *PgQueue[T]) CheckCondition(ctx context.Context, tx pgx.Tx, conditions ...Condition) (bool, error) {
	query := psql.Select(
		sm.From(q.queueTable()),
		sm.Columns("1"),
		sm.Limit(1),
	)

	for _, cond := range conditions {
		cond.ApplyToSelect(query)
	}

	sql, args, err := query.Build(ctx)
	if err != nil {
		return false, fmt.Errorf("could not build check query: %w", err)
	}

	var exists int
	err = tx.QueryRow(ctx, sql, args...).Scan(&exists)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("could not check condition: %w", err)
	}

	return true, nil
}

// Select builds and executes a SELECT against the queue table.
// sm.From is always prepended automatically so callers don't need it.
// scan is called once per row; returning an error stops iteration immediately.
// All filtering, ordering, and column selection is expressed as SelectMods
// (e.g. sm.Where, sm.OrderBy, sm.Limit, sm.Columns).
func (q *PgQueue[T]) Select(
	ctx context.Context,
	scan func(pgx.Rows) error,
	mods ...SelectMod,
) error {
	tx, txCtx, cancel, err := q.beginTx(ctx)
	if err != nil {
		return fmt.Errorf("unable to start transaction: %w", err)
	}
	defer cancel()
	defer rollback(tx)

	query := psql.Select(append([]SelectMod{sm.From(q.queueTable())}, mods...)...)

	sql, args, err := query.Build(txCtx)
	if err != nil {
		return fmt.Errorf("could not build select query: %w", err)
	}

	rows, err := tx.Query(txCtx, sql, args...)
	if err != nil {
		return fmt.Errorf("could not execute select query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}

	return rows.Err()
}

// SelectOne executes a SELECT and calls scan for the first matching row.
// Returns (true, nil) when a row was found, (false, nil) when none matched.
// scan receives a pgx.Rows already positioned on the row — call rows.Scan()
// directly inside it.
func (q *PgQueue[T]) SelectOne(
	ctx context.Context,
	scan func(pgx.Rows) error,
	mods ...SelectMod,
) (bool, error) {
	found := false
	err := q.Select(
		ctx,
		func(rows pgx.Rows) error {
			found = true
			return scan(rows)
		},
		mods...,
	)
	return found, err
}

func (q *PgQueue[T]) UpdateStatus(ctx context.Context, tx pgx.Tx, status string, conditions ...Condition) error {
	query := psql.Update(
		um.Table(q.queueTable()),
		um.SetCol("meta").To(psql.Raw("jsonb_set(meta, '{status}', to_jsonb(?::text), true)", status)),
		um.SetCol("updated_at").To(psql.Raw("NOW()")),
	)

	for _, cond := range conditions {
		cond.ApplyToUpdate(query)
	}

	sql, args, err := query.Build(ctx)
	if err != nil {
		return fmt.Errorf("could not build update status query: %w", err)
	}

	_, err = tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("could not update entry status: %w", err)
	}

	return nil
}

// NewPgQueue validates its inputs and constructs a PgQueue. A non-nil error is
// returned when any required argument is missing or the retry policy is invalid.
func NewPgQueue[T interface{}](ctx context.Context, pool *pgxpool.Pool, queueName string, retryPolicy *RetryPolicy, opts ...PgQueueOption) (*PgQueue[T], error) {
	if ctx == nil {
		return nil, fmt.Errorf("liteq: NewPgQueue: ctx must not be nil")
	}
	if pool == nil {
		return nil, fmt.Errorf("liteq: NewPgQueue: pool must not be nil")
	}
	if queueName == "" {
		return nil, fmt.Errorf("liteq: NewPgQueue: name must not be empty")
	}
	if err := validateIdentifier("queue name", queueName); err != nil {
		return nil, fmt.Errorf("liteq: NewPgQueue: %w", err)
	}

	cfg := newPgQueueConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	if cfg.schema != "" {
		if err := validateIdentifier("schema", cfg.schema); err != nil {
			return nil, fmt.Errorf("liteq: NewPgQueue: %w", err)
		}
	}
	if cfg.dlqName != "" {
		if err := validateIdentifier("dlq name", cfg.dlqName); err != nil {
			return nil, fmt.Errorf("liteq: NewPgQueue: %w", err)
		}
	}

	if retryPolicy != nil {
		if retryPolicy.MaxRetries < 0 {
			return nil, fmt.Errorf("liteq: NewPgQueue: retryPolicy.MaxRetries must be >= 0")
		}
		if retryPolicy.RetryDelayMs <= 0 {
			return nil, fmt.Errorf("liteq: NewPgQueue: retryPolicy.RetryDelayMs must be > 0")
		}
		canonical, err := ParseRetryStrategy(retryPolicy.Strategy)
		if err != nil {
			return nil, fmt.Errorf("liteq: NewPgQueue: retryPolicy.Strategy: %w", err)
		}
		retryPolicy.Strategy = canonical
	}

	if cfg.autoMigrate {
		sm := NewSchemaManager(pool, WithSchemaManagerSchema(cfg.schema))
		if err := sm.EnsureQueue(ctx, queueName, cfg.dlqName); err != nil {
			return nil, fmt.Errorf("liteq: NewPgQueue: auto-migrate: %w", err)
		}
	}

	return &PgQueue[T]{
		BaseQueue: BaseQueue{
			Ctx:         ctx,
			Schema:      cfg.schema,
			QueueName:   queueName,
			RetryPolicy: retryPolicy,
			TxTimeout:   5 * time.Second,
		},
		Pool: pool,
	}, nil
}

func (q *PgQueue[T]) GetRetryPolicy(ctx context.Context) (*RetryPolicy, error) {
	if q.RetryPolicy != nil {
		return q.RetryPolicy, nil
	}
	q.retryPolicyOnce.Do(func() {
		q.retryPolicyVal, q.retryPolicyErr = q.loadRetryPolicy(ctx)
	})
	return q.retryPolicyVal, q.retryPolicyErr
}

func (q *PgQueue[T]) loadRetryPolicy(ctx context.Context) (*RetryPolicy, error) {
	var rawPolicy []byte
	err := q.Pool.QueryRow(
		ctx,
		fmt.Sprintf("SELECT retry_policy FROM %s WHERE queue_name = $1 LIMIT 1", q.queueConfigsTable()),
		q.QueueName,
	).Scan(&rawPolicy)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &RetryPolicy{}, nil
		}
		return nil, fmt.Errorf("could not load queue retry policy for %s: %w", q.QueueName, err)
	}

	var retryPolicy RetryPolicy
	if err := json.Unmarshal(rawPolicy, &retryPolicy); err != nil {
		return nil, fmt.Errorf("could not parse queue retry policy for %s: %w", q.QueueName, err)
	}

	return &retryPolicy, nil
}
