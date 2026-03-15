package liteq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/im"
	"github.com/stephenafamo/bob/dialect/psql/sm"
	"github.com/stephenafamo/bob/dialect/psql/um"
)

type PgQueue[T interface{}] struct {
	BaseQueue
	Pool                   *pgxpool.Pool
	queueRetryPolicyMu     sync.Mutex
	cachedQueueRetryPolicy *RetryPolicy
}

func (q *PgQueue[T]) Enqueue(item T, tx pgx.Tx) error {
	entry, ok := interface{}(&item).(IQueueEntry)
	if !ok {
		return fmt.Errorf("item does not implement BaseQueueEntry")
	}

	query := psql.Insert(
		im.Into(q.QueueName, "data", "meta", "updated_at"),
		im.Values(psql.Arg(entry.GetBaseQueueEntry().Data, entry.GetBaseQueueEntry().Meta, psql.Raw("NOW()"))),
	)

	sql, args, err := query.Build(q.Ctx)
	log.Println(sql, args)
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
	tx, err := q.Pool.Begin(q.Ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start transaction: %w", err)
	}

	defer tx.Rollback(q.Ctx)

	rawQuery := `
		WITH claimed_tasks AS(
			SELECT *
			FROM %s
			WHERE (
				((meta->>'status')::text = 'PENDING' AND (meta->>'isRetry')::boolean = FALSE AND deleted_at IS NULL) OR -- NEW TASKS
				((meta->>'status')::text = 'PENDING' AND (meta->>'isRetry')::boolean = TRUE  AND (meta->>'nextRunAt')::timestamp <= NOW() AND deleted_at IS NULL) -- RETRY TASKS
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
	sql := fmt.Sprintf(rawQuery,
		psql.Quote(q.QueueName).String(),
		psql.Quote(q.QueueName).String(),
		psql.Quote(q.QueueName).String(),
		psql.Quote(q.QueueName).String(),
		psql.Quote(q.QueueName).String(),
	)

	rows, err := tx.Query(q.Ctx, sql, batchSize)
	if err != nil {
		return nil, fmt.Errorf("unable to claim tasks: %w", err)
	}
	defer rows.Close()

	tasks, err = pgx.CollectRows(rows, pgx.RowToStructByName[T])
	if err != nil {
		return nil, fmt.Errorf("unable to collect rows: %w", err)
	}

	tx.Commit(q.Ctx)
	return tasks, nil
}

func (q *PgQueue[T]) UpdateQueueEntryMeta(item T, tx pgx.Tx, conditions ...Condition) error {
	entry, ok := interface{}(&item).(IQueueEntry)
	if !ok {
		return fmt.Errorf("item does not implement GetBaseQueueEntry")
	}

	query := psql.Update(
		um.Table(q.QueueName),
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
		sm.From(q.QueueName),
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
	tx, err := q.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := psql.Select(append([]SelectMod{sm.From(q.QueueName)}, mods...)...)

	sql, args, err := query.Build(ctx)
	if err != nil {
		return fmt.Errorf("could not build select query: %w", err)
	}

	rows, err := tx.Query(ctx, sql, args...)
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
		um.Table(q.QueueName),
		um.SetCol("meta").To(psql.Raw(fmt.Sprintf(`jsonb_set(meta, '{status}', '"%s"', true)`, status))),
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

func NewPgQueue[T interface{}](ctx context.Context, pool *pgxpool.Pool, queueName string, retryPolicy *RetryPolicy) *PgQueue[T] {
	return &PgQueue[T]{
		BaseQueue: BaseQueue{
			Ctx:         ctx,
			QueueName:   queueName,
			RetryPolicy: retryPolicy,
		},
		Pool: pool,
	}
}

func (q *PgQueue[T]) GetRetryPolicy() (*RetryPolicy, error) {
	if q.RetryPolicy != nil {
		return q.RetryPolicy, nil
	}

	q.queueRetryPolicyMu.Lock()
	defer q.queueRetryPolicyMu.Unlock()

	if q.cachedQueueRetryPolicy != nil {
		return q.cachedQueueRetryPolicy, nil
	}

	var rawPolicy []byte
	err := q.Pool.QueryRow(
		q.Ctx,
		"SELECT retry_policy FROM queue_configs WHERE queue_name = $1 LIMIT 1",
		q.QueueName,
	).Scan(&rawPolicy)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("queue retry policy config not found; using zero-value fallback", "queueName", q.QueueName)
			return &RetryPolicy{}, nil
		}
		return nil, fmt.Errorf("could not load queue retry policy for %s: %w", q.QueueName, err)
	}

	var retryPolicy RetryPolicy
	if err := json.Unmarshal(rawPolicy, &retryPolicy); err != nil {
		return nil, fmt.Errorf("could not parse queue retry policy for %s: %w", q.QueueName, err)
	}

	q.cachedQueueRetryPolicy = &retryPolicy
	return q.cachedQueueRetryPolicy, nil
}
