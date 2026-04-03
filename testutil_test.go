package liteq

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestTask returns a Task with sensible defaults for unit tests.
func newTestTask(id string) Task {
	return Task{
		BaseQueueEntry: BaseQueueEntry{
			ID:      id,
			Data:    map[string]any{"key": "value"},
			Status:  string(PENDING),
			IsRetry: false,
			Retries: 0,
			RetryPolicy: &RetryPolicy{
				Strategy:     StrategyExponential,
				MaxRetries:   3,
				RetryDelayMs: 100,
				MaxDelayMs:   10000,
			},
		},
	}
}

// newFakePgQueue creates a PgQueue backed by a lazily-connecting pool.
// No real Postgres is needed; only methods that do NOT execute SQL are safe to call.
func newFakePgQueue(t *testing.T, name string) *PgQueue[Task] {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://fake:fake@localhost:5432/fake?connect_timeout=1")
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	q, err := NewPgQueue[Task](context.Background(), pool, name, &RetryPolicy{
		Strategy:     StrategyExponential,
		MaxRetries:   3,
		RetryDelayMs: 100,
		MaxDelayMs:   10000,
	})
	if err != nil {
		t.Fatalf("NewPgQueue: %v", err)
	}
	return q
}

// newTestWorker creates a Worker with two fake queues and a fixed retry policy selector.
func newTestWorker(t *testing.T) *Worker {
	t.Helper()
	q := newFakePgQueue(t, "queue_tasks")
	dlq := newFakePgQueue(t, "queue_tasks_dead_letter")
	w, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
		TaskBatchSize:   5,
		MaxConcurrency:  2,
		TaskTimeout:     100 * time.Millisecond,
		GetTaskRetryPolicy: func(task Task) (*RetryPolicy, error) {
			return task.RetryPolicy, nil
		},
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	return w
}

// fakeConsumer is a Consumer that returns a fixed error (or nil on success).
type fakeConsumer struct{ err error }

func (f *fakeConsumer) Consume(_ context.Context, _ Task) error { return f.err } //nolint:gocritic // test mock, value receiver matches Consumer interface

type mockQueue struct {
	label            string
	enqueueFn        func(Task, pgx.Tx) error
	dequeueFn        func(int) ([]Task, error)
	updateEntryFn    func(Task, pgx.Tx, ...Condition) error
	checkFn          func(context.Context, pgx.Tx, ...Condition) (bool, error)
	selectFn         func(context.Context, func(pgx.Rows) error, ...SelectMod) error
	selectOneFn      func(context.Context, func(pgx.Rows) error, ...SelectMod) (bool, error)
	updateStatusFn   func(context.Context, pgx.Tx, string, ...Condition) error
	getRetryPolicyFn func(context.Context) (*RetryPolicy, error)
	beginTxFn        func(context.Context) (pgx.Tx, context.Context, context.CancelFunc, error)
	pauseFn          func(context.Context) error
	resumeFn         func(context.Context) error
	isPausedFn       func(context.Context) (bool, error)
	drainFn          func(context.Context) error
}

func (m *mockQueue) Enqueue(item Task, tx pgx.Tx) error { //nolint:gocritic // test mock, value matches Queue interface
	if m.enqueueFn != nil {
		return m.enqueueFn(item, tx)
	}
	return nil
}

func (m *mockQueue) Dequeue(batchSize int) ([]Task, error) {
	if m.dequeueFn != nil {
		return m.dequeueFn(batchSize)
	}
	return nil, nil
}

func (m *mockQueue) UpdateEntry(item Task, tx pgx.Tx, conditions ...Condition) error { //nolint:gocritic // test mock, value matches Queue interface
	if m.updateEntryFn != nil {
		return m.updateEntryFn(item, tx, conditions...)
	}
	return nil
}

func (m *mockQueue) CheckCondition(ctx context.Context, tx pgx.Tx, conditions ...Condition) (bool, error) {
	if m.checkFn != nil {
		return m.checkFn(ctx, tx, conditions...)
	}
	return false, nil
}

func (m *mockQueue) Select(ctx context.Context, scan func(pgx.Rows) error, mods ...SelectMod) error {
	if m.selectFn != nil {
		return m.selectFn(ctx, scan, mods...)
	}
	return nil
}

func (m *mockQueue) SelectOne(ctx context.Context, scan func(pgx.Rows) error, mods ...SelectMod) (bool, error) {
	if m.selectOneFn != nil {
		return m.selectOneFn(ctx, scan, mods...)
	}
	return false, nil
}

func (m *mockQueue) UpdateStatus(ctx context.Context, tx pgx.Tx, status string, conditions ...Condition) error {
	if m.updateStatusFn != nil {
		return m.updateStatusFn(ctx, tx, status, conditions...)
	}
	return nil
}

func (m *mockQueue) GetRetryPolicy(ctx context.Context) (*RetryPolicy, error) {
	if m.getRetryPolicyFn != nil {
		return m.getRetryPolicyFn(ctx)
	}
	return &RetryPolicy{}, nil
}

func (m *mockQueue) BeginTx(ctx context.Context) (pgx.Tx, context.Context, context.CancelFunc, error) {
	if m.beginTxFn != nil {
		return m.beginTxFn(ctx)
	}
	return nil, ctx, func() {}, nil
}

func (m *mockQueue) QueueLabel() string {
	return m.label
}

func (m *mockQueue) Pause(ctx context.Context) error {
	if m.pauseFn != nil {
		return m.pauseFn(ctx)
	}
	return nil
}

func (m *mockQueue) Resume(ctx context.Context) error {
	if m.resumeFn != nil {
		return m.resumeFn(ctx)
	}
	return nil
}

func (m *mockQueue) IsPaused(ctx context.Context) (bool, error) {
	if m.isPausedFn != nil {
		return m.isPausedFn(ctx)
	}
	return false, nil
}

func (m *mockQueue) Drain(ctx context.Context) error {
	if m.drainFn != nil {
		return m.drainFn(ctx)
	}
	return nil
}
