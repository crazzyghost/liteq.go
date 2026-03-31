package liteq

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestTask returns a Task with sensible defaults for unit tests.
func newTestTask(id string) Task {
	return Task{
		BaseQueueEntry: BaseQueueEntry{
			Id:   id,
			Data: map[string]any{"key": "value"},
			Meta: BaseQueueEntryMetaData{
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
	w, err := NewWorker(context.Background(), WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
		TaskBatchSize:   5,
		MaxConcurrency:  2,
		TaskTimeout:     100 * time.Millisecond,
		GetTaskRetryPolicy: func(task Task) (*RetryPolicy, error) {
			return task.Meta.RetryPolicy, nil
		},
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	return w
}

// fakeConsumer is a Consumer that returns a fixed error (or nil on success).
type fakeConsumer struct{ err error }

func (f *fakeConsumer) Consume(_ context.Context, _ Task) error { return f.err }

// blockingConsumer blocks until its context is cancelled.
type blockingConsumer struct{}

func (blockingConsumer) Consume(ctx context.Context, _ Task) error {
	<-ctx.Done()
	return ctx.Err()
}
