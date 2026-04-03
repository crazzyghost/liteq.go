package liteq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---- ParseRetryStrategy ----

func TestParseRetryStrategy(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"", StrategyExponential, false},
		{"exponential", StrategyExponential, false},
		{"fixed", StrategyFixed, false},
		{"linear", StrategyLinear, false},
		{"unknown", "", true},
		{"FIXED", "", true},
		{"Exponential", "", true},
	}
	for _, tc := range tests {
		got, err := ParseRetryStrategy(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseRetryStrategy(%q) expected error, got nil", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseRetryStrategy(%q) unexpected error: %v", tc.input, err)
		}
		if got != tc.want {
			t.Errorf("ParseRetryStrategy(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// ---- NewPgQueue validation ----

func TestNewPgQueue_NilCtx(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	_, err := NewPgQueue[Task](nil, q.Pool, "q", nil) //nolint:staticcheck // deliberately testing nil ctx
	if err == nil {
		t.Error("expected error for nil ctx")
	}
}

func TestNewPgQueue_NilPool(t *testing.T) {
	_, err := NewPgQueue[Task](context.Background(), nil, "queue_tasks", nil)
	if err == nil {
		t.Error("expected error for nil pool")
	}
}

func TestNewPgQueue_EmptyName(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	_, err := NewPgQueue[Task](context.Background(), q.Pool, "", nil)
	if err == nil {
		t.Error("expected error for empty queue name")
	}
}

func TestNewPgQueue_NilRetryPolicy_OK(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	got, err := NewPgQueue[Task](context.Background(), q.Pool, "queue_tasks", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.RetryPolicy != nil {
		t.Error("RetryPolicy should be nil when not provided")
	}
}

func TestNewPgQueue_NegativeMaxRetries(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	_, err := NewPgQueue[Task](context.Background(), q.Pool, "q", &RetryPolicy{
		MaxRetries:   -1,
		RetryDelayMs: 100,
		Strategy:     StrategyFixed,
	})
	if err == nil {
		t.Error("expected error for negative MaxRetries")
	}
}

func TestNewPgQueue_ZeroRetryDelayMs(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	_, err := NewPgQueue[Task](context.Background(), q.Pool, "q", &RetryPolicy{
		MaxRetries:   3,
		RetryDelayMs: 0,
		Strategy:     StrategyFixed,
	})
	if err == nil {
		t.Error("expected error for zero RetryDelayMs")
	}
}

func TestNewPgQueue_InvalidStrategy(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	_, err := NewPgQueue[Task](context.Background(), q.Pool, "q", &RetryPolicy{
		MaxRetries:   3,
		RetryDelayMs: 100,
		Strategy:     "badstrategy",
	})
	if err == nil {
		t.Error("expected error for invalid retry strategy")
	}
}

func TestNewPgQueue_NormalisesEmptyStrategy(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	got, err := NewPgQueue[Task](context.Background(), q.Pool, "queue_tasks", &RetryPolicy{
		MaxRetries:   1,
		RetryDelayMs: 50,
		Strategy:     "", // empty → exponential
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.RetryPolicy.Strategy != StrategyExponential {
		t.Errorf("Strategy = %q, want %q", got.RetryPolicy.Strategy, StrategyExponential)
	}
}

func TestNewPgQueue_DefaultTxTimeout(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	got, err := NewPgQueue[Task](context.Background(), q.Pool, "queue_tasks", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.TxTimeout != 5*time.Second {
		t.Errorf("TxTimeout = %v, want 5s", got.TxTimeout)
	}
}

func TestNewPgQueue_DefaultSchema(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	got, err := NewPgQueue[Task](context.Background(), q.Pool, "queue_tasks", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Schema != defaultSchema {
		t.Fatalf("Schema = %q, want %q", got.Schema, defaultSchema)
	}
	if got.QualifiedQueueName() != "liteq.queue_tasks" {
		t.Fatalf("QualifiedQueueName() = %q, want %q", got.QualifiedQueueName(), "liteq.queue_tasks")
	}
}

func TestNewPgQueue_WithSchemaOptOut(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	got, err := NewPgQueue[Task](context.Background(), q.Pool, "queue_tasks", nil, WithSchema(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Schema != "" {
		t.Fatalf("Schema = %q, want empty string", got.Schema)
	}
	if got.QualifiedQueueName() != "queue_tasks" {
		t.Fatalf("QualifiedQueueName() = %q, want %q", got.QualifiedQueueName(), "queue_tasks")
	}
}

func TestNewPgQueue_InvalidSchema(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	_, err := NewPgQueue[Task](context.Background(), q.Pool, "queue_tasks", nil, WithSchema("bad-schema"))
	if err == nil {
		t.Fatal("expected error for invalid schema")
	}
}

func TestDefaultDeadLetterQueueName(t *testing.T) {
	if got := defaultDeadLetterQueueName("queue_tasks"); got != "queue_tasks_dead_letter" {
		t.Fatalf("defaultDeadLetterQueueName(queue_tasks) = %q", got)
	}
	if got := defaultDeadLetterQueueName("queue_tasks_dead_letter"); got != "" {
		t.Fatalf("defaultDeadLetterQueueName(queue_tasks_dead_letter) = %q, want empty string", got)
	}
}

// ---- GetRetryPolicy (static / in-memory) ----

func TestGetRetryPolicy_StaticPolicy(t *testing.T) {
	policy := &RetryPolicy{Strategy: StrategyFixed, MaxRetries: 2, RetryDelayMs: 200, MaxDelayMs: 1000}
	q := newFakePgQueue(t, "queue_tasks")
	q.RetryPolicy = policy

	got, err := q.GetRetryPolicy(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != policy {
		t.Error("GetRetryPolicy should return the in-memory policy as-is")
	}
}

// ---- WillExceedMaxRetries ----

func TestWillExceedMaxRetries(t *testing.T) {
	tests := []struct {
		retries    int
		maxRetries int
		want       bool
	}{
		{0, 3, false}, // 1st attempt, 3 allowed
		{2, 3, false}, // 3rd attempt, still within
		{3, 3, true},  // 4th attempt would exceed
		{0, 0, true},  // no retries allowed
		{5, 3, true},  // already exceeded
	}
	for _, tc := range tests {
		task := newTestTask("t")
		task.Retries = tc.retries
		got := task.WillExceedMaxRetries(tc.maxRetries)
		if got != tc.want {
			t.Errorf("WillExceedMaxRetries(retries=%d, max=%d) = %v, want %v",
				tc.retries, tc.maxRetries, got, tc.want)
		}
	}
}

// ---- TaskStatus constants ----

func TestTaskStatusValues(t *testing.T) {
	tests := []struct {
		status TaskStatus
		want   string
	}{
		{PENDING, "PENDING"},
		{RUNNING, "RUNNING"},
		{FAILED, "FAILED"},
		{COMPLETED, "COMPLETED"},
		{CANCELLED, "CANCELLED"}, //nolint:misspell // CANCELLED is the stored DB value
		{DLQFailed, "DLQ_FAILED"},
	}
	for _, tc := range tests {
		if string(tc.status) != tc.want {
			t.Errorf("TaskStatus(%q) = %q, want %q", tc.status, string(tc.status), tc.want)
		}
	}
}

// ---- BaseQueueEntry ----

func TestBaseQueueEntry_GetBaseQueueEntry(t *testing.T) {
	task := newTestTask("id-1")
	entry := task.GetBaseQueueEntry()
	if entry == nil {
		t.Fatal("GetBaseQueueEntry() returned nil")
	}
	if entry.ID != "id-1" {
		t.Errorf("entry.ID = %q, want %q", entry.ID, "id-1")
	}
}

// ---- fmt.Errorf wrapping (ConsumerError path) ----

func TestConsumerError_NonTransient_FlagSet(t *testing.T) {
	ce := &ConsumerError{
		Source:         fmt.Errorf("bad payload"),
		IsNonTransient: true,
	}
	if !ce.IsNonTransient {
		t.Error("IsNonTransient should be true")
	}
}

// ---- BaseQueueEntry.GetBaseQueueEntry (queue.go) ----

func TestBaseQueueEntry_GetBaseQueueEntry_Direct(t *testing.T) {
	entry := &BaseQueueEntry{ID: "direct-id"}
	got := entry.GetBaseQueueEntry()
	if got == nil {
		t.Fatal("GetBaseQueueEntry returned nil")
	}
	if got.ID != "direct-id" {
		t.Errorf("ID = %q, want %q", got.ID, "direct-id")
	}
	if got != entry {
		t.Error("GetBaseQueueEntry should return the same pointer")
	}
}

func newIntegrationTestPool(t *testing.T) (pool *pgxpool.Pool, schema string) {
	t.Helper()

	databaseURL := os.Getenv("LITEQ_TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("integration test requires LITEQ_TEST_DATABASE_URL or DATABASE_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping database: %v", err)
	}

	schema = fmt.Sprintf("liteq_phase014_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		mgr := NewSchemaManager(pool, WithSchemaManagerSchema(schema))
		if err := mgr.MigrateDownAll(context.Background()); err != nil {
			t.Fatalf("cleanup schema %s: %v", schema, err)
		}
		pool.Close()
	})

	return pool, schema
}

func newIntegrationTestQueue(t *testing.T) (*PgQueue[Task], *pgxpool.Pool) {
	t.Helper()

	pool, schema := newIntegrationTestPool(t)
	ctx := context.Background()
	queueName := fmt.Sprintf("queue_tasks_%d", time.Now().UnixNano())
	mgr := NewSchemaManager(pool, WithSchemaManagerSchema(schema))
	if err := mgr.EnsureQueue(ctx, queueName, ""); err != nil {
		t.Fatalf("EnsureQueue: %v", err)
	}

	q, err := NewPgQueue[Task](ctx, pool, queueName, &RetryPolicy{
		Strategy:     StrategyExponential,
		MaxRetries:   3,
		RetryDelayMs: 100,
		MaxDelayMs:   10000,
	}, WithSchema(schema))
	if err != nil {
		t.Fatalf("NewPgQueue: %v", err)
	}
	return q, pool
}

func enqueueIntegrationTestTasks(t *testing.T, q *PgQueue[Task], count int) {
	t.Helper()

	tx, txCtx, cancel, err := q.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer cancel()
	defer rollback(tx)

	for i := range count {
		task := newTestTask(fmt.Sprintf("task-%d", i))
		if err := q.Enqueue(task, tx); err != nil {
			t.Fatalf("Enqueue(%d): %v", i, err)
		}
	}

	if err := tx.Commit(txCtx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func countRowsForQueue(t *testing.T, pool *pgxpool.Pool, table, queueName string) int {
	t.Helper()

	var count int
	err := pool.QueryRow(
		context.Background(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE queue_name = $1", table),
		queueName,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count rows in %s: %v", table, err)
	}
	return count
}

func TestPgQueue_Enqueue_RejectsWhenPaused(t *testing.T) {
	q, _ := newIntegrationTestQueue(t)
	if err := q.Pause(context.Background()); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	err := q.Enqueue(newTestTask("paused-enqueue"), nil)
	if !errors.Is(err, ErrQueuePaused) {
		t.Fatalf("Enqueue error = %v, want ErrQueuePaused", err)
	}
}

func TestPgQueue_Enqueue_RejectsWhenDraining(t *testing.T) {
	q, _ := newIntegrationTestQueue(t)
	if err := q.setQueueState(context.Background(), "draining", "test draining state"); err != nil {
		t.Fatalf("setQueueState: %v", err)
	}

	err := q.Enqueue(newTestTask("draining-enqueue"), nil)
	if !errors.Is(err, ErrQueueDraining) {
		t.Fatalf("Enqueue error = %v, want ErrQueueDraining", err)
	}
}

func TestPgQueue_Dequeue_RejectsWhenPaused(t *testing.T) {
	q, _ := newIntegrationTestQueue(t)
	if err := q.Pause(context.Background()); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	_, err := q.Dequeue(1)
	if !errors.Is(err, ErrQueuePaused) {
		t.Fatalf("Dequeue error = %v, want ErrQueuePaused", err)
	}
}

func TestPgQueue_Dequeue_RejectsWhenDraining(t *testing.T) {
	q, _ := newIntegrationTestQueue(t)
	if err := q.setQueueState(context.Background(), "draining", "test draining state"); err != nil {
		t.Fatalf("setQueueState: %v", err)
	}

	_, err := q.Dequeue(1)
	if !errors.Is(err, ErrQueueDraining) {
		t.Fatalf("Dequeue error = %v, want ErrQueueDraining", err)
	}
}

func TestPgQueue_IsPaused_DefaultsToFalseForUnregistered(t *testing.T) {
	pool, schema := newIntegrationTestPool(t)
	mgr := NewSchemaManager(pool, WithSchemaManagerSchema(schema))
	if err := mgr.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	q, err := NewPgQueue[Task](context.Background(), pool, fmt.Sprintf("queue_unregistered_%d", time.Now().UnixNano()), nil, WithSchema(schema))
	if err != nil {
		t.Fatalf("NewPgQueue: %v", err)
	}

	paused, err := q.IsPaused(context.Background())
	if err != nil {
		t.Fatalf("IsPaused: %v", err)
	}
	if paused {
		t.Fatal("IsPaused returned true, want false")
	}
}

func TestPgQueue_PauseResume_RoundTrip(t *testing.T) {
	q, _ := newIntegrationTestQueue(t)
	ctx := context.Background()

	if err := q.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	paused, err := q.IsPaused(ctx)
	if err != nil {
		t.Fatalf("IsPaused after Pause: %v", err)
	}
	if !paused {
		t.Fatal("IsPaused after Pause = false, want true")
	}

	if err := q.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	paused, err = q.IsPaused(ctx)
	if err != nil {
		t.Fatalf("IsPaused after Resume: %v", err)
	}
	if paused {
		t.Fatal("IsPaused after Resume = true, want false")
	}
}

func TestPgQueue_Drain_RemovesAllEntries(t *testing.T) {
	q, pool := newIntegrationTestQueue(t)
	enqueueIntegrationTestTasks(t, q, 5)

	if err := q.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	var count int
	err := pool.QueryRow(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", q.queueTable())).Scan(&count)
	if err != nil {
		t.Fatalf("count queue rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("queue row count = %d, want 0", count)
	}
}

func TestPgQueue_Drain_TransitionsToPaused(t *testing.T) {
	q, pool := newIntegrationTestQueue(t)
	enqueueIntegrationTestTasks(t, q, 2)

	if err := q.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	paused, err := q.IsPaused(context.Background())
	if err != nil {
		t.Fatalf("IsPaused: %v", err)
	}
	if !paused {
		t.Fatal("IsPaused after Drain = false, want true")
	}

	state, err := q.getQueueState(context.Background())
	if err != nil {
		t.Fatalf("getQueueState: %v", err)
	}
	if state != "paused" {
		t.Fatalf("queue state = %q, want paused", state)
	}

	if got := countRowsForQueue(t, pool, q.queueStatesTable(), q.QueueName); got != 3 {
		t.Fatalf("queue_states rows = %d, want 3", got)
	}
}

func TestPgQueue_Drain_EmptyQueueSucceeds(t *testing.T) {
	q, _ := newIntegrationTestQueue(t)
	if err := q.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	paused, err := q.IsPaused(context.Background())
	if err != nil {
		t.Fatalf("IsPaused: %v", err)
	}
	if !paused {
		t.Fatal("IsPaused after empty drain = false, want true")
	}
}

func TestPgQueue_Drain_RecordsTwoEvents(t *testing.T) {
	q, pool := newIntegrationTestQueue(t)
	if err := q.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if got := countRowsForQueue(t, pool, q.queueStatesTable(), q.QueueName); got != 3 {
		t.Fatalf("queue_states rows = %d, want 3", got)
	}
}
