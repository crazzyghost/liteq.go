package liteq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// ---- NewWorker validation ----

func TestNewWorker_NilQueue(t *testing.T) {
	dlq := newFakePgQueue(t, "queue_tasks_dead_letter")
	_, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       nil,
		DeadLetterQueue: dlq,
	})
	if err == nil {
		t.Error("expected error for nil TaskQueue")
	}
}

func TestNewWorker_NilDLQ(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	_, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: nil,
	})
	if err == nil {
		t.Error("expected error for nil DeadLetterQueue")
	}
}

func TestNewWorker_TypedNilQueue(t *testing.T) {
	var q *PgQueue[Task]
	dlq := &mockQueue{label: "queue_tasks_dead_letter"}

	_, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
	})
	if err == nil {
		t.Error("expected error for typed nil TaskQueue")
	}
}

func TestNewWorker_AcceptsQueueInterface(t *testing.T) {
	q := &mockQueue{label: "queue_tasks"}
	dlq := &mockQueue{label: "queue_tasks_dead_letter"}

	w, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.TaskQueue.QueueLabel() != "queue_tasks" {
		t.Errorf("TaskQueue label = %q, want queue_tasks", w.TaskQueue.QueueLabel())
	}
	if w.DeadLetterQueue.QueueLabel() != "queue_tasks_dead_letter" {
		t.Errorf("DeadLetterQueue label = %q, want queue_tasks_dead_letter", w.DeadLetterQueue.QueueLabel())
	}
}

func TestNewWorker_NegativeBatchSize(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	dlq := newFakePgQueue(t, "queue_tasks_dead_letter")
	_, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
		TaskBatchSize:   -1,
	})
	if err == nil {
		t.Error("expected error for negative TaskBatchSize")
	}
}

func TestNewWorker_DefaultBatchSize(t *testing.T) {
	w := newTestWorker(t)
	// newTestWorker sets BatchSize=5, but test the zero default path explicitly.
	q := newFakePgQueue(t, "queue_tasks")
	dlq := newFakePgQueue(t, "queue_tasks_dead_letter")
	w2, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
		// TaskBatchSize=0 → should default to 10
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w2.TaskBatchSize != 10 {
		t.Errorf("default TaskBatchSize = %d, want 10", w2.TaskBatchSize)
	}
	_ = w
}

func TestNewWorker_DefaultMaxConcurrency(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	dlq := newFakePgQueue(t, "queue_tasks_dead_letter")
	w, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
		// MaxConcurrency=0 → should default to runtime.NumCPU()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.MaxConcurrency <= 0 {
		t.Errorf("MaxConcurrency should be > 0, got %d", w.MaxConcurrency)
	}
}

func TestNewWorker_DefaultTaskTimeout(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	dlq := newFakePgQueue(t, "queue_tasks_dead_letter")
	w, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.TaskTimeout != 30*time.Second {
		t.Errorf("default TaskTimeout = %v, want 30s", w.TaskTimeout)
	}
}

func TestNewWorker_DefaultHooks(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	dlq := newFakePgQueue(t, "queue_tasks_dead_letter")
	w, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.hooks == nil {
		t.Error("hooks should default to BaseHooks, not nil")
	}
	if _, ok := w.hooks.(BaseHooks); !ok {
		t.Errorf("default hooks should be BaseHooks, got %T", w.hooks)
	}
}

func TestNewWorker_CustomHooks(t *testing.T) {
	q := newFakePgQueue(t, "queue_tasks")
	dlq := newFakePgQueue(t, "queue_tasks_dead_letter")
	custom := SlogHooks{}
	w, err := NewWorker(context.Background(), &WorkerConfig{
		TaskQueue:       q,
		DeadLetterQueue: dlq,
		Hooks:           custom,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := w.hooks.(SlogHooks); !ok {
		t.Errorf("hooks should be SlogHooks, got %T", w.hooks)
	}
}

// ---- Stop / Run ----

func TestWorker_Stop_IdempotentMultipleCalls(t *testing.T) {
	w := newTestWorker(t)
	// Must not panic when called multiple times.
	w.Stop()
	w.Stop()
	w.Stop()
}

func TestWorker_Run_StopsCleanly(t *testing.T) {
	w := newTestWorker(t)

	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		// Use a 1-hour interval so the ticker never fires during the test.
		errCh <- w.Run(ctx, func() Consumer { return &fakeConsumer{} }, time.Hour)
	}()

	// Give the goroutine a moment to start then stop the worker.
	time.Sleep(20 * time.Millisecond)
	w.Stop()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run() returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Run() did not return after Stop()")
	}
}

func TestWorker_Run_CancelledCtx(t *testing.T) {
	w := newTestWorker(t)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(ctx, func() Consumer { return &fakeConsumer{} }, time.Hour)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run() on canceled ctx = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Run() did not return after ctx cancel")
	}
}

// ---- GetRetrySchedule ----

func TestGetRetrySchedule_Fixed(t *testing.T) {
	w := newTestWorker(t)
	task := newTestTask("t1")
	task.Retries = 0
	policy := &RetryPolicy{Strategy: StrategyFixed, RetryDelayMs: 200, MaxDelayMs: 1000}

	before := time.Now()
	next, err := w.GetRetrySchedule(&task, policy)
	after := time.Now().Add(200 * time.Millisecond)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.Before(before) {
		t.Error("next run should be >= now")
	}
	if next.After(after) {
		t.Errorf("next run too far in future: %v (want <= %v)", next, after)
	}
}

func TestGetRetrySchedule_Linear(t *testing.T) {
	w := newTestWorker(t)
	task := newTestTask("t1")
	task.Retries = 2 // delay = 100*(2+1) = 300ms, capped at MaxDelay
	policy := &RetryPolicy{Strategy: StrategyLinear, RetryDelayMs: 100, MaxDelayMs: 500}

	before := time.Now()
	next, err := w.GetRetrySchedule(&task, policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.Before(before) {
		t.Error("next run should be in the future")
	}
}

func TestGetRetrySchedule_Exponential(t *testing.T) {
	w := newTestWorker(t)
	task := newTestTask("t1")
	task.Retries = 3 // 100 * 2^3 = 800ms, under MaxDelay
	policy := &RetryPolicy{Strategy: StrategyExponential, RetryDelayMs: 100, MaxDelayMs: 10000}

	before := time.Now()
	next, err := w.GetRetrySchedule(&task, policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.Before(before) {
		t.Error("next run should be in the future")
	}
}

func TestGetRetrySchedule_ExponentialMaxDelayCap(t *testing.T) {
	w := newTestWorker(t)
	task := newTestTask("t1")
	task.Retries = 20 // would be astronomically large without cap
	policy := &RetryPolicy{Strategy: StrategyExponential, RetryDelayMs: 100, MaxDelayMs: 500}

	before := time.Now()
	next, err := w.GetRetrySchedule(&task, policy)
	after := time.Now().Add(500 * time.Millisecond)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.Before(before) {
		t.Error("next run should be in the future")
	}
	if next.After(after) {
		t.Errorf("next run %v exceeds MaxDelayMs cap %v", next, after)
	}
}

func TestGetRetrySchedule_InvalidStrategy(t *testing.T) {
	w := newTestWorker(t)
	task := newTestTask("t1")
	policy := &RetryPolicy{Strategy: "invalid", RetryDelayMs: 100, MaxDelayMs: 500}

	_, err := w.GetRetrySchedule(&task, policy)
	if err == nil {
		t.Error("expected error for invalid strategy")
	}
}

func TestGetRetrySchedule_LinearMaxDelayCap(t *testing.T) {
	w := newTestWorker(t)
	task := newTestTask("t1")
	task.Retries = 100 // 100 * 101 = 10100ms, exceeds MaxDelayMs=500
	policy := &RetryPolicy{Strategy: StrategyLinear, RetryDelayMs: 100, MaxDelayMs: 500}

	before := time.Now()
	next, err := w.GetRetrySchedule(&task, policy)
	after := time.Now().Add(500 * time.Millisecond)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.Before(before) {
		t.Error("next run should be in the future")
	}
	if next.After(after) {
		t.Errorf("linear next run %v exceeds MaxDelayMs cap %v", next, after)
	}
}

// ---- Retry — MaxRetriesExceededError paths (no DB needed) ----

func TestRetry_NilPolicy_ReturnsMaxRetriesExceeded(t *testing.T) {
	w := newTestWorker(t)
	// Override selector to return nil policy (meaning: no retries allowed).
	w.GetTaskRetryPolicy = func(_ Task) (*RetryPolicy, error) { return nil, nil }

	task := newTestTask("t1")
	err := w.Retry(context.Background(), &task, nil)

	var maxErr *MaxRetriesExceededError
	if !errors.As(err, &maxErr) {
		t.Errorf("expected MaxRetriesExceededError for nil policy, got: %v", err)
	}
	if maxErr.MaxRetries != 0 {
		t.Errorf("MaxRetries = %d, want 0", maxErr.MaxRetries)
	}
}

func TestRetry_MaxRetriesExceeded(t *testing.T) {
	w := newTestWorker(t)
	// Policy allows 0 retries.
	w.GetTaskRetryPolicy = func(_ Task) (*RetryPolicy, error) {
		return &RetryPolicy{Strategy: StrategyFixed, MaxRetries: 0, RetryDelayMs: 100, MaxDelayMs: 1000}, nil
	}

	task := newTestTask("t1")
	task.Retries = 0 // WillExceedMaxRetries(0): (0+1) > 0 = true

	err := w.Retry(context.Background(), &task, nil)

	var maxErr *MaxRetriesExceededError
	if !errors.As(err, &maxErr) {
		t.Errorf("expected MaxRetriesExceededError, got: %v", err)
	}
}

func TestRetry_PolicySelectorError(t *testing.T) {
	w := newTestWorker(t)
	w.GetTaskRetryPolicy = func(_ Task) (*RetryPolicy, error) {
		return nil, fmt.Errorf("policy store unavailable")
	}

	task := newTestTask("t1")
	err := w.Retry(context.Background(), &task, nil)
	if err == nil {
		t.Error("expected error when policy selector fails")
	}
}
