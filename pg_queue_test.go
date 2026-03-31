package liteq

import (
	"context"
	"fmt"
	"testing"
	"time"
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
	_, err := NewPgQueue[Task](nil, q.Pool, "q", nil) //nolint:staticcheck
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
		task.Meta.Retries = tc.retries
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
		{CANCELLED, "CANCELLED"},
		{DLQ_FAILED, "DLQ_FAILED"},
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
	if entry.Id != "id-1" {
		t.Errorf("entry.Id = %q, want %q", entry.Id, "id-1")
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
	entry := &BaseQueueEntry{Id: "direct-id"}
	got := entry.GetBaseQueueEntry()
	if got == nil {
		t.Fatal("GetBaseQueueEntry returned nil")
	}
	if got.Id != "direct-id" {
		t.Errorf("Id = %q, want %q", got.Id, "direct-id")
	}
	if got != entry {
		t.Error("GetBaseQueueEntry should return the same pointer")
	}
}
