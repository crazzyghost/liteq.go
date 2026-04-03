package liteq

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestTaskError_Error(t *testing.T) {
	inner := fmt.Errorf("disk full")
	te := &TaskError{TaskID: "task-42", Err: inner}
	want := "task task-42: disk full"
	if got := te.Error(); got != want {
		t.Errorf("TaskError.Error() = %q, want %q", got, want)
	}
}

func TestTaskError_Unwrap(t *testing.T) {
	sentinel := fmt.Errorf("sentinel")
	te := &TaskError{TaskID: "t1", Err: sentinel}
	if !errors.Is(te, sentinel) {
		t.Error("errors.Is through TaskError should find the wrapped sentinel")
	}
}

func TestBatchError_Error(t *testing.T) {
	tests := []struct {
		total, failed int
		want          string
	}{
		{10, 3, "batch: 3/10 tasks failed"},
		{1, 1, "batch: 1/1 tasks failed"},
		{0, 0, "batch: 0/0 tasks failed"},
	}
	for _, tc := range tests {
		be := &BatchError{Total: tc.total, Failed: tc.failed}
		if got := be.Error(); got != tc.want {
			t.Errorf("BatchError.Error() = %q, want %q", got, tc.want)
		}
	}
}

func TestBatchError_Unwrap(t *testing.T) {
	sentinel1 := fmt.Errorf("err1")
	sentinel2 := fmt.Errorf("err2")
	be := &BatchError{
		Total:  2,
		Failed: 2,
		Errors: []*TaskError{
			{TaskID: "t1", Err: sentinel1},
			{TaskID: "t2", Err: sentinel2},
		},
	}

	errs := be.Unwrap()
	if len(errs) != 2 {
		t.Fatalf("BatchError.Unwrap() len = %d, want 2", len(errs))
	}
	if !errors.Is(errs[0], sentinel1) {
		t.Error("first unwrapped error should be sentinel1")
	}
	if !errors.Is(errs[1], sentinel2) {
		t.Error("second unwrapped error should be sentinel2")
	}
}

func TestBatchError_Unwrap_Empty(t *testing.T) {
	be := &BatchError{}
	if errs := be.Unwrap(); len(errs) != 0 {
		t.Errorf("empty BatchError.Unwrap() len = %d, want 0", len(errs))
	}
}

func TestMaxRetriesExceededError_Error(t *testing.T) {
	tests := []struct {
		retries, max int
		want         string
	}{
		{3, 3, "max retries exceeded: 3/3"},
		{0, 0, "max retries exceeded: 0/0"},
		{5, 3, "max retries exceeded: 5/3"},
	}
	for _, tc := range tests {
		e := &MaxRetriesExceededError{Retries: tc.retries, MaxRetries: tc.max}
		if got := e.Error(); got != tc.want {
			t.Errorf("MaxRetriesExceededError.Error() = %q, want %q", got, tc.want)
		}
	}
}

func TestConsumerError_Error(t *testing.T) {
	inner := fmt.Errorf("connection refused")
	ce := &ConsumerError{Source: inner, IsNonTransient: false}
	want := "consumer error: connection refused"
	if got := ce.Error(); got != want {
		t.Errorf("ConsumerError.Error() = %q, want %q", got, want)
	}
}

func TestConsumerError_Unwrap(t *testing.T) {
	sentinel := fmt.Errorf("db down")
	ce := &ConsumerError{Source: sentinel}
	if !errors.Is(ce, sentinel) {
		t.Error("errors.Is through ConsumerError should find the wrapped sentinel")
	}
}

func TestConsumerError_IsNonTransient(t *testing.T) {
	ce := &ConsumerError{Source: fmt.Errorf("bad input"), IsNonTransient: true}
	if !ce.IsNonTransient {
		t.Error("IsNonTransient should be true")
	}
}

func TestConsumerError_Transient(t *testing.T) {
	ce := &ConsumerError{Source: fmt.Errorf("timeout"), IsNonTransient: false}
	if ce.IsNonTransient {
		t.Error("IsNonTransient should be false for transient error")
	}
}

func TestTaskError_As(t *testing.T) {
	inner := fmt.Errorf("inner")
	te := &TaskError{TaskID: "t1", Err: inner}

	var got *TaskError
	if !errors.As(te, &got) {
		t.Fatal("errors.As should find *TaskError")
	}
	if got.TaskID != "t1" {
		t.Errorf("got.TaskID = %q, want %q", got.TaskID, "t1")
	}
}

func TestBatchError_ContainsTaskError(t *testing.T) {
	inner := fmt.Errorf("upstream failed")
	be := &BatchError{
		Total:  1,
		Failed: 1,
		Errors: []*TaskError{{TaskID: "t1", Err: inner}},
	}
	if !errors.Is(be, inner) {
		t.Error("errors.Is should find inner through BatchError -> TaskError chain")
	}
}

func TestErrQueuePaused_WrappingAndUnwrapping(t *testing.T) {
	if !errors.Is(ErrQueuePaused, ErrQueuePaused) {
		t.Fatal("ErrQueuePaused should match itself via errors.Is")
	}

	wrapped := fmt.Errorf("enqueue queue_tasks: %w", ErrQueuePaused)
	if !errors.Is(wrapped, ErrQueuePaused) {
		t.Fatal("wrapped paused error should match ErrQueuePaused")
	}
	if got := wrapped.Error(); !strings.Contains(got, "queue_tasks") || !strings.Contains(got, "queue is paused") {
		t.Fatalf("wrapped paused error = %q", got)
	}

	doubleWrapped := fmt.Errorf("operation failed: %w", wrapped)
	if !errors.Is(doubleWrapped, ErrQueuePaused) {
		t.Fatal("double-wrapped paused error should match ErrQueuePaused")
	}
}

func TestErrQueueDraining_WrappingAndUnwrapping(t *testing.T) {
	if !errors.Is(ErrQueueDraining, ErrQueueDraining) {
		t.Fatal("ErrQueueDraining should match itself via errors.Is")
	}

	wrapped := fmt.Errorf("enqueue queue_tasks: %w", ErrQueueDraining)
	if !errors.Is(wrapped, ErrQueueDraining) {
		t.Fatal("wrapped draining error should match ErrQueueDraining")
	}
	if got := wrapped.Error(); !strings.Contains(got, "queue_tasks") || !strings.Contains(got, "queue is draining") {
		t.Fatalf("wrapped draining error = %q", got)
	}

	doubleWrapped := fmt.Errorf("operation failed: %w", wrapped)
	if !errors.Is(doubleWrapped, ErrQueueDraining) {
		t.Fatal("double-wrapped draining error should match ErrQueueDraining")
	}
}
