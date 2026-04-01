package liteq

import "fmt"

// TaskError wraps a single task processing failure.
type TaskError struct {
	TaskID string
	Err    error
}

func (e *TaskError) Error() string {
	return fmt.Sprintf("task %s: %s", e.TaskID, e.Err)
}

func (e *TaskError) Unwrap() error { return e.Err }

// BatchError aggregates task errors from a single Work() batch.
type BatchError struct {
	Total  int          // tasks attempted
	Failed int          // tasks that errored
	Errors []*TaskError // individual failures
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("batch: %d/%d tasks failed", e.Failed, e.Total)
}

func (e *BatchError) Unwrap() []error {
	out := make([]error, len(e.Errors))
	for i, te := range e.Errors {
		out[i] = te
	}
	return out
}

// MaxRetriesExceededError indicates a task has exceeded its maximum retry count.
type MaxRetriesExceededError struct {
	Retries    int
	MaxRetries int
}

func (e *MaxRetriesExceededError) Error() string {
	return fmt.Sprintf("max retries exceeded: %d/%d", e.Retries, e.MaxRetries)
}

// ConsumerError wraps an error returned by a Consumer with transience metadata.
type ConsumerError struct {
	Source         error
	IsNonTransient bool
}

func (e *ConsumerError) Error() string {
	return fmt.Sprintf("consumer error: %v", e.Source)
}

func (e *ConsumerError) Unwrap() error {
	return e.Source
}
