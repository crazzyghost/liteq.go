package liteq

import "time"

// TaskStatus represents the lifecycle state of a task in the queue.
type TaskStatus string

// Task status constants represent all possible states in a task's lifecycle.
const (
	PENDING   TaskStatus = "PENDING"
	RUNNING   TaskStatus = "RUNNING"
	FAILED    TaskStatus = "FAILED"
	COMPLETED TaskStatus = "COMPLETED"
	CANCELLED TaskStatus = "CANCELLED" //nolint:misspell // CANCELLED is the stored DB value
	DLQFailed TaskStatus = "DLQ_FAILED"
)

// Task is a queue entry representing a unit of work to be processed.
// ID stores the canonical UUID string for the queue row. Leave it empty on
// enqueue to let PostgreSQL generate a new UUID.
type Task struct {
	ID          string       `json:"id" db:"id"`
	Data        QueueData    `json:"data" db:"data"`
	Status      string       `json:"status" db:"status"`
	IsRetry     bool         `json:"isRetry" db:"is_retry"`
	Retries     int          `json:"retries" db:"retries"`
	RetryPolicy *RetryPolicy `json:"retryPolicy" db:"retry_policy"`
	NextRunAt   *time.Time   `json:"nextRunAt" db:"next_run_at"`
	LastRunAt   *time.Time   `json:"lastRunAt" db:"last_run_at"`
	ProcessedAt *time.Time   `json:"processedAt" db:"processed_at"`
	EnqueuedAt  *time.Time   `json:"enqueued_at" db:"enqueued_at"`
	DequeuedAt  *time.Time   `json:"dequeued_at" db:"dequeued_at"`
	CreatedAt   *time.Time   `json:"created_at" db:"created_at"`
	UpdatedAt   *time.Time   `json:"updated_at" db:"updated_at"`
	DeletedAt   *time.Time   `json:"deleted_at" db:"deleted_at"`
}

// WillExceedMaxRetries reports whether the next retry would exceed maxRetries.
func (t *Task) WillExceedMaxRetries(maxRetries int) bool {
	return (t.Retries + 1) > maxRetries
}
