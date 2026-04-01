package liteq

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
type Task struct {
	BaseQueueEntry
}

// GetBaseQueueEntry returns the underlying BaseQueueEntry for this task.
func (t *Task) GetBaseQueueEntry() *BaseQueueEntry {
	return &t.BaseQueueEntry
}

// WillExceedMaxRetries reports whether the next retry would exceed maxRetries.
func (t *Task) WillExceedMaxRetries(maxRetries int) bool {
	return (t.Retries + 1) > maxRetries
}
