package liteq

type TaskStatus string

const (
	PENDING    TaskStatus = "PENDING"
	RUNNING    TaskStatus = "RUNNING"
	FAILED     TaskStatus = "FAILED"
	COMPLETED  TaskStatus = "COMPLETED"
	CANCELLED  TaskStatus = "CANCELLED"
	DLQ_FAILED TaskStatus = "DLQ_FAILED"
)

type Task struct {
	BaseQueueEntry
}

func (t *Task) GetBaseQueueEntry() *BaseQueueEntry {
	return &t.BaseQueueEntry
}

func (t *Task) WillExceedMaxRetries(maxRetries int) bool {
	return (t.Retries + 1) > maxRetries
}
