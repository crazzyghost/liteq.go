package liteq

import (
	"context"
	"time"
)

type RetryPolicy struct {
	Strategy     string `json:"strategy"`
	MaxRetries   int    `json:"maxRetries"`
	RetryDelayMs int    `json:"retryDelayMs"`
	MaxDelayMs   int    `json:"maxDelayMs"`
}

type ScheduleConfig struct {
	DelayMs        int    `json:"delayMs"`
	CronExpression string `json:"cronExpression"`
	Timezone       string `json:"timezone"`
}

type BaseQueue struct {
	Ctx         context.Context
	QueueName   string
	RetryPolicy *RetryPolicy
}

type BaseQueueEntryData interface{}

type BaseQueueEntryMetaData struct {
	Status         string         `json:"status"`
	IsRetry        bool           `json:"isRetry"`
	RetryPolicy    RetryPolicy    `json:"retryPolicy"`
	Retries        int            `json:"retries"`
	ScheduleConfig ScheduleConfig `json:"scheduleConfig"`
	NextRunAt      *time.Time     `json:"nextRunAt"`
	LastRunAt      *time.Time     `json:"lastRunAt"`
	ProcessedAt    *time.Time     `json:"processedAt"`
}

type BaseQueueEntry struct {
	Id         string                 `json:"id"`
	Data       BaseQueueEntryData     `json:"data"`
	Meta       BaseQueueEntryMetaData `json:"meta"`
	EnqueuedAt *time.Time             `json:"enqueued_at"`
	DequeuedAt *time.Time             `json:"dequeued_at"`
	CreatedAt  *time.Time             `json:"created_at"`
	UpdatedAt  *time.Time             `json:"updated_at"`
	DeletedAt  *time.Time             `json:"deleted_at"`
}

type IQueueEntry interface {
	GetBaseQueueEntry() *BaseQueueEntry
}

func (e *BaseQueueEntry) GetBaseQueueEntry() *BaseQueueEntry {
	return e
}
