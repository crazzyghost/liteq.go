package liteq

import (
	"context"
	"fmt"
	"time"
)

const (
	StrategyFixed       = "fixed"
	StrategyExponential = "exponential"
	StrategyLinear      = "linear"
)

// ParseRetryStrategy validates and normalises a retry strategy string.
// An empty string is treated as the default exponential strategy.
func ParseRetryStrategy(s string) (string, error) {
	switch s {
	case "", StrategyExponential:
		return StrategyExponential, nil
	case StrategyFixed:
		return StrategyFixed, nil
	case StrategyLinear:
		return StrategyLinear, nil
	default:
		return "", fmt.Errorf("unknown retry strategy %q, want one of: fixed, exponential, linear", s)
	}
}

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
	TxTimeout   time.Duration // default 5s; applied to every transaction begin
}

type BaseQueueEntryData interface{}

type BaseQueueEntryMetaData struct {
	Status         string         `json:"status"`
	IsRetry        bool           `json:"isRetry"`
	RetryPolicy    *RetryPolicy   `json:"retryPolicy"`
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
