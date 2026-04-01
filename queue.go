package liteq

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Retry strategy constants define the back-off algorithms available for task retries.
const (
	StrategyFixed       = "fixed"
	StrategyExponential = "exponential"
	StrategyLinear      = "linear"

	defaultSchema = "liteq"
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

// RetryPolicy configures the back-off behavior for failed tasks.
type RetryPolicy struct {
	Strategy     string `json:"strategy"`
	MaxRetries   int    `json:"maxRetries"`
	RetryDelayMs int    `json:"retryDelayMs"`
	MaxDelayMs   int    `json:"maxDelayMs"`
}

// BaseQueue holds the shared configuration for all queue implementations.
type BaseQueue struct {
	Ctx         context.Context
	Schema      string
	QueueName   string
	RetryPolicy *RetryPolicy
	TxTimeout   time.Duration // default 5s; applied to every transaction begin
}

// QualifiedQueueName returns the schema-qualified queue table name.
func (q BaseQueue) QualifiedQueueName() string {
	return qualifyIdentifier(q.Schema, q.QueueName)
}

// Queue defines the stable queue operations supported by liteq backends.
//
// The method set mirrors the current PgQueue API so callers can depend on an
// interface without forcing a breaking rewrite of the package surface.
type Queue[T any] interface {
	Enqueue(item T, tx pgx.Tx) error
	Dequeue(batchSize int) ([]T, error)
	UpdateEntry(item T, tx pgx.Tx, conditions ...Condition) error
	CheckCondition(ctx context.Context, tx pgx.Tx, conditions ...Condition) (bool, error)
	Select(ctx context.Context, scan func(pgx.Rows) error, mods ...SelectMod) error
	SelectOne(ctx context.Context, scan func(pgx.Rows) error, mods ...SelectMod) (bool, error)
	UpdateStatus(ctx context.Context, tx pgx.Tx, status string, conditions ...Condition) error
	GetRetryPolicy(ctx context.Context) (*RetryPolicy, error)
	BeginTx(ctx context.Context) (pgx.Tx, context.Context, context.CancelFunc, error)
	QueueLabel() string
}

// BaseQueueEntryData is the data payload stored in a queue entry.
type BaseQueueEntryData interface{}

// BaseQueueEntry represents a single row in a queue table.
type BaseQueueEntry struct {
	ID          string             `json:"id" db:"id"`
	Data        BaseQueueEntryData `json:"data" db:"data"`
	Status      string             `json:"status" db:"status"`
	IsRetry     bool               `json:"isRetry" db:"is_retry"`
	Retries     int                `json:"retries" db:"retries"`
	RetryPolicy *RetryPolicy       `json:"retryPolicy" db:"retry_policy"`
	NextRunAt   *time.Time         `json:"nextRunAt" db:"next_run_at"`
	LastRunAt   *time.Time         `json:"lastRunAt" db:"last_run_at"`
	ProcessedAt *time.Time         `json:"processedAt" db:"processed_at"`
	EnqueuedAt  *time.Time         `json:"enqueued_at" db:"enqueued_at"`
	DequeuedAt  *time.Time         `json:"dequeued_at" db:"dequeued_at"`
	CreatedAt   *time.Time         `json:"created_at" db:"created_at"`
	UpdatedAt   *time.Time         `json:"updated_at" db:"updated_at"`
	DeletedAt   *time.Time         `json:"deleted_at" db:"deleted_at"`
}

// IQueueEntry is implemented by types that embed a BaseQueueEntry.
type IQueueEntry interface {
	GetBaseQueueEntry() *BaseQueueEntry
}

// GetBaseQueueEntry returns the receiver itself, satisfying IQueueEntry.
func (e *BaseQueueEntry) GetBaseQueueEntry() *BaseQueueEntry {
	return e
}

type pgQueueConfig struct {
	schema      string
	autoMigrate bool
	dlqName     string
}

// PgQueueOption configures a PgQueue during construction.
type PgQueueOption func(*pgQueueConfig)

// WithSchema sets the PostgreSQL schema for queue tables.
func WithSchema(schema string) PgQueueOption {
	return func(c *pgQueueConfig) {
		c.schema = schema
	}
}

// WithAutoMigrate enables automatic schema migration on queue creation.
func WithAutoMigrate() PgQueueOption {
	return func(c *pgQueueConfig) {
		c.autoMigrate = true
	}
}

// WithDLQName overrides the DLQ table name used by WithAutoMigrate.
// When unset, liteq derives "<queue>_dead_letter" for non-DLQ queues.
func WithDLQName(name string) PgQueueOption {
	return func(c *pgQueueConfig) {
		c.dlqName = name
	}
}

func newPgQueueConfig() pgQueueConfig {
	return pgQueueConfig{
		schema: defaultSchema,
	}
}
