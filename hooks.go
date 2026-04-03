package liteq

import (
	"context"
	"log/slog"
	"time"
)

// Hooks defines lifecycle callbacks for observability. Implement this interface
// to integrate custom logging, metrics, or tracing into the worker. Embed
// BaseHooks to selectively override only the methods you need.
type Hooks interface {
	OnEnqueue(ctx context.Context, queueName, entryID string)
	OnDequeue(ctx context.Context, queueName string, count int)
	OnTaskStart(ctx context.Context, taskID string)
	OnTaskComplete(ctx context.Context, taskID string, duration time.Duration)
	OnRetry(ctx context.Context, taskID string, attempt int, nextRunAt time.Time)
	OnDLQ(ctx context.Context, taskID, reason string)
	OnDLQFailed(ctx context.Context, taskID string, err error)
	OnQueuePaused(ctx context.Context, queueName string)
	OnQueueResumed(ctx context.Context, queueName string)
	OnQueueDrained(ctx context.Context, queueName string)
}

// BaseHooks is a no-op Hooks implementation. Embed it in your own struct to
// selectively override only the hooks you care about.
type BaseHooks struct{}

// OnEnqueue is a no-op implementation.
func (BaseHooks) OnEnqueue(context.Context, string, string) {}

// OnDequeue is a no-op implementation.
func (BaseHooks) OnDequeue(context.Context, string, int) {}

// OnTaskStart is a no-op implementation.
func (BaseHooks) OnTaskStart(context.Context, string) {}

// OnTaskComplete is a no-op implementation.
func (BaseHooks) OnTaskComplete(context.Context, string, time.Duration) {}

// OnRetry is a no-op implementation.
func (BaseHooks) OnRetry(context.Context, string, int, time.Time) {}

// OnDLQ is a no-op implementation.
func (BaseHooks) OnDLQ(context.Context, string, string) {}

// OnDLQFailed is a no-op implementation.
func (BaseHooks) OnDLQFailed(context.Context, string, error) {}

// OnQueuePaused is a no-op implementation.
func (BaseHooks) OnQueuePaused(context.Context, string) {}

// OnQueueResumed is a no-op implementation.
func (BaseHooks) OnQueueResumed(context.Context, string) {}

// OnQueueDrained is a no-op implementation.
func (BaseHooks) OnQueueDrained(context.Context, string) {}

// SlogHooks emits structured log lines via slog for each lifecycle event.
type SlogHooks struct {
	BaseHooks
	Logger *slog.Logger
}

// OnEnqueue logs a task enqueue event.
func (h SlogHooks) OnEnqueue(ctx context.Context, queueName, entryID string) {
	h.Logger.InfoContext(ctx, "task enqueued", "queue", queueName, "id", entryID)
}

// OnDequeue logs a batch dequeue event.
func (h SlogHooks) OnDequeue(ctx context.Context, queueName string, count int) {
	h.Logger.InfoContext(ctx, "tasks dequeued", "queue", queueName, "count", count)
}

// OnTaskStart logs a task start event.
func (h SlogHooks) OnTaskStart(ctx context.Context, taskID string) {
	h.Logger.InfoContext(ctx, "task started", "id", taskID)
}

// OnTaskComplete logs a task completion event with duration.
func (h SlogHooks) OnTaskComplete(ctx context.Context, taskID string, duration time.Duration) {
	h.Logger.InfoContext(ctx, "task completed", "id", taskID, "duration", duration)
}

// OnRetry logs a task retry scheduling event.
func (h SlogHooks) OnRetry(ctx context.Context, taskID string, attempt int, nextRunAt time.Time) {
	h.Logger.InfoContext(ctx, "task scheduled for retry", "id", taskID, "attempt", attempt, "nextRunAt", nextRunAt)
}

// OnDLQ logs a dead-letter queue enqueue event.
func (h SlogHooks) OnDLQ(ctx context.Context, taskID, reason string) {
	h.Logger.WarnContext(ctx, "task sent to dead-letter queue", "id", taskID, "reason", reason)
}

// OnDLQFailed logs a dead-letter queue enqueue failure.
func (h SlogHooks) OnDLQFailed(ctx context.Context, taskID string, err error) {
	h.Logger.ErrorContext(ctx, "task could not be sent to dead-letter queue", "id", taskID, "error", err)
}

// OnQueuePaused logs a queue pause event.
func (h SlogHooks) OnQueuePaused(ctx context.Context, queueName string) {
	h.Logger.InfoContext(ctx, "queue paused", slog.String("queue", queueName))
}

// OnQueueResumed logs a queue resume event.
func (h SlogHooks) OnQueueResumed(ctx context.Context, queueName string) {
	h.Logger.InfoContext(ctx, "queue resumed", slog.String("queue", queueName))
}

// OnQueueDrained logs a queue drain event.
func (h SlogHooks) OnQueueDrained(ctx context.Context, queueName string) {
	h.Logger.InfoContext(ctx, "queue drained", slog.String("queue", queueName))
}

// safeHook calls f, recovering from any panic so that a misbehaving hook
// implementation cannot crash the worker.
func safeHook(f func()) {
	defer func() { recover() }() //nolint:errcheck // recover() return value is intentionally discarded
	f()
}
