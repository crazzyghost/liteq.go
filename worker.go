package liteq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type TaskRetryPolicySelector func(task Task) (*RetryPolicy, error)

type WorkerConfig struct {
	TaskBatchSize      int
	TaskQueue          *PgQueue[Task]
	DeadLetterQueue    *PgQueue[Task]
	GetTaskRetryPolicy TaskRetryPolicySelector
	MaxConcurrency     int           // default: runtime.NumCPU()
	TaskTimeout        time.Duration // default: 30s
}

type Worker struct {
	Ctx                context.Context
	TaskBatchSize      int
	TaskQueue          *PgQueue[Task]
	DeadLetterQueue    *PgQueue[Task]
	GetTaskRetryPolicy TaskRetryPolicySelector
	MaxConcurrency     int
	TaskTimeout        time.Duration
	done               chan struct{}
	stopOnce           sync.Once
}

// NewWorker validates config and constructs a Worker. A non-nil error is
// returned when any required field is missing or has an invalid value.
func NewWorker(ctx context.Context, config WorkerConfig) (*Worker, error) {
	if config.TaskQueue == nil {
		return nil, fmt.Errorf("liteq: NewWorker: queue must not be nil")
	}
	if config.DeadLetterQueue == nil {
		return nil, fmt.Errorf("liteq: NewWorker: dlq must not be nil")
	}
	if config.TaskBatchSize == 0 {
		config.TaskBatchSize = 10
	}
	if config.TaskBatchSize < 1 {
		return nil, fmt.Errorf("liteq: NewWorker: batchSize must be >= 1, got %d", config.TaskBatchSize)
	}
	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = runtime.NumCPU()
	}
	if config.TaskTimeout == 0 {
		config.TaskTimeout = 30 * time.Second
	}

	return &Worker{
		Ctx:                ctx,
		TaskBatchSize:      config.TaskBatchSize,
		TaskQueue:          config.TaskQueue,
		DeadLetterQueue:    config.DeadLetterQueue,
		GetTaskRetryPolicy: config.GetTaskRetryPolicy,
		MaxConcurrency:     config.MaxConcurrency,
		TaskTimeout:        config.TaskTimeout,
		done:               make(chan struct{}),
	}, nil
}

// Stop signals the worker to drain and exit. Safe to call multiple times.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() { close(w.done) })
}

// Run polls in a loop until Stop() is called or ctx is cancelled.
// Errors from individual Work() batches are logged but do not abort the loop.
func (w *Worker) Run(ctx context.Context, factory ConsumerFactory, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.done:
			return nil
		case <-ticker.C:
			if err := w.Work(ctx, factory); err != nil {
				slog.Error("work batch error", slog.Any("error", err))
			}
		}
	}
}

func (w *Worker) Poll() (tasks []Task, err error) {
	tasks, err = w.TaskQueue.Dequeue(w.TaskBatchSize)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (w *Worker) HandleProcessed(task Task, status TaskStatus, tx pgx.Tx) (err error) {
	task.Meta.Status = string(status)

	err = w.TaskQueue.UpdateQueueEntryMeta(task, tx)
	if err != nil {
		slog.Error("could not mark task as processed", "err", err)
		return err
	}
	return nil
}

// GetRetrySchedule calculates the next run time for a task based on the retry
// policy strategy. Supported strategies are fixed, linear, and exponential
// (default). A jitter in the range [0, delayMs] is applied to spread load.
func (w *Worker) GetRetrySchedule(task *Task, retryPolicy *RetryPolicy) (time.Time, error) {
	retries := task.Meta.Retries

	strategy, err := ParseRetryStrategy(retryPolicy.Strategy)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid retry strategy: %w", err)
	}

	var delayMs int
	switch strategy {
	case StrategyFixed:
		delayMs = retryPolicy.RetryDelayMs
	case StrategyLinear:
		delayMs = min(retryPolicy.RetryDelayMs*(retries+1), retryPolicy.MaxDelayMs)
	default: // StrategyExponential
		backoff := retryPolicy.RetryDelayMs * int(math.Pow(2, float64(retries)))
		delayMs = min(backoff, retryPolicy.MaxDelayMs)
	}

	jitteredMs := rand.IntN(delayMs + 1)
	return time.Now().Add(time.Duration(jitteredMs) * time.Millisecond), nil
}

func (w *Worker) Retry(task Task, tx pgx.Tx) (err error) {
	meta := task.Meta
	retryPolicy, err := w.GetTaskRetryPolicy(task)
	if err != nil {
		return fmt.Errorf("could not resolve retry policy: %w", err)
	}

	if retryPolicy == nil {
		slog.Warn("retry policy not configured; treating max retries as 0")
		return &MaxRetriesExceededError{Retries: meta.Retries, MaxRetries: 0}
	}

	if task.WillExceedMaxRetries(retryPolicy.MaxRetries) {
		slog.Error("task has exceeded max retries")
		return &MaxRetriesExceededError{Retries: meta.Retries, MaxRetries: retryPolicy.MaxRetries}
	}

	nextRunAt, err := w.GetRetrySchedule(&task, retryPolicy)
	if err != nil {
		return fmt.Errorf("could not compute retry schedule: %w", err)
	}

	task.Meta.IsRetry = true
	task.Meta.Retries += 1
	task.Meta.NextRunAt = &nextRunAt
	task.Meta.Status = "PENDING"

	err = w.TaskQueue.Enqueue(task, tx)
	if err != nil {
		slog.Error("could not add task to retry queue", slog.Any("error", err))
		return err
	}

	return nil
}

func (w *Worker) DlqEnqueue(task Task, tx pgx.Tx) (err error) {
	task.Meta.Status = string(FAILED)
	task.Meta.IsRetry = false
	task.Meta.LastRunAt = task.Meta.NextRunAt
	task.Meta.NextRunAt = nil
	err = w.DeadLetterQueue.Enqueue(task, tx)
	if err != nil {
		slog.Error("could not push task to dead letter queue", "err", err)
		return err
	}

	return nil
}

// dlqEnqueueWithRetry attempts to enqueue a task into the dead-letter queue
// with up to 3 retries and linear backoff (100ms, 200ms, 300ms). On exhaustion,
// the task is marked DLQ_FAILED in the task queue so it can be recovered manually.
func (w *Worker) dlqEnqueueWithRetry(ctx context.Context, task Task) error {
	delays := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond}
	var lastErr error
	for _, delay := range delays {
		tx, txCtx, cancel, err := w.DeadLetterQueue.beginTx(ctx)
		if err != nil {
			lastErr = err
			time.Sleep(delay)
			continue
		}
		if err := w.DlqEnqueue(task, tx); err != nil {
			cancel()
			rollback(tx)
			lastErr = err
			time.Sleep(delay)
			continue
		}
		if err := tx.Commit(txCtx); err != nil {
			cancel()
			rollback(tx)
			lastErr = err
			time.Sleep(delay)
			continue
		}
		cancel()
		return nil
	}

	// Exhausted retries — mark task as DLQ_FAILED so it is not silently lost.
	tx, txCtx, cancel, err := w.TaskQueue.beginTx(ctx)
	if err != nil {
		return fmt.Errorf("dlq enqueue failed after %d retries and could not start status update: %w",
			len(delays), errors.Join(lastErr, err))
	}
	defer cancel()
	defer rollback(tx)
	if err := w.TaskQueue.UpdateStatus(ctx, tx, string(DLQ_FAILED), IDEquals(task.Id)); err != nil {
		return fmt.Errorf("dlq enqueue failed after %d retries and status update failed: %w",
			len(delays), errors.Join(lastErr, err))
	}
	if err := tx.Commit(txCtx); err != nil {
		return fmt.Errorf("dlq enqueue failed after %d retries and status update commit failed: %w",
			len(delays), errors.Join(lastErr, err))
	}
	return fmt.Errorf("dlq enqueue failed after %d retries, marked DLQ_FAILED: %w", len(delays), lastErr)
}

// HandleUnit runs the consumer for one task with a deadline, then commits the
// result (completed, retry, or DLQ) inside a transaction. A timeout is treated
// as a transient failure and follows the normal retry path.
func (w *Worker) HandleUnit(ctx context.Context, task Task, consumer Consumer) error {
	taskCtx, cancel := context.WithTimeout(ctx, w.TaskTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- consumer.Consume(taskCtx, task) }()

	var consumerErr error
	select {
	case err := <-done:
		consumerErr = err
	case <-taskCtx.Done():
		consumerErr = fmt.Errorf("task %s timed out after %s: %w",
			task.Id, w.TaskTimeout, taskCtx.Err())
	}

	tx, txCtx, txCancel, dbErr := w.TaskQueue.beginTx(ctx)
	if dbErr != nil {
		return fmt.Errorf("could not start transaction: %w", dbErr)
	}
	defer txCancel()
	defer rollback(tx)

	if consumerErr != nil {
		if err := w.HandleFailure(ctx, task, consumerErr, tx); err != nil {
			return fmt.Errorf("could not handle task failure: %v", err)
		}
	} else {
		if dbErr = w.HandleProcessed(task, COMPLETED, tx); dbErr != nil {
			return fmt.Errorf("could not mark task as completed: %w", dbErr)
		}
	}

	if dbErr = tx.Commit(txCtx); dbErr != nil {
		return fmt.Errorf("could not commit transaction: %w", dbErr)
	}

	return nil
}

func (w *Worker) HandleFailure(ctx context.Context, task Task, failure error, tx pgx.Tx) error {
	if err := w.HandleProcessed(task, FAILED, tx); err != nil {
		return fmt.Errorf("could not mark task as failed: %w", err)
	}

	var consumerErr *ConsumerError
	if errors.As(failure, &consumerErr) && consumerErr.IsNonTransient {
		if err := w.dlqEnqueueWithRetry(ctx, task); err != nil {
			return fmt.Errorf("could not push task to dead letter queue: %w", err)
		}
		return nil
	}

	if err := w.Retry(task, tx); err != nil {
		var maxRetriesExceed *MaxRetriesExceededError
		if errors.As(err, &maxRetriesExceed) {
			if queueError := w.dlqEnqueueWithRetry(ctx, task); queueError != nil {
				return fmt.Errorf("could not push task to dead letter queue: %w", queueError)
			}
			return nil
		}
		return fmt.Errorf("could not add task to retry queue: %w", err)
	}

	return nil
}

// Work dequeues one batch and processes all tasks using a bounded worker pool.
// All tasks complete regardless of individual failures; errors are aggregated
// into a BatchError.
func (w *Worker) Work(ctx context.Context, factory ConsumerFactory) error {
	tasks, err := w.Poll()
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}

	numWorkers := min(w.MaxConcurrency, len(tasks))
	taskCh := make(chan Task, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	var (
		wg       sync.WaitGroup
		errsMu   sync.Mutex
		taskErrs []*TaskError
	)

	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range taskCh {
				if err := w.HandleUnit(ctx, t, factory()); err != nil {
					slog.Error("could not process task", slog.Any("error", err))
					errsMu.Lock()
					taskErrs = append(taskErrs, &TaskError{TaskID: t.Id, Err: err})
					errsMu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	if len(taskErrs) == 0 {
		return nil
	}
	return &BatchError{
		Total:  len(tasks),
		Failed: len(taskErrs),
		Errors: taskErrs,
	}
}
