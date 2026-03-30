package liteq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
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
}

type Worker struct {
	Ctx                context.Context
	TaskBatchSize      int
	TaskQueue          *PgQueue[Task]
	DeadLetterQueue    *PgQueue[Task]
	GetTaskRetryPolicy TaskRetryPolicySelector
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

	return &Worker{
		Ctx:                ctx,
		TaskBatchSize:      config.TaskBatchSize,
		TaskQueue:          config.TaskQueue,
		DeadLetterQueue:    config.DeadLetterQueue,
		GetTaskRetryPolicy: config.GetTaskRetryPolicy,
	}, nil
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

func (w *Worker) HandleUnit(consumer Consumer, task Task) error {
	consumerErr := consumer.Consume(task)
	tx, dbErr := w.TaskQueue.Pool.Begin(w.Ctx)
	if dbErr != nil {
		return fmt.Errorf("could not start transaction: %w", dbErr)
	}
	defer rollback(tx)

	if consumerErr != nil {
		if err := w.HandleFailure(task, consumerErr, tx); err != nil {
			return fmt.Errorf("could not handle task failure: %v", err)
		}
	} else {
		if dbErr = w.HandleProcessed(task, COMPLETED, tx); dbErr != nil {
			return fmt.Errorf("could not mark task as completed: %w", dbErr)
		}
	}

	if dbErr = tx.Commit(w.Ctx); dbErr != nil {
		return fmt.Errorf("could not commit transaction: %w", dbErr)
	}

	return nil
}

func (w *Worker) HandleFailure(task Task, failure error, tx pgx.Tx) error {
	if err := w.HandleProcessed(task, FAILED, tx); err != nil {
		return fmt.Errorf("could not mark task as failed: %w", err)
	}

	var consumerErr *ConsumerError
	if errors.As(failure, &consumerErr) && consumerErr.IsNonTransient {
		if err := w.DlqEnqueue(task, tx); err != nil {
			return fmt.Errorf("could not push task to dead letter queue: %w", err)
		}
		return nil
	}

	if err := w.Retry(task, tx); err != nil {
		var maxRetriesExceed *MaxRetriesExceededError
		if errors.As(err, &maxRetriesExceed) {
			if queueError := w.DlqEnqueue(task, tx); queueError != nil {
				return fmt.Errorf("could not push task to dead letter queue: %w", queueError)
			}
			return nil
		}
		return fmt.Errorf("could not add task to retry queue: %w", err)
	}

	return nil
}

func (w *Worker) Work(consumerFactory ConsumerFactory) error {
	tasks, err := w.Poll()
	if err != nil {
		return err
	}

	var (
		wg     sync.WaitGroup
		errsMu sync.Mutex
		errs   []error
	)

	for _, task := range tasks {
		wg.Add(1)

		go func(t Task) {
			defer wg.Done()
			if err := w.HandleUnit(consumerFactory(), t); err != nil {
				slog.Error("could not process task", slog.Any("error", err))
				errsMu.Lock()
				errs = append(errs, err)
				errsMu.Unlock()
			}
		}(task)
	}

	wg.Wait()

	return errors.Join(errs...)
}
