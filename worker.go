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

func NewWorker(ctx context.Context, config WorkerConfig) *Worker {
	return &Worker{
		Ctx:                ctx,
		TaskBatchSize:      config.TaskBatchSize,
		TaskQueue:          config.TaskQueue,
		DeadLetterQueue:    config.DeadLetterQueue,
		GetTaskRetryPolicy: config.GetTaskRetryPolicy,
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

func (w *Worker) GetRetrySchedule(task *Task, retryPolicy *RetryPolicy) (nextRunAt time.Time) {
	retries := task.Meta.Retries

	backoff := retryPolicy.RetryDelayMs * int(math.Pow(2, float64(retries)))
	maxBackoff := min(backoff, retryPolicy.MaxDelayMs)

	backoffMs := rand.IntN(maxBackoff + 1)
	nextRunAt = time.Now().Add(time.Duration(backoffMs) * time.Millisecond)

	return nextRunAt
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

	nextRunAt := w.GetRetrySchedule(&task, retryPolicy)

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
	defer tx.Rollback(w.Ctx)

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

func (w *Worker) Work(consumerFactory ConsumerFactory) (err error) {
	tasks, err := w.Poll()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	for _, task := range tasks {
		wg.Add(1)

		go func(t Task) {
			defer wg.Done()
			if err = w.HandleUnit(consumerFactory(), task); err != nil {
				slog.Error("could not process task", slog.Any("error", err))
			}
		}(task)

	}

	wg.Wait()

	return nil
}
