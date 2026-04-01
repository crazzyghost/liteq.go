package liteq

import "context"

// Consumer processes a single dequeued task.
type Consumer interface {
	Consume(ctx context.Context, task Task) (err error)
}

// ConsumerFactory creates a new Consumer instance for each work cycle.
type ConsumerFactory func() Consumer
