package liteq

import "context"

type Consumer interface {
	Consume(ctx context.Context, task Task) (err error)
}

type ConsumerFactory func() Consumer
