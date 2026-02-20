package liteq

type Consumer interface {
	Consume(task Task) (err error)
}

type ConsumerFactory func() Consumer
