package liteq

import "fmt"

type MaxRetriesExceededError struct {
	Retries    int
	MaxRetries int
}

func (e *MaxRetriesExceededError) Error() string {
	return fmt.Sprintf("max retries exceeded: %d/%d", e.Retries, e.MaxRetries)
}

type ConsumerError struct {
	Source         error
	IsNonTransient bool
}

func (e *ConsumerError) Error() string {
	return fmt.Sprintf("consumer error: %v", e.Source)
}

func (e *ConsumerError) Unwrap() error {
	return e.Source
}
