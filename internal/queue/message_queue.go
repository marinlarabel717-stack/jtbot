package queue

import (
	"context"
	"errors"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
)

type MessageQueue struct {
	jobs chan model.DMJob
}

func NewMessageQueue(size int) *MessageQueue {
	return &MessageQueue{
		jobs: make(chan model.DMJob, size),
	}
}

func (q *MessageQueue) Publish(ctx context.Context, job model.DMJob) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.jobs <- job:
		return nil
	default:
		return errors.New("queue is full")
	}
}

func (q *MessageQueue) Consume() <-chan model.DMJob {
	return q.jobs
}
