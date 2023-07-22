package pubsub

import (
	"context"
	"errors"
)

var (
	ErrBusClosed          = errors.New("bus closed")
	ErrTopicNotFound      = errors.New("topic not found")
	ErrPublisherClosed    = errors.New("publisher closed")
	ErrSubscriberDropped  = errors.New("subscriber dropped")
	ErrSubscriberClosed   = errors.New("subscriber closed")
	ErrTopicAlreadyExists = errors.New("topic already exists")
)

type Publisher interface {
	Publish(ctx context.Context, msg interface{}) error
}

type Subscriber interface {
	Next(ctx context.Context) (interface{}, error)
	Close()
}

type TopicPublisher interface {
	Publish(ctx context.Context, topic string, msg interface{}) error
}

type TopicSubscriber interface {
	Subscribe(ctx context.Context, topic string) (Subscriber, error)
}

type Bus interface {
	TopicPublisher
	TopicSubscriber
}
