package channels

import (
	"context"
	"github.com/InjectiveLabs/injective-pubsub/pubsub"
	"sync"
)

type ChannelPublisher struct {
	subscribers map[*ChannelSubscriber]struct{}
	messages    chan interface{}
	closed      chan struct{}
	mx          sync.RWMutex
	isClosed    bool
}

func NewChannelPublisher() *ChannelPublisher {
	return &ChannelPublisher{
		subscribers: make(map[*ChannelSubscriber]struct{}),
		messages:    make(chan interface{}),
		closed:      make(chan struct{}),
	}
}

func (s *ChannelPublisher) Publish(ctx context.Context, msg interface{}) error {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.isClosed {
		return pubsub.ErrPublisherClosed
	}

	for subscriber := range s.subscribers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case subscriber.buffer <- msg:
		default:
			subscriber.Drop()
			delete(s.subscribers, subscriber)
		}
	}

	return nil
}

func (s *ChannelPublisher) AddSubscriber(bufferSize int) (*ChannelSubscriber, error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.isClosed {
		return nil, pubsub.ErrPublisherClosed
	}

	buffer := make(chan interface{}, bufferSize)
	subscriber := NewChannelSubscriber(buffer)
	s.subscribers[subscriber] = struct{}{}

	return subscriber, nil
}

func (s *ChannelPublisher) close() {
	s.mx.Lock()
	defer s.mx.Unlock()
	if s.isClosed {
		return
	}

	var wg sync.WaitGroup
	for subscriber := range s.subscribers {
		wg.Add(1)
		go func(subscriber *ChannelSubscriber) {
			defer wg.Done()
			subscriber.stopWithErr(pubsub.ErrPublisherClosed)
		}(subscriber)
	}

	wg.Wait()
	s.subscribers = nil
	s.isClosed = true
	close(s.closed)
}

func (s *ChannelPublisher) IsClosed() bool {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return s.isClosed
}
