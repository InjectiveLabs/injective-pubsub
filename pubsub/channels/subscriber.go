package channels

import (
	"context"
	"github.com/InjectiveLabs/injective-pubsub/pubsub"
	"sync"
)

type ChannelSubscriber struct {
	buffer   chan interface{}
	closed   chan struct{}
	closeErr error
	mx       sync.RWMutex
	isClosed bool
}

func NewChannelSubscriber(buffer chan interface{}) *ChannelSubscriber {
	return &ChannelSubscriber{
		buffer: buffer,
		closed: make(chan struct{}),
	}
}

func (s *ChannelSubscriber) Next(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, s.closeErr
	case msg := <-s.buffer:
		// we need this double check to prevents dropped/stopped consumers from slowly
		// draining the buffer channel
		select {
		case <-s.closed:
			return nil, s.closeErr
		default:
		}

		return msg, nil
	}
}

func (s *ChannelSubscriber) Close() {
	s.stopWithErr(pubsub.ErrSubscriberClosed)
}

func (s *ChannelSubscriber) Drop() {
	s.stopWithErr(pubsub.ErrSubscriberDropped)
}

func (s *ChannelSubscriber) stopWithErr(err error) {
	s.mx.Lock()
	defer s.mx.Unlock()
	if s.isClosed {
		return
	}

	s.isClosed = true
	s.closeErr = err
	close(s.closed)
}

func (s *ChannelSubscriber) IsClosed() bool {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return s.isClosed
}
