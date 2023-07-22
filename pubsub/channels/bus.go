package channels

import (
	"context"
	"sync"

	"github.com/InjectiveLabs/injective-pubsub/pubsub"
)

var DefaultBufferSize = 10

type ChannelBus struct {
	cfg        Config
	publishers map[string]*ChannelPublisher
	mx         sync.RWMutex
	isClosed   bool
}

type Config struct {
	BufferSize int
}

type Option func(*Config)

func WithBufferSize(bufferSize int) Option {
	return func(cfg *Config) {
		cfg.BufferSize = bufferSize
	}
}

func NewChannelBus(opts ...Option) *ChannelBus {
	cfg := Config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return NewChannelBusWithConfig(cfg)
}

func NewChannelBusWithConfig(cfg Config) *ChannelBus {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultBufferSize
	}
	return &ChannelBus{
		cfg:        cfg,
		publishers: make(map[string]*ChannelPublisher),
	}
}

func (b *ChannelBus) Publish(ctx context.Context, topic string, msg interface{}) error {
	b.mx.RLock()
	if b.isClosed {
		b.mx.RUnlock()
		return pubsub.ErrBusClosed
	}

	p, ok := b.publishers[topic]
	b.mx.RUnlock()

	if !ok {
		return pubsub.ErrTopicNotFound
	}

	// publisher could be closed by now, it will check this on its own mutex
	return p.Publish(ctx, msg)
}

func (b *ChannelBus) AddTopic(_ context.Context, topic string) error {
	b.mx.Lock()
	defer b.mx.Unlock()

	if b.isClosed {
		return pubsub.ErrBusClosed
	}

	_, ok := b.publishers[topic]
	if ok {
		return pubsub.ErrTopicAlreadyExists
	}

	b.publishers[topic] = NewChannelPublisher()
	return nil
}

func (b *ChannelBus) Subscribe(_ context.Context, topic string) (pubsub.Subscriber, error) {
	b.mx.Lock()
	defer b.mx.Unlock()

	if b.isClosed {
		return nil, pubsub.ErrBusClosed
	}

	p, ok := b.publishers[topic]
	if !ok {
		return nil, pubsub.ErrTopicNotFound
	}

	return p.AddSubscriber(b.cfg.BufferSize)
}

func (b *ChannelBus) Close() {
	b.mx.Lock()
	defer b.mx.Unlock()
	if b.isClosed {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(b.publishers))
	for _, publisher := range b.publishers {
		go func(publisher *ChannelPublisher) {
			publisher.close()
			wg.Done()
		}(publisher)
	}

	b.publishers = nil
	b.isClosed = true
	wg.Wait()
}

func (b *ChannelBus) IsClosed() bool {
	b.mx.RLock()
	defer b.mx.RUnlock()
	return b.isClosed
}
