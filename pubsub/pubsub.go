package pubsub

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	DefaultPublisherBuffer = 100
	DefaultCloseTimeout    = 5 * time.Second
)

var (
	ErrBusClosed          = errors.New("bus closed")
	ErrTopicNotFound      = errors.New("topic not found")
	ErrPublisherClosed    = errors.New("publisher closed")
	ErrSubscriberDropped  = errors.New("subscriber dropped")
	ErrSubscriberClosed   = errors.New("subscriber closed")
	ErrTopicAlreadyExists = errors.New("topic already exists")
)

type Message struct {
	ID      []byte
	Payload interface{}
}

type Subscriber interface {
	// Next blocks until a message is received or:
	//	- the context is canceled, returning the context error
	//	- the subscriber is closed, returning ErrSubscriberClosed
	//	- the publisher closes and the bus configuration doesn't wait on subscribers
	Next(ctx context.Context) (*Message, error)

	// Close closes the subscriber and releases any resources held.
	//
	// If close is while Next is waiting on a message, Next will
	// return with ErrSubscriberClosed.
	Close()
}

type Bus interface {
	Publish(ctx context.Context, topic string, msg *Message) error
	Subscribe(ctx context.Context, topic string) (Subscriber, error)
	AddTopic(ctx context.Context, topic string) error
}

type MemBusConfig struct {
	// PublisherBuffer is the buffer size for each topic publisher.
	PublisherBuffer int

	// SubscriberBuffer is the buffer size for each subscriber.
	SubscriberBuffer int

	// DropSlowSubscribers will close and drop a subscriber if their buffer is full.
	DropSlowSubscribers bool

	// CloseTimeout sets the timeout for waiting on subscribers to receive all buffered messages
	//  when closing the bus.
	CloseTimeout time.Duration
}

type MemBus struct {
	cfg    *MemBusConfig
	topics map[string]*memPublisher
	closed bool
	mx     sync.RWMutex
}

func NewMemBus(cfg MemBusConfig) *MemBus {
	return &MemBus{
		cfg:    &cfg,
		topics: make(map[string]*memPublisher),
	}
}

func (m *MemBus) AddTopic(_ context.Context, topic string) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	if m.closed {
		return ErrBusClosed
	}
	if m.topics[topic] != nil {
		return ErrTopicAlreadyExists
	}

	p := newMemPublisher(m.cfg)
	p.run()

	m.topics[topic] = p
	return nil
}

func (m *MemBus) Subscribe(_ context.Context, topic string) (Subscriber, error) {
	m.mx.Lock()
	defer m.mx.Unlock()

	if m.closed {
		return nil, ErrBusClosed
	}
	p, ok := m.topics[topic]
	if !ok {
		return nil, ErrTopicNotFound
	}

	s := newMemSubscriber(m.cfg.SubscriberBuffer)

	if err := p.addSubscriber(s); err != nil {
		return nil, err
	}

	return s, nil
}

func (m *MemBus) Publish(ctx context.Context, topic string, msg *Message) error {
	m.mx.RLock()
	if m.closed {
		m.mx.RUnlock()
		return ErrBusClosed
	}
	p, ok := m.topics[topic]
	m.mx.RUnlock()

	if !ok {
		return ErrTopicNotFound
	}

	return p.publish(ctx, msg)
}

func (m *MemBus) Close() {
	m.mx.Lock()
	defer m.mx.Unlock()

	if m.closed {
		return
	}
	m.closed = true

	t := m.cfg.CloseTimeout
	if t == 0 {
		t = DefaultCloseTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()

	var wg sync.WaitGroup
	for _, p := range m.topics {
		wg.Add(1)
		go func(p *memPublisher) {
			defer wg.Done()
			p.close(ctx)
		}(p)
	}
	wg.Wait()
	m.topics = nil
}

type memPublisher struct {
	cfg        *MemBusConfig
	subscriber []*memSubscriber
	messages   chan *Message
	mx         sync.RWMutex
	wg         sync.WaitGroup
	closed     bool
	done       chan struct{}
	stop       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

func newMemPublisher(cfg *MemBusConfig) *memPublisher {
	return &memPublisher{
		cfg:      cfg,
		messages: make(chan *Message),
	}
}

func (p *memPublisher) publish(ctx context.Context, msg *Message) error {
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return ErrPublisherClosed
	}
	// we need to add the wait group before unlocking or calling
	// close may panic on thw workgroup wait
	p.wg.Add(1)
	p.mx.Unlock()
	defer p.wg.Done()

	select {
	case <-p.stop:
		// wrong usage, bus closed while publishing
		return ErrBusClosed
	case <-ctx.Done():
		return ctx.Err()
	case p.messages <- msg:
		return nil
	}
}

func (p *memPublisher) run() {
	// fanout is called before the publisher is available, no need to lock
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-p.stop:
				return
			case msg := <-p.messages:
				p.fanoutToSubscribers(msg)
			}
		}
	}()
}

func (p *memPublisher) fanoutToSubscribers(msg *Message) {
	p.mx.Lock()
	defer p.mx.Unlock()

	var wg sync.WaitGroup
	for i, s := range p.subscriber {
		wg.Add(1)
		go func(i int, s *memSubscriber) {
			defer wg.Done()
			select {
			case s.buffer <- msg:
				return
			case <-p.stop:
			case <-s.stop:
			default:
				s.stopWithErr(ErrSubscriberDropped)
			}
			p.subscriber[i] = p.subscriber[len(p.subscriber)-1]
			p.subscriber = p.subscriber[:len(p.subscriber)-1]
		}(i, s)
	}
	wg.Wait()
}

func (p *memPublisher) close(ctx context.Context) {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return
	}
	p.closed = true
	close(p.stop)

	p.wg.Wait()
}

func (p *memPublisher) addSubscriber(subscriber *memSubscriber) error {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		// wrong usage, publisher closed while subscribing
		return ErrPublisherClosed
	}

	p.subscriber = append(p.subscriber, subscriber)

	p.wg.Add(1)
	go func() {
		for {
			select {
			case <-p.stop:
				return
			}
		}
	}()

	return nil
}

type memSubscriber struct {
	buffer chan *Message
	mx     sync.RWMutex
	stop   chan struct{}
	done   chan struct{}
	err    error
	closed bool
}

func newMemSubscriber(maxBuffer int) *memSubscriber {
	return &memSubscriber{
		buffer: make(chan *Message, maxBuffer),
		stop:   make(chan struct{}),
	}
}

func (s *memSubscriber) Next(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.stop:
		return nil, s.err
	case msg, ok := <-s.buffer:
		if !ok {
			return nil, ErrPublisherClosed
		}
		return msg, nil
	}
}

func (s *memSubscriber) close() {
	s.stopWithErr(ErrSubscriberClosed)
}

func (s *memSubscriber) stopWithErr(err error) {
	s.mx.Lock()
	if s.closed {
		s.mx.Unlock()
		return
	}
	s.closed = true
	s.mx.Unlock()

	s.err = err
	close(s.stop)
}
