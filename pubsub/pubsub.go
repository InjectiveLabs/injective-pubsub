package pubsub

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/InjectiveLabs/metrics"
)

var (
	ErrTopicNotFound      = errors.New("topic not found")
	ErrSubscriberNotFound = errors.New("subscriber not found")
)

type EventBus interface {
	EventSubscriber

	AddTopic(name string, src <-chan interface{}) error
	Subscribe(name string) (<-chan interface{}, int, error)
	Unsubscribe(name string, subID int) error
	Topics() []string
}

type EventSubscriber interface {
	EventSubscribe(topic string) (msgs <-chan interface{}, subID int, err error)
	EventUnsubscribe(topic string, subID int) error
}

type subscriber struct {
	ch     chan<- interface{}
	closed uint32
}

func (s *subscriber) close() {
	if atomic.SwapUint32(&s.closed, 1) == 1 {
		return
	}
	close(s.ch)
	s.ch = nil
}

func (s *subscriber) send(msg interface{}) {
	if atomic.LoadUint32(&s.closed) == 1 {
		return
	}

	s.ch <- msg
}

type MemEventBus struct {
	topics                 map[string]<-chan interface{}
	topicsMux              *sync.RWMutex
	subscribers            map[string]map[int]*subscriber
	subscribersMux         *sync.RWMutex
	ids                    int
	slowSubscriberMaxDelay time.Duration

	svcTags metrics.Tags
}

func NewEventBus(
	slowSubscriberMaxDelay time.Duration,
) *MemEventBus {
	return &MemEventBus{
		topics:                 make(map[string]<-chan interface{}),
		topicsMux:              new(sync.RWMutex),
		subscribers:            make(map[string]map[int]*subscriber),
		subscribersMux:         new(sync.RWMutex),
		slowSubscriberMaxDelay: slowSubscriberMaxDelay,
		svcTags: metrics.Tags{
			"svc": "pubsub",
		},
	}
}

func (m *MemEventBus) Topics() (topics []string) {
	m.topicsMux.RLock()
	defer m.topicsMux.RUnlock()

	topics = make([]string, 0, len(m.topics))
	for topicName := range m.topics {
		topics = append(topics, topicName)
	}

	return topics
}

func (m *MemEventBus) AddTopic(name string, src <-chan interface{}) error {
	m.topicsMux.Lock()
	_, ok := m.topics[name]
	if ok {
		m.topicsMux.Unlock()
		return errors.New("topic already registered")
	}
	m.topics[name] = src
	m.topicsMux.Unlock()
	m.publishTopic(name, src)
	return nil
}

func (m *MemEventBus) Subscribe(topic string) (<-chan interface{}, int, error) {

	m.topicsMux.RLock()
	_, ok := m.topics[topic]
	m.topicsMux.RUnlock()

	if !ok {
		return nil, 0, ErrTopicNotFound
	}

	inCh := make(chan interface{})
	outCh := make(chan interface{})
	m.subscribersMux.Lock()
	defer m.subscribersMux.Unlock()

	m.ids++
	if m.ids <= 0 {
		m.ids = 1
	}
	subID := m.ids
	if m.subscribers[topic] == nil {
		m.subscribers[topic] = make(map[int]*subscriber)
	}
	s := &subscriber{ch: inCh}
	m.subscribers[topic][subID] = s

	// here we use a double channel to avoid blocking the publisher
	go func() {
		defer close(outCh)
		metrics.ReportClosureFuncCall("subscriber_publish", m.svcTags)
		doneFn := metrics.ReportClosureFuncTiming("subscriber_publish", m.svcTags)
		defer doneFn()

		for msg := range inCh {
			timeout := time.NewTimer(m.slowSubscriberMaxDelay)

			select {
			case outCh <- msg:
				timeout.Stop()

			case <-timeout.C:
				timeout.Stop()

				metrics.SlowSubscriberEventsDropped(1, m.svcTags)
				s.close()
				return
			}
		}
	}()

	return outCh, subID, nil
}

func (m *MemEventBus) Unsubscribe(topic string, subID int) error {
	m.subscribersMux.Lock()
	defer m.subscribersMux.Unlock()

	topicSubscribers := m.subscribers[topic]
	s, ok := topicSubscribers[subID]
	if !ok {
		return fmt.Errorf("%w: to %s with ID %d", ErrSubscriberNotFound, topic, subID)
	}

	delete(topicSubscribers, subID)
	s.close()

	return nil
}

func (m *MemEventBus) EventUnsubscribe(topic string, subID int) error {
	return m.Unsubscribe(topic, subID)
}

// EventSubscribe implements EventSubscriber
func (m *MemEventBus) EventSubscribe(topic string) (msgs <-chan interface{}, subID int, err error) {
	return m.Subscribe(topic)
}

func (m *MemEventBus) publishTopic(topic string, src <-chan interface{}) {
	go func() {
		for {
			select {
			case msg, ok := <-src:
				if !ok {
					m.topicsMux.Lock()
					delete(m.topics, topic)
					m.closeAllSubscribers(topic)
					m.topicsMux.Unlock()
					return
				}
				// this cant be  parallelized because we need to keep messages ordered
				m.publishAllSubscribers(topic, msg)
			}
		}
	}()
}

func (m *MemEventBus) closeAllSubscribers(topic string) {
	m.subscribersMux.Lock()
	subscribers := m.subscribers[topic]
	delete(m.subscribers, topic)
	m.subscribersMux.Unlock()

	for _, sub := range subscribers {
		sub.close()
	}
}

func (m *MemEventBus) publishAllSubscribers(topic string, msg interface{}) {
	m.subscribersMux.RLock()
	defer m.subscribersMux.RUnlock()

	for _, sub := range m.subscribers[topic] {
		sub.send(msg)
	}
}
