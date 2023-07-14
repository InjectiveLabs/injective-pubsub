package pubsub

import (
	"sync"
	"time"

	"github.com/InjectiveLabs/metrics"
	"github.com/avast/retry-go"

	log "github.com/InjectiveLabs/suplog"
	"github.com/pkg/errors"
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

type memEventBus struct {
	topics                 map[string]<-chan interface{}
	topicsMux              *sync.RWMutex
	subscribers            map[string]map[int]chan<- interface{}
	subscribersMux         *sync.RWMutex
	ids                    uint64
	slowSubscriberMaxDelay time.Duration

	svcTags metrics.Tags
}

func NewEventBus(
	slowSubscriberMaxDelay time.Duration,
) EventBus {
	return &memEventBus{
		topics:                 make(map[string]<-chan interface{}),
		topicsMux:              new(sync.RWMutex),
		subscribers:            make(map[string]map[int]chan<- interface{}),
		subscribersMux:         new(sync.RWMutex),
		slowSubscriberMaxDelay: slowSubscriberMaxDelay,

		svcTags: metrics.Tags{
			"svc": "pubsub",
		},
	}
}

func (m *memEventBus) Topics() (topics []string) {
	m.topicsMux.RLock()
	defer m.topicsMux.RUnlock()

	topics = make([]string, 0, len(m.topics))
	for topicName := range m.topics {
		topics = append(topics, topicName)
	}

	return topics
}

func (m *memEventBus) AddTopic(name string, src <-chan interface{}) error {
	m.topicsMux.RLock()
	_, ok := m.topics[name]
	m.topicsMux.RUnlock()

	if ok {
		return errors.New("topic already registered")
	}

	m.topicsMux.Lock()
	m.topics[name] = src
	m.topicsMux.Unlock()

	go m.publishTopic(name, src)

	return nil
}

var errNoTopic = errors.New("topic not found")

func (m *memEventBus) Subscribe(topic string) (<-chan interface{}, int, error) {
	if err := retry.Do(
		func() (err error) {
			m.topicsMux.RLock()
			_, ok := m.topics[topic]
			m.topicsMux.RUnlock()

			if !ok {
				return errNoTopic
			}

			return nil
		},
		retry.Attempts(5),
		retry.Delay(1*time.Millisecond),
		retry.MaxDelay(100*time.Millisecond),
		retry.DelayType(retry.BackOffDelay),
	); err != nil {
		return nil, 0, errors.Errorf("topic not found: %s", topic)
	}

	ch := make(chan interface{})
	m.subscribersMux.Lock()
	defer m.subscribersMux.Unlock()

	m.ids++
	subID := int(m.ids)
	if m.subscribers[topic] == nil {
		m.subscribers[topic] = make(map[int]chan<- interface{})
	}
	m.subscribers[topic][subID] = ch

	return ch, subID, nil
}

func (m *memEventBus) Unsubscribe(topic string, subID int) error {
	m.subscribersMux.Lock()
	defer m.subscribersMux.Unlock()

	topicSubscribers := m.subscribers[topic]
	ch, ok := topicSubscribers[subID]
	if ok {
		delete(topicSubscribers, subID)
		close(ch)
	} else {
		err := errors.Errorf("subscriber to %s with ID %d not found", topic, subID)
		return err
	}

	return nil
}

func (m *memEventBus) EventUnsubscribe(topic string, subID int) error {
	return m.Unsubscribe(topic, subID)
}

// EventSubscribe implements EventSubscriber
func (m *memEventBus) EventSubscribe(topic string) (msgs <-chan interface{}, subID int, err error) {
	return m.Subscribe(topic)
}

func (m *memEventBus) publishTopic(topic string, src <-chan interface{}) {
	for {
		msg, ok := <-src
		if !ok {
			m.closeAllSubscribers(topic)
			m.topicsMux.Lock()
			delete(m.topics, topic)
			m.topicsMux.Unlock()

			return
		}
		go m.publishAllSubscribers(topic, msg)
	}
}

func (m *memEventBus) closeAllSubscribers(topic string) {
	m.subscribersMux.Lock()
	defer m.subscribersMux.Unlock()

	subscribers := m.subscribers[topic]
	delete(m.subscribers, topic)

	for _, sub := range subscribers {
		close(sub)
	}
}

func copyMap(src map[int]chan<- interface{}) map[int]chan<- interface{} {
	dst := make(map[int]chan<- interface{}, len(src))

	for k, v := range src {
		dst[k] = v
	}

	return dst
}

func (m *memEventBus) publishAllSubscribers(topic string, msg interface{}) {
	var subscribers map[int]chan<- interface{}

	m.subscribersMux.RLock()
	subscribers = copyMap(m.subscribers[topic])
	m.subscribersMux.RUnlock()

	// if there is no subscribers for this topic, simply drop msg
	if len(subscribers) == 0 {
		return
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(subscribers))

	for _, sub := range subscribers {
		go func(subscriber chan<- interface{}) {
			metrics.ReportClosureFuncCall("subscriber_publish", m.svcTags)
			doneFn := metrics.ReportClosureFuncTiming("subscriber_publish", m.svcTags)
			defer doneFn()

			defer wg.Done()

			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						log.WithError(err).Warningln("recovered in publishAllSubscribers")
					} else {
						log.WithField("error", r).Warningln("recovered in publishAllSubscribers")
					}
				}
			}()

			timeout := time.NewTimer(m.slowSubscriberMaxDelay)
			defer timeout.Stop()

			select {
			case subscriber <- msg:
			case <-timeout.C:
				metrics.SlowSubscriberEventsDropped(1, m.svcTags)
			}
		}(sub)
	}

	wg.Wait()
}
