package channels

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/injective-pubsub/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelBus(t *testing.T) {
	t.Run("Publish_SuccessfullySendsMessageToSubscriber", func(t *testing.T) {
		bus := NewChannelBus()
		ctx := context.Background()

		err := bus.AddTopic(ctx, "topic")
		assert.NoError(t, err)

		subscriber, err := bus.Subscribe(ctx, "topic")
		assert.NoError(t, err)

		msg := "test message"

		err = bus.Publish(ctx, "topic", msg)
		assert.NoError(t, err)

		receivedMsg, err := subscriber.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, msg, receivedMsg)
	})

	t.Run("AddTopic_ErrBusClosed", func(t *testing.T) {
		bus := NewChannelBus()
		ctx := context.Background()

		bus.Close()
		err := bus.AddTopic(ctx, "topic")
		assert.Equal(t, pubsub.ErrBusClosed, err)
	})

	t.Run("AddTopic_ErrTopicAlreadyExists", func(t *testing.T) {
		bus := NewChannelBus()
		ctx := context.Background()

		err := bus.AddTopic(ctx, "topic")
		assert.NoError(t, err)

		err = bus.AddTopic(ctx, "topic")
		assert.Equal(t, pubsub.ErrTopicAlreadyExists, err)
	})

	t.Run("Publish_SuccessfullyBufferedSubscriber", func(t *testing.T) {
		ctx := context.Background()
		bufferSize := 5
		bus := NewChannelBus(WithBufferSize(bufferSize))

		err := bus.AddTopic(ctx, "topic")
		assert.NoError(t, err)

		subscriber, err := bus.Subscribe(ctx, "topic")
		assert.NoError(t, err)

		for i := 0; i < bufferSize; i++ {
			err = bus.Publish(ctx, "topic", i)
			assert.NoError(t, err)
		}

		for i := 0; i < bufferSize; i++ {
			msg, err := subscriber.Next(ctx)
			assert.NoError(t, err)
			assert.Equal(t, i, msg)
		}
	})

	t.Run("Publish_TopicNotFound_ReturnsError", func(t *testing.T) {
		bus := NewChannelBus()

		msg := "test message"

		err := bus.Publish(context.Background(), "topic", msg)
		assert.Equal(t, pubsub.ErrTopicNotFound, err)
	})

	t.Run("Subscribe_TopicNotFound_ReturnsError", func(t *testing.T) {
		bus := NewChannelBus()

		_, err := bus.Subscribe(context.Background(), "topic")
		assert.Equal(t, pubsub.ErrTopicNotFound, err)
	})

	t.Run("Publish_ClosedBus_ReturnsError", func(t *testing.T) {
		bus := NewChannelBus()

		bus.Close()

		msg := "test message"

		err := bus.Publish(context.Background(), "topic", msg)
		assert.Equal(t, pubsub.ErrBusClosed, err)
	})

	t.Run("Close_ClosesAllPublishers", func(t *testing.T) {
		ctx := context.Background()
		bus := NewChannelBus()

		err := bus.AddTopic(ctx, "topic1")
		assert.NoError(t, err)
		err = bus.AddTopic(ctx, "topic2")
		assert.NoError(t, err)

		var subscribers []pubsub.Subscriber
		for i := 0; i < 5; i++ {
			s1, err := bus.Subscribe(ctx, "topic1")
			assert.NoError(t, err)
			s2, err := bus.Subscribe(ctx, "topic2")
			assert.NoError(t, err)
			subscribers = append(subscribers, s1, s2)
		}

		bus.Close()
		require.True(t, bus.IsClosed())

		for _, subscriber := range subscribers {
			_, err := subscriber.Next(ctx)
			assert.Equal(t, pubsub.ErrPublisherClosed, err)
		}

		t.Run("CloseIsIdempotent", func(t *testing.T) {
			bus.Close()
			assert.True(t, bus.IsClosed())
		})

		t.Run("PublishAfterClose_ReturnsError", func(t *testing.T) {
			msg := "test message"
			err := bus.Publish(ctx, "topic1", msg)
			assert.Equal(t, pubsub.ErrBusClosed, err)
		})

		t.Run("SubscribeAfterClose_ReturnsError", func(t *testing.T) {
			_, err = bus.Subscribe(ctx, "topic1")
			assert.Equal(t, pubsub.ErrBusClosed, err)
		})
	})

	t.Run("IsClosed_ReturnsTrueAfterClose", func(t *testing.T) {
		bus := NewChannelBus()
		assert.False(t, bus.IsClosed())
		bus.Close()
		assert.True(t, bus.IsClosed())
	})
}

func TestBusConcurrency(t *testing.T) {
	ctx := context.Background()
	topic1 := "topic-1"
	topic2 := "topic-2"

	bus := NewChannelBus(WithBufferSize(50))
	err := bus.AddTopic(context.Background(), topic1)
	require.NoError(t, err)
	err = bus.AddTopic(context.Background(), topic2)
	require.NoError(t, err)

	var publisherClosed uint32
	var subscriberClosed uint32
	var dropped uint32
	errors := map[error]*uint32{
		pubsub.ErrPublisherClosed:   &publisherClosed,
		pubsub.ErrSubscriberClosed:  &subscriberClosed,
		pubsub.ErrSubscriberDropped: &dropped,
	}

	var wg sync.WaitGroup
	subscriberCount := 50
	subscribers := make(map[string][]pubsub.Subscriber)
	for _, topic := range []string{topic1, topic2} {
		for i := 0; i < subscriberCount; i++ {
			sub, err := bus.Subscribe(context.Background(), topic)
			require.NoError(t, err)
			subscribers[topic] = append(subscribers[topic], sub)

			wg.Add(1)
			go func(index int) {
				var last int
				defer wg.Done()
				for {
					msg, err := sub.Next(ctx)
					if err != nil {
						atomic.AddUint32(errors[err], 1)
						break
					}
					assert.Equal(t, last+1, msg.(int), "messages are not ordered")
					last++

					// 10 subscribers per topic will be slow and be dropped
					if index >= 40 {
						time.Sleep(2 * time.Millisecond)

						// 5 subscribers will close the subscription
					} else if index >= 35 && last == 500 {
						sub.Close()
					}
				}
			}(i)
		}
	}

	// publish continuously until the bus is closed
	for _, topic := range []string{topic1, topic2} {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			var (
				msg int
				err error
			)
			for {
				msg++
				err = bus.Publish(ctx, topic, msg)
				if err == pubsub.ErrBusClosed {
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(topic)
	}

	// Close the bus and wait
	<-time.After(time.Second)
	bus.Close()
	wg.Wait()

	// counter should match
	assert.EqualValues(t, 20, dropped)
	assert.EqualValues(t, 10, subscriberClosed)
	assert.EqualValues(t, 70, publisherClosed)
}
