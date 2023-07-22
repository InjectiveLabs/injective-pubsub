package channels

import (
	"context"
	"github.com/InjectiveLabs/injective-pubsub/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChannelPublisher(t *testing.T) {
	t.Run("Publish_SendsMessageToSubscribers", func(t *testing.T) {
		publisher := NewChannelPublisher()

		subscriber1, err := publisher.AddSubscriber(1)
		assert.NoError(t, err)

		subscriber2, err := publisher.AddSubscriber(1)
		assert.NoError(t, err)

		msg := "test message"
		ctx := context.Background()

		err = publisher.Publish(context.Background(), msg)
		assert.NoError(t, err)

		msg1, err1 := subscriber1.Next(ctx)
		msg2, err2 := subscriber2.Next(ctx)

		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.Equal(t, msg, msg1)
		assert.Equal(t, msg, msg2)
	})

	t.Run("Publish_CancelContext", func(t *testing.T) {
		publisher := NewChannelPublisher()
		for i := 0; i < 10; i++ {
			_, err := publisher.AddSubscriber(1)
			require.NoError(t, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := publisher.Publish(ctx, "test message")
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("Publish_WithClosedSubscriber_DropsSubscriber", func(t *testing.T) {
		publisher := NewChannelPublisher()

		bufferSize := 5
		subscriber, err := publisher.AddSubscriber(bufferSize)
		assert.NoError(t, err)

		for i := 0; i < bufferSize+1; i++ {
			_ = publisher.Publish(context.Background(), i)
		}

		_, err = subscriber.Next(context.Background())
		assert.Equal(t, pubsub.ErrSubscriberDropped, err)
	})

	t.Run("Publish_ClosedPublisher_ReturnsError", func(t *testing.T) {
		publisher := NewChannelPublisher()
		publisher.close()

		msg := "test message"

		err := publisher.Publish(context.Background(), msg)
		assert.Equal(t, pubsub.ErrPublisherClosed, err)
		assert.True(t, publisher.IsClosed())

		t.Run("Publish_ClosedPublisher_Idempotent", func(t *testing.T) {
			publisher.close()
			assert.Equal(t, pubsub.ErrPublisherClosed, err)
		})
	})

	t.Run("AddSubscriber_AfterClose_ReturnsError", func(t *testing.T) {
		publisher := NewChannelPublisher()
		publisher.close()

		subscriber, err := publisher.AddSubscriber(10)
		assert.Nil(t, subscriber)
		assert.Equal(t, pubsub.ErrPublisherClosed, err)
	})

	t.Run("Close_ClosesSubscribersBuffer", func(t *testing.T) {
		publisher := NewChannelPublisher()

		subscriber, err := publisher.AddSubscriber(1)
		assert.NoError(t, err)

		publisher.close()

		_, err = subscriber.Next(context.Background())
		assert.Equal(t, pubsub.ErrPublisherClosed, err)
	})

	t.Run("IsClosed_ReturnsTrueAfterClose", func(t *testing.T) {
		publisher := NewChannelPublisher()
		assert.False(t, publisher.IsClosed())
		publisher.close()
		assert.True(t, publisher.IsClosed())
	})
}
