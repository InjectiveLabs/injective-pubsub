package channels

import (
	"context"
	"testing"
	"time"

	"github.com/InjectiveLabs/injective-pubsub/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestChannelSubscriber(t *testing.T) {
	t.Run("Next_ReturnsMessage", func(t *testing.T) {
		buffer := make(chan interface{}, 1)
		subscriber := NewChannelSubscriber(buffer)

		expectedMsg := "test message"

		go func() {
			buffer <- expectedMsg
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		receivedMsg, err := subscriber.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedMsg, receivedMsg)
	})

	t.Run("Next_ContextCancelled_ReturnsError", func(t *testing.T) {
		buffer := make(chan interface{}, 1)
		subscriber := NewChannelSubscriber(buffer)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the context immediately

		receivedMsg, err := subscriber.Next(ctx)
		assert.Nil(t, receivedMsg)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("Next_SubscriberClosed_ReturnsError", func(t *testing.T) {
		buffer := make(chan interface{}, 1)
		subscriber := NewChannelSubscriber(buffer)

		subscriber.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		receivedMsg, err := subscriber.Next(ctx)
		assert.Nil(t, receivedMsg)
		assert.Equal(t, pubsub.ErrSubscriberClosed, err)
	})

	t.Run("Next_SubscriberDropped_ReturnsError", func(t *testing.T) {
		buffer := make(chan interface{}, 1)
		subscriber := NewChannelSubscriber(buffer)

		subscriber.Drop()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		receivedMsg, err := subscriber.Next(ctx)
		assert.Nil(t, receivedMsg)
		assert.Equal(t, pubsub.ErrSubscriberDropped, err)
	})

	t.Run("Next_Timeout_ReturnsError", func(t *testing.T) {
		buffer := make(chan interface{}, 1)
		subscriber := NewChannelSubscriber(buffer)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		receivedMsg, err := subscriber.Next(ctx)
		assert.Nil(t, receivedMsg)
		assert.Equal(t, context.DeadlineExceeded, err)
	})

	t.Run("IsClosed_ReturnsTrueAfterClose", func(t *testing.T) {
		buffer := make(chan interface{}, 1)
		subscriber := NewChannelSubscriber(buffer)

		assert.False(t, subscriber.IsClosed())
		subscriber.Close()
		assert.True(t, subscriber.IsClosed())

		t.Run("CloseIsIdempotent", func(t *testing.T) {
			subscriber.Close()
			assert.True(t, subscriber.IsClosed())
		})
	})
}
