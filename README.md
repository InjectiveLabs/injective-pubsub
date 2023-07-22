# Injective Pubsub

The `pubsub` package is a simple publish-subscribe system built in Go. It allows you to send messages from publishers to subscribers over topics, with built-in features like automatic dropping of slow subscribers and safe closing of publishers and the bus.

## Installation

```shell
go get github.com/InjectiveLabs/injective-pubsub
```

## Usage

### Creating a Bus

```go
import (
    "github.com/InjectiveLabs/injective-pubsub/channels"
)

// Create a new bus with a buffer size of 100.
bus := channels.NewChannelBus(channels.WithBufferSize(100))
```

### Adding a Topic

```go
import (
    "context"
    "github.com/InjectiveLabs/injective-pubsub/pubsub"
)

err := bus.AddTopic(context.Background(), "topic")
if err != nil {
    // handle error
}
```

### Subscribing to a Topic

```go
sub, err := bus.Subscribe(context.Background(), "topic")
if err != nil {
    // handle error
}
```

#### Unsubscribing from a Topic
```go
sub.Close()
```
NOTE: After closing a subscription, any call to `Next` will return `pubsub.ErrSubscriberClosed`.

### Publishing to a Topic

```go
msg := "some payload"
err := bus.Publish(context.Background(), "topic", msg)
if err != nil {
    // handle error
}
```

### Receiving Messages

```go
msg, err := sub.Next(context.Background())
if err != nil {
    // handle error
}
fmt.Println(msg)  // "some payload"
```

### Closing the Bus
Closing the bus will free all resources and close all publishers and subscribers.
Attempting to perform any operation on a closed bus will return an error.
```go
bus.Close()
```

## Error Handling

The `injective-pubsub` package provides several error types that can be used to handle specific situations:

- `pubsub.ErrBusClosed` is returned when an operation is attempted on a closed bus.
- `pubsub.ErrTopicNotFound` is returned when an operation is attempted on a non-existent topic.
- `pubsub.ErrPublisherClosed` is returned when an operation is attempted on a closed publisher.
- `pubsub.ErrSubscriberDropped` is returned when a subscriber is dropped due to being slow.
- `pubsub.ErrSubscriberClosed` is returned when an operation is attempted on a closed subscriber.
- `pubsub.ErrTopicAlreadyExists` is returned when attempting to add a topic that already exists.

## Tests

Tests can be run using the Go test tool:

```shell
make test
```
