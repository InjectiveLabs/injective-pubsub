package pubsub

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAddTopic(t *testing.T) {
	assert := assert.New(t)

	q := NewEventBus(time.Duration(time.Hour))

	err := q.AddTopic("kek", make(chan interface{}))
	if !assert.NoError(err) {
		return
	}

	err = q.AddTopic("lol", make(chan interface{}))
	if !assert.NoError(err) {
		return
	}

	err = q.AddTopic("lol", make(chan interface{}))
	if !assert.Error(err) {
		return
	}

	// sometimes, order does not match -> need sort
	topics := q.Topics()
	sort.Slice(topics, func(i, j int) bool {
		return topics[i] < topics[j]
	})

	assert.EqualValues([]string{"kek", "lol"}, topics)
}

func TestSubscribe(t *testing.T) {
	assert := assert.New(t)

	q := NewEventBus(time.Duration(time.Hour))

	kekSrc := make(chan interface{})
	q.AddTopic("kek", kekSrc)

	lolSrc := make(chan interface{})
	q.AddTopic("lol", lolSrc)

	kekSubC, _, err := q.Subscribe("kek")
	if !assert.NoError(err) {
		return
	}

	lolSubC, _, err := q.Subscribe("lol")
	if !assert.NoError(err) {
		return
	}

	lol2SubC, _, err := q.Subscribe("lol")
	if !assert.NoError(err) {
		return
	}

	wg := new(sync.WaitGroup)
	wg.Add(4)

	go func() {
		defer wg.Done()
		msg := <-kekSubC
		log.Println("kek:", msg)
		assert.EqualValues(1, msg)
	}()

	go func() {
		defer wg.Done()
		msg := <-lolSubC
		log.Println("lol:", msg)
		assert.EqualValues(1, msg)
	}()

	go func() {
		defer wg.Done()
		msg := <-lol2SubC
		log.Println("lol2:", msg)
		assert.EqualValues(1, msg)
	}()

	go func() {
		defer wg.Done()

		time.Sleep(time.Second)
		kekSrc <- 1
		lolSrc <- 1

		time.Sleep(time.Second)
		close(kekSrc)
		close(lolSrc)
	}()

	wg.Wait()
	time.Sleep(time.Second)
}

func TestGracefulShutdown(t *testing.T) {
	q := NewEventBus(time.Hour)

	topic := make(chan interface{})
	err := q.AddTopic("topic", topic)
	if err != nil {
		t.Fatal(err)
	}

	// SUBSCRIBERS
	subNumber := 10
	// 10 subscribers
	subscribersMap := make(map[int]<-chan interface{})
	for i := 0; i < subNumber; i++ {
		c, id, err := q.Subscribe("topic")
		if err != nil {
			t.Fatal(err)
		}
		subscribersMap[id] = c
	}

	cwg := new(sync.WaitGroup)
	// clients receive messages
	resultMap := make(map[int]int)
	m := new(sync.Mutex)
	for sid, top := range subscribersMap {
		cwg.Add(1)
		go func(subId int, topic <-chan interface{}) {
			for {
				select {
				case msg, ok := <-topic:
					if !ok {
						fmt.Println("closed:", subId)
						cwg.Done()
						return
					}
					m.Lock()
					resultMap[subId] = msg.(int)
					m.Unlock()
				}
			}
		}(sid, top)
	}

	// PUBLISHER
	pctx, _ := context.WithTimeout(context.Background(), time.Millisecond*1000)
	// publish messages to topic
	var i = 0
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-pctx.Done():
				// no more messages
				// publisher explicitly closes topic
				close(topic)
				return
			default:
				i++
				topic <- i
			}
		}
	}()
	wg.Wait()
	cwg.Wait()
	// assert that all subscribers got all messages
	for _, v := range resultMap {
		if v != i {
			t.Fatal("not all subscribers got all messages")
		}
	}
}

func TestSlowSubscriber(t *testing.T) {
	q := NewEventBus(time.Second)

	topic := make(chan interface{})
	err := q.AddTopic("topic", topic)
	if err != nil {
		t.Fatal(err)
	}

	subCh, _, err := q.Subscribe("topic")
	if err != nil {
		t.Fatal(err)
	}

	cwg := new(sync.WaitGroup)
	// clients receive messages
	cwg.Add(1)
	go func() {
		for {
			select {
			case _, ok := <-subCh:
				if !ok {
					cwg.Done()
					return
				}
				// simulate slow subscriber
				time.Sleep(time.Second * 2)
			}
		}
	}()

	// PUBLISHER
	// publish messages to topic
	var i = 0
	topic <- i
	i++
	topic <- i
	cwg.Wait()
}
