package pubsub

import (
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
