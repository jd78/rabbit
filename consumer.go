package rabbit

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

type Handler func(message interface{})

type IConsumer interface {
	AddHandler(messageType string, handler Handler)
	StartConsuming(queue string, ack, activePassive bool, concurrentConsumers int, args map[string]interface{}) string
}

type consumer struct {
	handlers        map[string]Handler
	log             *rabbitLogger
	channel         *amqp.Channel
	consumerRunning bool
}

func (r *rabbit) configureConsumer(prefetch int) IConsumer {
	channel, err := r.connection.Channel()
	checkError(err, "Error creating the producing channel", r.log)

	qErr := channel.Qos(prefetch, 0, false)
	checkError(qErr, "Error assigning prefetch on the channel", r.log)

	go func() {
		ch := make(chan *amqp.Error)
		channel.NotifyClose(ch)
		err := <-ch
		r.log.err(fmt.Sprintf("Channel closed - Error=%s", err.Error()))
		panic("Channel closed")
	}()

	go func() {
		ch := make(chan bool)
		channel.NotifyFlow(ch)
		for {
			status := <-ch
			r.log.warn(fmt.Sprintf("channel flow detected - flow enabled: %t", status))
		}
	}()

	h := make(map[string]Handler)
	return &consumer{h, r.log, channel, false}
}

func (c *consumer) handlerExists(messageType string) bool {
	_, exists := c.handlers[messageType]
	return exists
}

func (c *consumer) AddHandler(messageType string, handler Handler) {
	if c.handlerExists(messageType) {
		err := fmt.Sprintf("messageType %s already mapped", messageType)
		c.log.err(err)
		panic(err)
	}
	c.handlers[messageType] = handler
}

func (c *consumer) StartConsuming(queue string, ack, activePassive bool, concurrentConsumers int, args map[string]interface{}) string {
	if c.consumerRunning {
		err := errors.New("Consumer already running, please configure a new consumer for concurrent processing")
		checkError(err, "Error starting the consumer", c.log)
	}

	b := make([]byte, 4) //equals 8 charachters
	rand.Read(b)
	s := hex.EncodeToString(b)

	delivery, err := c.channel.Consume(queue, s, !ack, activePassive, false, false, args)
	checkError(err, "Error starting the consumer", c.log)

	for i := 0; i < concurrentConsumers; i++ {
		go func(work <-chan amqp.Delivery) {
			for w := range work {
				if !c.handlerExists(w.Type) {
					if ack {
						w.Ack(false) //TODO error
					}
					return
				}

				handler := c.handlers[w.Type]
				obj, err := deserialize(w.Body, ContentType(w.ContentType))
				if err != nil {
					//TODO!!
				}
				handler(obj) //TODO check error
				if ack {
					w.Ack(false) //TODO error
				}
			}
		}(delivery)
	}

	return s
}
