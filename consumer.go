package rabbit

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"

	"github.com/streadway/amqp"
)

type Handler func(message interface{}) HandlerResponse

type IConsumer interface {
	AddHandler(messageType string, concreteType reflect.Type, handler Handler)
	StartConsuming(queue string, ack, activePassive bool, concurrentConsumers int, args map[string]interface{}) string
}

type consumer struct {
	handlers        map[string]Handler
	types           map[string]reflect.Type
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
	t := make(map[string]reflect.Type)
	return &consumer{h, t, r.log, channel, false}
}

func (c *consumer) handlerExists(messageType string) bool {
	_, exists := c.handlers[messageType]
	return exists
}

func (c *consumer) AddHandler(messageType string, concreteType reflect.Type, handler Handler) {
	if c.handlerExists(messageType) {
		err := fmt.Sprintf("messageType %s already mapped", messageType)
		c.log.err(err)
		panic(err)
	}
	c.handlers[messageType] = handler
	c.types[messageType] = concreteType
}

//StartConsuming will start a new consumer
//concurrentConsumers will create concurrent go routines that will read from the delivery rabbit channel
func (c *consumer) StartConsuming(queue string, ack, activePassive bool, concurrentConsumers int, args map[string]interface{}) string {
	if c.consumerRunning {
		err := errors.New("Consumer already running, please configure a new consumer for concurrent processing")
		checkError(err, "Error starting the consumer", c.log)
	}

	b := make([]byte, 4)
	rand.Read(b)
	s := hex.EncodeToString(b)

	delivery, err := c.channel.Consume(queue, s, !ack, activePassive, false, false, args)
	checkError(err, "Error starting the consumer", c.log)

	maybeAckMessage := func(m amqp.Delivery) {
		if ack {
			err := m.Ack(false)
			checkErrorLight(err, "Could not ack the message, it will be eventually requeued", c.log)
		}
	}

	maybeNackMessage := func(m amqp.Delivery) {
		if ack {
			err := m.Nack(false, true)
			checkErrorLight(err, "Could not nack the message, it will be eventually requeued", c.log)
		}
	}

	for i := 0; i < concurrentConsumers; i++ {
		go func(work <-chan amqp.Delivery) {
			for w := range work {
				if !c.handlerExists(w.Type) {
					maybeAckMessage(w)
					continue
				}

				handler := c.handlers[w.Type]
				obj, err := deserialize(w.Body, ContentType(w.ContentType), c.types[w.Type])
				if err != nil {
					c.log.err(fmt.Sprintf("MessageID=%s, could not deserialize the message, requeueing...", w.MessageId))
					maybeNackMessage(w)
				}
				handler(obj) //TODO implement response
				maybeAckMessage(w)
			}
		}(delivery)
	}

	return s
}
