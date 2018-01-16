package rabbit

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/streadway/amqp"
)

type Handler func(message interface{}) HandlerResponse

type IConsumer interface {
	AddHandler(messageType string, concreteType reflect.Type, handler Handler)
	StartConsuming(queue string, ack, activePassive bool, activePassiveRetryInterval time.Duration,
		concurrentConsumers int, args map[string]interface{}) string
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

func (c *consumer) handle(w amqp.Delivery, ack bool) {
	handler := c.handlers[w.Type]
	obj, err := deserialize(w.Body, ContentType(w.ContentType), c.types[w.Type])
	if err != nil {
		c.log.err(fmt.Sprintf("MessageID=%s, CorrelationId=%s, could not deserialize the message, requeueing...", w.MessageId, w.CorrelationId))
		(&envelope{&w}).maybeNackMessage(ack, c.log)
	}

	handler(obj) //TODO
	(&envelope{&w}).maybeAckMessage(ack, c.log)
}

//StartConsuming will start a new consumer
//concurrentConsumers will create concurrent go routines that will read from the delivery rabbit channel
func (c *consumer) StartConsuming(queue string, ack, activePassive bool, activePassiveRetryInterval time.Duration,
	concurrentConsumers int, args map[string]interface{}) string {
	if c.consumerRunning {
		err := errors.New("Consumer already running, please configure a new consumer for concurrent processing")
		checkError(err, "Error starting the consumer", c.log)
	}

	if activePassive {
		logOnce := executeOnce{}
		for {
			qInfo := Queues[queue]
			q, err := c.channel.QueueDeclarePassive(qInfo.name, qInfo.durable, qInfo.autoDelete, qInfo.exclusive,
				false, qInfo.args)
			checkError(err, "Error declaring queue passive", c.log)
			if q.Consumers == 0 {
				break
			} else {
				logOnce.MaybeExecute(func() { c.log.info(fmt.Sprintf("Consumer passive on queue %s", queue)) })
				time.Sleep(activePassiveRetryInterval)
			}
		}
	}

	consumerId := getUniqueId()

	delivery, err := c.channel.Consume(queue, consumerId, !ack, activePassive, false, false, args)
	checkError(err, "Error starting the consumer", c.log)

	if activePassive {
		c.log.info(fmt.Sprintf("Consumer active on queue %s", queue))
	}

	for i := 0; i < concurrentConsumers; i++ {
		go func(work <-chan amqp.Delivery) {
			for w := range work {
				if !c.handlerExists(w.Type) {
					(&envelope{&w}).maybeAckMessage(ack, c.log)
					continue
				}

				c.handle(w, ack)
			}
		}(delivery)
	}

	c.consumerRunning = true
	return consumerId
}
