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
		concurrentConsumers, retryTimesOnError int, args map[string]interface{}) string
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
		checkError(err, "Channel closed", r.log)
	}()

	go func() {
		ch := make(chan bool)
		channel.NotifyFlow(ch)
		for {
			status := <-ch
			if r.log.logLevel >= Warn {
				r.log.warn(fmt.Sprintf("channel flow detected - flow enabled: %t", status))
			}
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
		err := fmt.Errorf("messageType %s already mapped", messageType)
		checkError(err, "", c.log)
	}
	c.handlers[messageType] = handler
	c.types[messageType] = concreteType
}

func (c *consumer) handle(w amqp.Delivery, ack bool, retried, maxRetry int) {
	if w.Redelivered {
		if c.log.logLevel >= Info {
			c.log.info(fmt.Sprintf("MessageID=%s, CorrelationId=%s, has been redelivered",
				w.MessageId, w.CorrelationId))
		}
	}
	handler := c.handlers[w.Type]
	obj, err := deserialize(w.Body, ContentType(w.ContentType), c.types[w.Type])
	if err != nil {
		if c.log.logLevel >= Error {
			c.log.err(fmt.Sprintf("MessageID=%s, CorrelationId=%s, could not deserialize the message, requeueing...",
				w.MessageId, w.CorrelationId))
		}
		(&envelope{&w}).maybeRequeueMessage(ack, c.log)
	}

	response := handler(obj)
	switch response {
	case Completed:
		if c.log.logLevel >= Debug {
			c.log.debug(fmt.Sprintf("MessageId=%s, CorrelationId=%s, completed.",
				w.MessageId, w.CorrelationId))
		}
		(&envelope{&w}).maybeAckMessage(ack, c.log)
	case Requeue:
		if c.log.logLevel >= Debug {
			c.log.debug(fmt.Sprintf("MessageId=%s, CorrelationId=%s, requeueing message...",
				w.MessageId, w.CorrelationId))
		}
		(&envelope{&w}).maybeRequeueMessage(ack, c.log)
	case Reject:
		if c.log.logLevel >= Info {
			c.log.info(fmt.Sprintf("MessageId=%s, CorrelationId=%s, rejecting message...",
				w.MessageId, w.CorrelationId))
		}
		(&envelope{&w}).maybeRejectMessage(ack, c.log)
	case Err:
		if retried > maxRetry-1 {
			if c.log.logLevel >= Warn {
				c.log.warn(fmt.Sprintf("MessageId=%s, CorrelationId=%s, max retry reached, rejecting message...",
					w.MessageId, w.CorrelationId))
			}
			(&envelope{&w}).maybeRejectMessage(ack, c.log)
		} else {
			time.Sleep(time.Duration(retried*200) * time.Microsecond)
			if c.log.logLevel >= Debug {
				c.log.debug(fmt.Sprintf("MessageId=%s, CorrelationId=%s, retry=%d times, retrying due to error...",
					w.MessageId, w.CorrelationId, retried))
			}
			c.handle(w, ack, retried+1, maxRetry)
		}
	}
}

//StartConsuming will start a new consumer
//concurrentConsumers will create concurrent go routines that will read from the delivery rabbit channel
func (c *consumer) StartConsuming(queue string, ack, activePassive bool, activePassiveRetryInterval time.Duration,
	concurrentConsumers, retryTimesOnError int, args map[string]interface{}) string {
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
				if c.log.logLevel >= Info {
					logOnce.MaybeExecute(func() { c.log.info(fmt.Sprintf("Consumer passive on queue %s", queue)) })
				}
				time.Sleep(activePassiveRetryInterval)
			}
		}
	}

	consumerId := getUniqueId()

	delivery, err := c.channel.Consume(queue, consumerId, !ack, activePassive, false, false, args)
	checkError(err, "Error starting the consumer", c.log)

	if activePassive {
		if c.log.logLevel >= Info {
			c.log.info(fmt.Sprintf("Consumer active on queue %s", queue))
		}
	}

	for i := 0; i < concurrentConsumers; i++ {
		go func(work <-chan amqp.Delivery) {
			for w := range work {
				if !c.handlerExists(w.Type) {
					(&envelope{&w}).maybeAckMessage(ack, c.log)
					continue
				}

				c.handle(w, ack, 0, retryTimesOnError)
			}
		}(delivery)
	}

	c.consumerRunning = true
	return consumerId
}
