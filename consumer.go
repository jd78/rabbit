package rabbit

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/jd78/partitioner"
	"github.com/streadway/amqp"
)

type Handler func(message interface{}) HandlerResponse

type IConsumer interface {
	AddHandler(messageType string, concreteType reflect.Type, handler Handler)
	AddRetryHandler(messageType string, concreteType reflect.Type, handler Handler, maxRetries int)
	StartConsuming(queue string, ack, activePassive bool, activePassiveRetryInterval, requeueTimeIntervalOnError time.Duration,
		concurrentConsumers int, args map[string]interface{}) string
	StartConsumingPartitions(queue string, ack, activePassive bool, activePassiveRetryInterval, requeueTimeIntervalOnError,
		maxWaitingTimeRetryIntervalOnPartitionError time.Duration, partitions int,
		partitionResolver map[reflect.Type]func(message interface{}) int64, args map[string]interface{}) string
}

type consumer struct {
	handlers        map[string]Handler
	types           map[string]reflect.Type
	maxRetries      map[string]int
	log             *rabbitLogger
	channel         *amqp.Channel
	consumerRunning map[string]bool
}

var roundrobin int64

type partition struct {
	message           interface{}
	partitionResolver map[reflect.Type]func(message interface{}) int64
}

func (p partition) GetPartition() int64 {
	t := reflect.TypeOf(p.message)
	if _, exists := p.partitionResolver[t]; exists {
		return p.partitionResolver[t](p.message)
	}

	return atomic.AddInt64(&roundrobin, 1)
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
	retries := make(map[string]int)
	return &consumer{h, t, retries, r.log, channel, make(map[string]bool)}
}

func (c *consumer) handlerExists(messageType string) bool {
	_, exists := c.handlers[messageType]
	return exists
}

func (c *consumer) getMaxRetries(messageType string) (bool, int) {
	maxRetries, exists := c.maxRetries[messageType]
	return exists, maxRetries
}

func (c *consumer) AddHandler(messageType string, concreteType reflect.Type, handler Handler) {
	if c.handlerExists(messageType) {
		err := fmt.Errorf("messageType %s already mapped", messageType)
		checkError(err, "", c.log)
	}
	c.handlers[messageType] = handler
	c.types[messageType] = concreteType
}

//maxRetries: number of retries before discarding a message. Takes effect if enableRetries is true
func (c *consumer) AddRetryHandler(messageType string, concreteType reflect.Type, handler Handler, maxRetries int) {
	if c.handlerExists(messageType) {
		err := fmt.Errorf("messageType %s already mapped", messageType)
		checkError(err, "", c.log)
	}
	c.handlers[messageType] = handler
	c.types[messageType] = concreteType
	c.maxRetries[messageType] = maxRetries
}

func (c *consumer) handle(w amqp.Delivery, message interface{}, ack bool, retried int,
	requeueTimeMillisecondsOnError time.Duration) {
	if w.Redelivered {
		if c.log.logLevel >= Info {
			c.log.info(fmt.Sprintf("MessageID=%s, CorrelationId=%s, has been redelivered",
				w.MessageId, w.CorrelationId))
		}
	}
	handler := c.handlers[w.Type]

	response := handler(message)
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
		retryEnabled, maxRetries := c.getMaxRetries(w.Type)
		if retryEnabled {
			if retried > maxRetries-1 {
				if c.log.logLevel >= Warn {
					c.log.warn(fmt.Sprintf("MessageId=%s, CorrelationId=%s, max retry reached, rejecting message...",
						w.MessageId, w.CorrelationId))
				}
				(&envelope{&w}).maybeRejectMessage(ack, c.log)
			} else {
				time.Sleep(time.Duration(retried*100) * time.Millisecond)
				if c.log.logLevel >= Debug {
					c.log.debug(fmt.Sprintf("MessageId=%s, CorrelationId=%s, retry=%d times, retrying due to error...",
						w.MessageId, w.CorrelationId, retried))
				}
				c.handle(w, message, ack, retried+1, requeueTimeMillisecondsOnError)
			}
		} else {
			go func() {
				time.Sleep(requeueTimeMillisecondsOnError)
				(&envelope{&w}).maybeRequeueMessage(ack, c.log)
			}()
		}
	}
}

func (c *consumer) deserializeMessage(w amqp.Delivery) (interface{}, error) {
	obj, err := deserialize(w.Body, ContentType(w.ContentType), c.types[w.Type])
	if err != nil {
		if c.log.logLevel >= Error {
			c.log.err(fmt.Sprintf("MessageID=%s, CorrelationId=%s, could not deserialize the message, requeueing...",
				w.MessageId, w.CorrelationId))
		}
	}
	return obj, err
}

//StartConsuming will start a new consumer
//concurrentConsumers will create concurrent go routines that will read from the delivery rabbit channel
//queue: queue name
//ack: true enables ack
//activePassive: enables acrive and sleepy passive consumers
//activePassiveRetryInterval: time interval checking if the queue has a consumer
//requeueTimeIntervalOnError: time interval requeueing a message in case of handler error (message ordering will be lost). Takes effect if enableRetries is false
//concurrentConsumers: number of consumers
//args: consumer args
func (c *consumer) StartConsuming(queue string, ack, activePassive bool, activePassiveRetryInterval, requeueTimeIntervalOnError time.Duration,
	concurrentConsumers int, args map[string]interface{}) string {
	if _, exists := c.consumerRunning[queue]; exists {
		err := errors.New("Consumer already running, please configure a new consumer for concurrent processing or set the concurrentConsumers")
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

				message, err := c.deserializeMessage(w)
				if err != nil {
					(&envelope{&w}).maybeRequeueMessage(ack, c.log)
					continue
				}

				c.handle(w, message, ack, 0, requeueTimeIntervalOnError)
			}
		}(delivery)
	}

	c.consumerRunning[queue] = true
	return consumerId
}

//StartConsuming will start a new consumer
//concurrentConsumers will create concurrent go routines that will read from the delivery rabbit channel
//queue: queue name
//ack: true enables ack
//activePassive: enables acrive and sleepy passive consumers
//activePassiveRetryInterval: time interval checking if the queue has a consumer
//requeueTimeIntervalOnError: time interval requeueing a message in case of handler error (message ordering will be lost). Takes effect if enableRetries is false
//maxWaitingTimeRetryIntervalOnPartitionError: Sleep time between retries in case of handler error
//concurrentConsumers: number of consumers
//partitions: number of consurrent/consistent partitions
//partitionResolver: map[reflect.Type]func(message interface{}) int64, for each message type specify a function that will return the key used to partition
//args: consumer args
func (c *consumer) StartConsumingPartitions(queue string, ack, activePassive bool, activePassiveRetryInterval,
	requeueTimeIntervalOnError, maxWaitingTimeRetryIntervalOnPartitionError time.Duration,
	partitions int, partitionResolver map[reflect.Type]func(message interface{}) int64, args map[string]interface{}) string {
	if _, exists := c.consumerRunning[queue]; exists {
		err := errors.New("Consumer already running, please configure a new consumer for concurrent processing or set partitions")
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

	part := partitioner.CreatePartitioner(partitions, maxWaitingTimeRetryIntervalOnPartitionError)

	go func(work <-chan amqp.Delivery) {
		for w := range work {
			if !c.handlerExists(w.Type) {
				(&envelope{&w}).maybeAckMessage(ack, c.log)
				continue
			}

			message, err := c.deserializeMessage(w)
			if err != nil {
				(&envelope{&w}).maybeRequeueMessage(ack, c.log)
				continue
			}

			cw := w
			part.HandleInSequence(func(done chan bool) {
				c.handle(cw, message, ack, 0, requeueTimeIntervalOnError)
				done <- true
			}, partition{message, partitionResolver})
		}
	}(delivery)

	c.consumerRunning[queue] = true
	return consumerId
}
