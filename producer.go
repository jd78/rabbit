package rabbit

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	pattern "github.com/jd78/gopatternmatching"

	"github.com/streadway/amqp"
)

type IProducer interface {
	Send(message interface{}, routingKey, messageID, correlationId, messageType string, header map[string]interface{},
		contentType ContentType) error
}

type sendMessage struct {
	message         interface{}
	routingKey      string
	messageID       string
	messageType     string
	header          map[string]interface{}
	contentType     ContentType
	responseChannel *chan error
	producer        *producer
	producerIndex   int
	correlationId   string
}

type producer struct {
	numberOfProducers int
	channels          []*amqp.Channel
	roundRobin        int32
	exchangeName      string
	deliveryMode      DeliveryMode
	log               *rabbitLogger
	confirmPublish    bool
	confirms          []chan amqp.Confirmation
	producers         []chan sendMessage
}

func (r *rabbit) configureProducer(numberOfProducers int, exchangeName string, deliveryMode DeliveryMode,
	confirmPublish bool) IProducer {
	if numberOfProducers < 1 {
		err := errors.New("numberOfProducers is less than 1")
		checkError(err, "", r.log)
	}

	channels := make([]*amqp.Channel, numberOfProducers, numberOfProducers)
	confirms := make([]chan amqp.Confirmation, numberOfProducers, numberOfProducers)
	producers := make([]chan sendMessage, numberOfProducers, numberOfProducers)
	for i := 0; i < numberOfProducers; i++ {
		channel, err := r.connection.Channel()

		checkError(err, "Error creating the producing channel", r.log)
		channels[i] = channel

		if confirmPublish {
			confirms[i] = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
			err := channel.Confirm(false)
			checkError(err, "failed to create channel confirmation", r.log)
		}

		go func() {
			ch := make(chan *amqp.Error)
			channel.NotifyClose(ch)
			err := <-ch
			checkError(err, "Channel closed!", r.log)
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

		producers[i] = make(chan sendMessage, 1)
		go func(i int) {
			for {
				s := <-producers[i]
				send(&s, r.log.logLevel)
			}
		}(i)
	}

	return &producer{numberOfProducers, channels, 0, exchangeName, deliveryMode, r.log, confirmPublish,
		confirms, producers}
}

func send(s *sendMessage, logLevel LogLevel) {
	serialized, err := serialize(s.message, s.contentType)
	checkError(err, "json serializer error", s.producer.log)

	mt := pattern.ResultMatch(s.messageType).
		WhenValue("", func() interface{} { return reflect.TypeOf(s.message).String() }).
		ResultOrDefault(s.messageType).(string)

	if logLevel >= Debug {
		s.producer.log.debug(fmt.Sprintf("Sending Message %s: %s", mt, serialized))
	}

	pErr := s.producer.channels[s.producerIndex].Publish(s.producer.exchangeName, s.routingKey, false, false, amqp.Publishing{
		Headers:       s.header,
		ContentType:   string(s.contentType),
		DeliveryMode:  uint8(s.producer.deliveryMode),
		MessageId:     s.messageID,
		Timestamp:     time.Now().UTC(),
		Type:          mt,
		Body:          serialized,
		CorrelationId: s.correlationId,
	})

	if pErr != nil {
		*s.responseChannel <- pErr
		return
	}

	if s.producer.confirmPublish {
		if confirmed := <-s.producer.confirms[s.producerIndex]; confirmed.Ack {
			*s.responseChannel <- nil
			return
		}

		*s.responseChannel <- errors.New("unable to publish")
		return
	}

	*s.responseChannel <- nil
}

func (p *producer) getNext() int {
	return int(atomic.AddInt32(&p.roundRobin, 1))
}

//Send a message.
//messageType: if empty the message type will be reflected from the message
func (p *producer) Send(message interface{}, routingKey, messageID, correlationId, messageType string, header map[string]interface{}, contentType ContentType) error {
	i := p.getNext() % p.numberOfProducers
	response := make(chan error, 1)

	//p.log.info(fmt.Sprintf("message id %s, assigned to worker %d", messageID, i))

	s := sendMessage{
		producerIndex:   i,
		contentType:     contentType,
		header:          header,
		message:         message,
		messageID:       messageID,
		messageType:     messageType,
		producer:        p,
		responseChannel: &response,
		routingKey:      routingKey,
		correlationId:   correlationId,
	}

	p.producers[i] <- s
	return <-response
}
