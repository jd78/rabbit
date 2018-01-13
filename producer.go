package rabbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	pattern "github.com/jd78/gopatternmatching"

	"github.com/streadway/amqp"
)

type IProducer interface {
	Send(message interface{}, routingKey, messageID, messageType string, header map[string]interface{},
		contentType ContentType) error
}

type producer struct {
	numberOfProducers int
	channels          []*amqp.Channel
	roundRobin        int32
	exchangeName      string
	deliveryMode      DeliveryMode
	log               *rabbitLogger
	logLevel          LogLevel
}

func (r *rabbit) configureProducer(numberOfProducers int, exchangeName string, deliveryMode DeliveryMode) IProducer {
	if numberOfProducers < 1 {
		msg := "numberOfProducers is less than 1"
		r.log.err(msg)
		panic(msg)
	}

	channels := make([]*amqp.Channel, numberOfProducers, numberOfProducers)
	for i := 0; i < numberOfProducers; i++ {
		channel, err := r.connection.Channel()

		checkError(err, "Error creating the producing channel", r.log)
		channels[i] = channel

		go func() {
			ch := make(chan *amqp.Error)
			channel.NotifyClose(ch)
			err := <-ch
			r.log.err(fmt.Sprintf("Connection lost - Error=%s", err.Error()))
			panic("connection lost")
		}()

		go func() {
			ch := make(chan bool)
			channel.NotifyFlow(ch)
			for {
				status := <-ch
				r.log.warn(fmt.Sprintf("channel flow detected - flow enabled: %t", status))
			}
		}()
	}

	return &producer{numberOfProducers, channels, 0, exchangeName, deliveryMode, r.log, r.logLevel}
}

func (p *producer) getChannel() *amqp.Channel {
	i := atomic.AddInt32(&p.roundRobin, 1)
	return p.channels[int(i)%p.numberOfProducers]
}

//Send a message.
//messageType: if empty the message type will be reflected from the message
func (p *producer) Send(message interface{}, routingKey, messageID string, messageType string, header map[string]interface{}, contentType ContentType) error {
	channel := p.getChannel()
	serialized, err := serialize(message, contentType)
	checkError(err, "json serializer error", p.log)

	mt := pattern.ResultMatch(messageType).
		WhenValue("", func() interface{} { return messageType }).
		ResultOrDefault(reflect.TypeOf(message).String()).(string)

	if p.logLevel >= Debug {
		p.log.debug(fmt.Sprintf("Sending Message %s: %s", mt, serialized))
	}

	pErr := channel.Publish(p.exchangeName, routingKey, false, false, amqp.Publishing{
		Headers:      header,
		ContentType:  string(contentType),
		DeliveryMode: uint8(p.deliveryMode),
		MessageId:    messageID,
		Timestamp:    time.Now().UTC(),
		Type:         mt,
		Body:         serialized,
	})
	return pErr
}

func serialize(message interface{}, contentType ContentType) ([]byte, error) {
	switch contentType {
	case Json:
		serialized, err := json.Marshal(message)
		return serialized, err
	default:
		return nil, errors.New("unmapped content type")
	}
}
