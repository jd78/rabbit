package rabbit

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

type producer struct {
	numberOfProducers int
	channels          []*amqp.Channel
	roundRobin        int32
	exchangeName      string
	deliveryMode      DeliveryMode
}

var _producer producer

func (r *rabbit) configureProducer(numberOfProducers int, exchangeName string, deliveryMode DeliveryMode) {
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

	_producer = producer{numberOfProducers, channels, 0, exchangeName, deliveryMode}
}

func (p *producer) getChannel() *amqp.Channel {
	i := atomic.AddInt32(&p.roundRobin, 1)
	return p.channels[int(i)%p.numberOfProducers]
}

func (p *producer) send(message interface{}, routingKey, messageID string, header map[string]interface{}, contentType ContentType) error {
	channel := p.getChannel()
	err := channel.Publish(p.exchangeName, routingKey, false, false, amqp.Publishing{
		Headers:      header,
		ContentType:  string(contentType),
		DeliveryMode: uint8(p.deliveryMode),
		MessageId:    messageID,
		Timestamp:    time.Now().UTC(),
		Type:         reflect.TypeOf(message).String(),
	})
	return err
}
