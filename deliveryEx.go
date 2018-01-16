package rabbit

import (
	"fmt"

	"github.com/streadway/amqp"
)

type envelope struct {
	*amqp.Delivery
}

func (m *envelope) maybeAckMessage(ack bool, log *rabbitLogger) {
	if ack {
		err := m.Ack(false)
		checkErrorLight(err, fmt.Sprintf("MessageId=%s, CorrelationId=%s, could not ack the message, it will be eventually requeued", m.MessageId, m.CorrelationId), log)
	}
}

func (m *envelope) maybeRequeueMessage(ack bool, log *rabbitLogger) {
	if ack {
		err := m.Nack(false, true)
		checkErrorLight(err, fmt.Sprintf("MessageId=%s, CorrelationId=%s, could not nack the message, it will be eventually requeued", m.MessageId, m.CorrelationId), log)
	}
}

func (m *envelope) maybeRejectMessage(ack bool, log *rabbitLogger) {
	if ack {
		err := m.Nack(false, false)
		checkErrorLight(err, fmt.Sprintf("MessageId=%s, CorrelationId=%s, could not nack the message, it will be eventually requeued", m.MessageId, m.CorrelationId), log)
	}
}
