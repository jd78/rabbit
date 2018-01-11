package rabbit

import (
	"github.com/streadway/amqp"
)

type topology struct {
	channel *amqp.Channel
	log     *rabbitLogger
}

func (r *rabbit) topologyConfiguration() *topology {
	channel, err := r.connection.Channel()
	checkError(err, "error creating topology channel", r.log)
	return &topology{channel, r.log}
}

func (t *topology) DeclareExchange(name, kind string, durable, autoDelete, internal bool, args map[string]interface{}) *topology {
	err := t.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, false, args)
	checkError(err, "error creating exchange", t.log)
	return t
}

func (t *topology) DeclareQueue(name string, durable, autoDelete, exclusive bool, args map[string]interface{}) *topology {
	_, err := t.channel.QueueDeclare(name, durable, autoDelete, exclusive, false, args)
	checkError(err, "error creating queue", t.log)
	return t
}

func (t *topology) BindQueue(name, routingKey, exchangeName string, args map[string]interface{}) *topology {
	err := t.channel.QueueBind(name, routingKey, exchangeName, false, args)
	checkError(err, "error creating the queue bind", t.log)
	return t
}

func (t *topology) Complete() {
	t.channel.Close()
}
