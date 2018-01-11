package rabbit

import "github.com/streadway/amqp"
import "fmt"

type rabbit struct {
	connection *amqp.Connection
	log        *rabbitLogger
}

func initialize(endpoint string, log *rabbitLogger) rabbit {
	conn, err := amqp.Dial(endpoint)
	checkError(err, "error during connection", log)
	go func() {
		ch := make(chan *amqp.Error)
		conn.NotifyClose(ch)
		err := <-ch
		log.err(fmt.Sprintf("Connection lost - Error=%s", err.Error()))
	}()

	return rabbit{conn, log}
}

func (r *rabbit) close() {
	r.connection.Close()
}

func (r *rabbit) declareExchange(name, kind string, durable, autoDelete, internal bool, args map[string]interface{}) {
	channel, err := r.connection.Channel()
	checkError(err, "error creating topology channel", r.log)
	defer channel.Close()

	exErr := channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, false, args)
	checkError(exErr, "error creating exchange", r.log)
}

func (r *rabbit) declareQueue(name string, durable, autoDelete, exclusive bool, args map[string]interface{}) {
	channel, err := r.connection.Channel()
	checkError(err, "error creating topology channel", r.log)
	defer channel.Close()

	_, qErr := channel.QueueDeclare(name, durable, autoDelete, exclusive, false, args)
	checkError(qErr, "error creating queue", r.log)
}

func (r *rabbit) bindQueue(name, routingKey, exchangeName string, args map[string]interface{}) {
	channel, err := r.connection.Channel()
	checkError(err, "error creating topology channel", r.log)
	defer channel.Close()

	bErr := channel.QueueBind(name, routingKey, exchangeName, false, args)
	checkError(bErr, "error creating the queue bind", r.log)
}
