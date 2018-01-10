package rabbit

import "github.com/streadway/amqp"

var connection *amqp.Connection

func Connect(endpoint string, logger *rabbitLogger) {
	conn, err := amqp.Dial(endpoint)
	if err != nil {
		logger.fatal(err.Error())
		panic(err.Error())
	}

	connection = conn
}
