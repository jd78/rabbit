package rabbit

import (
	"github.com/streadway/amqp"
)

type topology struct {
	channel *amqp.Channel
}

func (r *rabbit) topologyConfiguration() topology {
	channel, err := r.connection.Channel()
	checkError(err, "error creating topology channel", r.log)
	return topology{channel}
}

//func (t *topology)
