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
		panic("connection lost")
	}()

	go func() {
		ch := make(chan amqp.Blocking)
		conn.NotifyBlocked(ch)
		for {
			status := <-ch
			log.warn(fmt.Sprintf("connection blocked detected - block enabled: %t, reason: %s", status.Active, status.Reason))
		}
	}()

	return rabbit{conn, log}
}

func (r *rabbit) close() {
	r.connection.Close()
}
