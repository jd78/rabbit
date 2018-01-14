package rabbit

import (
	"fmt"
)

type Handler func(message interface{}, params ...interface{})

type IConsumer interface {
	AddHandler(messageType string, handler Handler)
}

type consumer struct {
	handlers map[string]Handler
	log      *rabbitLogger
}

func (r *rabbit) configureConsumer() IConsumer {
	h := make(map[string]Handler)
	return &consumer{h, r.log}
}

func (c *consumer) AddHandler(messageType string, handler Handler) {
	if _, exists := c.handlers[messageType]; exists == true {
		err := fmt.Sprintf("messageType %s already mapped", messageType)
		c.log.err(err)
		panic(err)
	}
	c.handlers[messageType] = handler
}
