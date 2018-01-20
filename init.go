package rabbit

import (
	"time"
)

var _r rabbit

func Initialize(log *rabbitLogger, endpoint string) {
	_r = initialize(endpoint, log)
}

func TopologyConfiguration() *topology {
	return _r.topologyConfiguration()
}

func ConfigureProducer(numberOfProducers int, exchangeName string, deliveryMode DeliveryMode, confirmPublish bool) IProducer {
	return _r.configureProducer(numberOfProducers, exchangeName, deliveryMode, confirmPublish)
}

//requeueWaitingTimeOnError: time interval requeueing a message in case of handler error (message ordering will be lost). Takes effect if enableRetries is false
func ConfigureConsumer(preferch int, requeueWaitingTimeOnError time.Duration) IConsumer {
	return _r.configureConsumer(preferch, requeueWaitingTimeOnError)
}
