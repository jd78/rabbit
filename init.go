package rabbit

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

func ConfigureConsumer(preferch int) IConsumer {
	return _r.configureConsumer(preferch)
}
