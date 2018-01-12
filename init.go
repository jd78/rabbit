package rabbit

var _r rabbit

func Initialize(log *rabbitLogger, endpoint string) {
	_r = initialize(endpoint, log)
}

func TopologyConfiguration() *topology {
	return _r.topologyConfiguration()
}

func ConfigureProducer(numberOfProducers int, exchangeName string, deliveryMode DeliveryMode) IProducer {
	return _r.configureProducer(numberOfProducers, exchangeName, deliveryMode)
}
