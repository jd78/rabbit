package rabbit

var _r rabbit

func Initialize(log *rabbitLogger, endpoint string, logLevel LogLevel) {
	_r = initialize(endpoint, log, logLevel)
}

func TopologyConfiguration() *topology {
	return _r.topologyConfiguration()
}

func ConfigureProducer(numberOfProducers int, exchangeName string, deliveryMode DeliveryMode, confirmPublish bool) IProducer {
	return _r.configureProducer(numberOfProducers, exchangeName, deliveryMode, confirmPublish)
}
