package rabbit

var _r rabbit

func Initialize(log *rabbitLogger, endpoint string) {
	_r = initialize(endpoint, log)
}

func TopologyConfiguration() *topology {
	return _r.topologyConfiguration()
}
