package rabbit

var _r rabbit

func Initialize(log *rabbitLogger, endpoint string) {
	_r = initialize(endpoint, log)
}

func DeclareExchange(name, kind string, durable, autoDelete, internal bool, args map[string]interface{}) {
	_r.declareExchange(name, kind, durable, autoDelete, internal, args)
}

func DeclareQueue(name string, durable, autoDelete, exclusive bool, args map[string]interface{}) {
	_r.declareQueue(name, durable, autoDelete, exclusive, args)
}

func BindQueue(name, routingKey, exchangeName string, args map[string]interface{}) {
	_r.bindQueue(name, routingKey, exchangeName, args)
}
