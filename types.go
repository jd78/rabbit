package rabbit

type logger func(string)

type rabbitLogger struct {
	fatal    logger
	err      logger
	info     logger
	warn     logger
	debug    logger
	logLevel LogLevel
}

func CreateLogger(debug, info, warn, err, fatal logger, logLevel LogLevel) *rabbitLogger {
	return &rabbitLogger{fatal, err, info, warn, debug, logLevel}
}

type ContentType string

const (
	Json     ContentType = "json"
	Protobuf ContentType = "protobuf"
)

type DeliveryMode uint8

const (
	Transient  DeliveryMode = 0
	Persistent DeliveryMode = 1
)

type LogLevel int

const (
	Fatal LogLevel = iota
	Error
	Warn
	Info
	Debug
)

type HandlerResponse int

const (
	Completed HandlerResponse = iota
	Requeue
	Reject
	Err
)
