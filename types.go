package rabbit

type logger func(string)

type rabbitLogger struct {
	fatal logger
	err   logger
	info  logger
	warn  logger
	debug logger
}

func CreateLogger(debug, info, warn, err, fatal logger) *rabbitLogger {
	return &rabbitLogger{fatal, err, info, warn, debug}
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
