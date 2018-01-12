package rabbit

type logger func(string)

type rabbitLogger struct {
	fatal logger
	err   logger
	info  logger
	warn  logger
}

func CreateLogger(info logger, err logger, fatal logger, warn logger) *rabbitLogger {
	return &rabbitLogger{fatal, err, info, warn}
}

type ContentType string

const (
	text     ContentType = "text"
	json     ContentType = "json"
	protobuf ContentType = "protobuf"
)

type DeliveryMode uint8

const (
	transient  DeliveryMode = 0
	persistent DeliveryMode = 1
)
