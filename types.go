package rabbit

type logger func(string)

type rabbitLogger struct {
	fatal logger
	err   logger
	info  logger
}

func CreateLogger(info logger, err logger, fatal logger) *rabbitLogger {
	return &rabbitLogger{fatal, err, info}
}
