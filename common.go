package rabbit

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type Queue struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	args       map[string]interface{}
}

var Queues map[string]Queue

func checkError(err error, additionalData string, lg *rabbitLogger) {
	if err != nil {
		l := fmt.Sprintf("%s: %s", additionalData, err.Error())
		lg.fatal(l)
		panic(l)
	}
}

func checkErrorLight(err error, additionalData string, lg *rabbitLogger) {
	if err != nil {
		if lg.logLevel >= Warn {
			lg.warn(fmt.Sprintf("%s: %s", additionalData, err.Error()))
		}
	}
}

func getUniqueId() string {
	b := make([]byte, 4)
	rand.Read(b)
	return hex.EncodeToString(b)
}
