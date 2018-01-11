package rabbit

import (
	"fmt"
)

func checkError(err error, additionalData string, lg *rabbitLogger) {
	if err != nil {
		l := fmt.Sprintf("%s: %s", additionalData, err.Error())
		lg.fatal(l)
		panic(l)
	}
}
