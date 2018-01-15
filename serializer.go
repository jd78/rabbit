package rabbit

import (
	"encoding/json"
	"errors"
)

func serialize(message interface{}, contentType ContentType) ([]byte, error) {
	switch contentType {
	case Json:
		serialized, err := json.Marshal(message)
		return serialized, err
	default:
		return nil, errors.New("unmapped content type")
	}
}

func deserialize(message []byte, contentType ContentType) (interface{}, error) {
	switch contentType {
	case Json:
		var obj interface{}
		err := json.Unmarshal(message, &obj)
		return obj, err
	default:
		return nil, errors.New("unmapped content type")
	}
}
