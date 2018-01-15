package rabbit

import (
	"encoding/json"
	"errors"
	"reflect"
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

func deserialize(message []byte, contentType ContentType, concreteType reflect.Type) (interface{}, error) {
	switch contentType {
	case Json:
		pointer := reflect.New(concreteType).Interface()
		err := json.Unmarshal(message, &pointer)
		if err != nil {
			return nil, err
		}
		noPointer := reflect.Indirect(reflect.ValueOf(pointer)).Interface()
		return noPointer, nil
	default:
		return nil, errors.New("unmapped content type")
	}
}
