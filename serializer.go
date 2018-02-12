package rabbit

import (
	"encoding/json"
	"reflect"

	"github.com/golang/protobuf/proto"
)

func serialize(message interface{}, contentType ContentType) ([]byte, error) {
	serializeJson := func() ([]byte, error) {
		serialized, err := json.Marshal(message)
		return serialized, err
	}

	switch contentType {
	case Json:
		return serializeJson()
	case Protobuf:
		return proto.Marshal(message.(proto.Message))
	default: //use json
		return serializeJson()
	}
}

func deserialize(message []byte, contentType ContentType, concreteType reflect.Type) (interface{}, error) {
	deserializeJson := func() (interface{}, error) {
		pointer := reflect.New(concreteType).Interface()
		err := json.Unmarshal(message, &pointer)
		if err != nil {
			return nil, err
		}
		noPointer := reflect.Indirect(reflect.ValueOf(pointer)).Interface()
		return noPointer, nil
	}

	deserializeProtobuf := func() (interface{}, error) {
		pointer := reflect.New(concreteType).Interface().(proto.Message)
		err := proto.Unmarshal(message, pointer)
		if err != nil {
			return nil, err
		}
		noPointer := reflect.Indirect(reflect.ValueOf(pointer)).Interface()
		return noPointer, nil
	}

	switch contentType {
	case Json:
		return deserializeJson()
	case Protobuf:
		return deserializeProtobuf()
	default:
		return deserializeJson()
	}
}
