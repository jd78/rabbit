package main

import (
	"fmt"
	"log"
	"rabbit"
	"reflect"
	"runtime"
	"time"

	"github.com/golang/protobuf/proto"
)

type TestMessage struct {
	Id   int
	Name string
}

var roundrobin int64 = 0

func main() {

	runtime.GOMAXPROCS(4)

	logger := rabbit.CreateLogger(
		func(l string) { log.Println(l) },
		func(l string) { log.Println(l) },
		func(l string) { log.Println(l) },
		func(l string) { log.Fatal(l) },
		func(l string) { log.Println(l) },
		rabbit.Debug)

	rabbit.Initialize(logger, "amqp://guest:guest@localhost:5672/")
	rabbit.TopologyConfiguration().
		DeclareExchange("test.output", "topic", true, false, false, nil).
		DeclareExchange("consumer.output", "fanout", true, false, false, nil).
		DeclareQueue("test.inbound", true, false, false, nil).
		BindQueue("test.inbound", "#", "test.output", nil).
		DeclareQueue("consumer.inbound", true, false, false, map[string]interface{}{
			"x-message-ttl": int32(600000),
		}).
		BindQueue("consumer.inbound", "", "consumer.output", nil).
		Complete()

	producer := rabbit.ConfigureProducer(3, "test.output", rabbit.Transient, true)
	consumerProducer := rabbit.ConfigureProducer(3, "consumer.output", rabbit.Transient, true)

	consumer := rabbit.ConfigureConsumer(100, 5*time.Second)
	h := TestMessageHandler("teststring", consumerProducer)
	consumer.AddHandler("main.TestMessage", reflect.TypeOf(TestMessage{}), h)

	protoH := ProtoMessageHandler()
	consumer.AddHandler("main.Test", reflect.TypeOf(Test{}), protoH)

	//consumer.AddRetryHandler("main.TestMessage", reflect.TypeOf(TestMessage{}), h, 10)
	partitionerResolver := make(map[reflect.Type]func(message interface{}) int64)
	partitionerResolver[reflect.TypeOf(TestMessage{})] = func(message interface{}) int64 { return int64(message.(TestMessage).Id) }
	//consumer.StartConsuming("test.inbound", true, true, 1000*time.Millisecond, 1, nil)
	consumer.StartConsumingPartitions("test.inbound", true, true,
		3000*time.Millisecond, 5*time.Second, 30, partitionerResolver, nil)

	for i := 0; i < 1; i++ {
		go func(i int) {
			message := TestMessage{i, "test"}
			err := producer.Send(message, "testmessage.1", fmt.Sprintf("%d", i), "correlation", "", nil, rabbit.Json)
			if err != nil {
				log.Fatal(err)
			}

			protoMessage := &Test{
				Id:    proto.Int32(int32(i)),
				Label: proto.String("test protobuf"),
			}
			err2 := producer.Send(protoMessage, "protoMessage.1", fmt.Sprintf("%d", i), "correlation", "", nil, rabbit.Protobuf)
			if err2 != nil {
				log.Fatal(err2)
			}
		}(i)
	}

	fmt.Scanln()
}
