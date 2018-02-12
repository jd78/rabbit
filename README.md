# Rebbit

Rabbit is built on top of streadway/amqp RabbitMQ client (https://github.com/streadway/amqp) and brings the following functionalities:

- Can produce and consume messages in jSON and Protobuf.
- Can produce and consume concurrently, specifying the number of consumers/producers. The producer is concurrent round robin, the consumer is a concurrent multichannels.
- Partition consumer, can consume messages in sequences while concurrent. Assigning a partition id to a message, this will be consumed by the same channel, in order.
- Dynamic handlers.
- Message retries.
- Can discard or requeue a message just returning the wanted behaviour from the handler.
- Active and sleepy passive consumer.
- Topology configurator.
- Injection logger.

#### Logger injection and rabbit initializer

```go

import "github.com/jd78/rabbit"

logger := rabbit.CreateLogger(
		func(l string) { log.Println(l) }, //debug
		func(l string) { log.Println(l) }, //info
        func(l string) { log.Println(l) }, //warn
		func(l string) { log.Fatal(l) },  //error
		func(l string) { log.Println(l) }, //fata;
        rabbit.Debug //logLevel
        )

rabbit.Initialize(logger, "amqp://username:password@localhost:5672/")
```

#### Topology configuration

It's possible to configure the topology, creating exchnges, queues and bindings.

```go

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
```

#### Producer

It's necessary to configure a producer for each output exchange you want to send messages to. 
Each producer has at least one rabbit channel. You can specify how many concurrent and round robin producers you want, keeping in mind that each producer specified in the parameter "numberOfProducers" will create it's own rabbit channel. This has been done to ensure that the publish confirmation works correctly. Two messages cannot be sent at the same time for the same channel. Sends are queued in a round robin way.

```go

//params: number of producers, output exchange, publish type (Transient or Persistent), confirm publish
testOutputProducer := rabbit.ConfigureProducer(1, "test.output", rabbit.Transient, true)
consumerProducer := rabbit.ConfigureProducer(3, "consumer.output", rabbit.Transient, true)

```

### Message types and message handlers

Supposing we have the following message type we want to publish and consume

```go

type TestMessage struct {
	Id   int
	Name string
}

```

Firstly we want to create a handler for this message

```go

func TestMessageHandler(test string, producer rabbit.IProducer) func(message interface{}, header map[string]interface{}) rabbit.HandlerResponse {
	return func(message interface{}, header map[string]interface{}) rabbit.HandlerResponse {
		testMessage := message.(TestMessage)
		log.Println("executing testMessage, test: " + test)
		log.Println(testMessage)
		log.Println(fmt.Sprintf("header: %v", header))
		time.Sleep(500 * time.Millisecond)
		err := producer.Send(message, "", "test", "correlation", "", nil, rabbit.Json)
		if err != nil {
			log.Println("Error sending the message")
			return rabbit.Err
		}
		return rabbit.Completed
	}
}

```

A message handler can have parameters or not. You might want to inject your repository, a rabbit producer, etc.
A handler always returns the function func(message interface{}, header map[string]interface{}) rabbit.HandlerResponse, where message and handler are respectively the message and the handler that will be injected by the rabbit library; and rabbit.HandlerResponse is the return status of the handler that will be read by the rabbit library to perfom specific operations like ack, reqeueing, etc.

rabbit.HandlerResponse can be:
 - rabbit.Completed, if all good, the message will be eventually acked
 - rabbit.Reject, if you want to reject the message
 - rabbit.Requeue, if you with to requeue the message and re-consume it again
 - rabbit.Err, an error occurred. The message will be requeued or retried N time based on the client configuration that we are going to discuss later.

 In the previous example, we are just handling the TestMessage writing some log and publishing the same message again.

 ### Consumer

 ```go
//params: prefetch, requeue waiting time on error
consumer := rabbit.ConfigureConsumer(100, 5*time.Second)

 ```

 We can specify a prefetch and the requeue waiting time on error. The second parameter is the waiting time before the message gets requeued when you return rabbit.Err from a handler.

 Below is how you have to register your handlers

 ```go

h := TestMessageHandler("teststring", consumerProducer) //I'm passing a string and the producer dependency
consumer.AddHandler("main.TestMessage", reflect.TypeOf(TestMessage{}), h)

 ```

 main.TestMessage is the type that will be send as envelope type, and the reflected type as second parameter is the is the contract representing the message. Finally, as third parameter, we pass the handler that will handle the message.

 At this point, to start the consumer, without using the partitioner, it's enough to do

 ```go

//parameters: queue name, ack, use active passive, passive check for consumer interval, concurrent consumers, amqp consumer args
 consumer.StartConsuming("test.inbound", true, true, 1000*time.Millisecond, 1, nil)

 ```

If you want to use the partitioner instead you need to configure the partition resolver.

```go

partitionerResolver := make(map[reflect.Type]func(message interface{}) int64)
partitionerResolver[reflect.TypeOf(TestMessage{})] = func(message interface{}) int64 { return int64(message.(TestMessage).Id) }

//parameters: queue name, ack, use active passive, passive check for consumer interval, max time waiting on partition handler error, 
//number or partitions, resolver, amqp consumer args.
consumer.StartConsumingPartitions("test.inbound", true, true,
		3000*time.Millisecond, 5*time.Second, 30, partitionerResolver, nil)

```

Be aware that if a partition is in error, no messages will be consumed for that partition but will be queued until the message in error gets correctly consumed.

### Demo

A simple demo is located in the example folder
