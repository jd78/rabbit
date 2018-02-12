package main

import (
	"fmt"
	"log"
	"rabbit"
	"time"
)

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

func ProtoMessageHandler() func(message interface{}, header map[string]interface{}) rabbit.HandlerResponse {
	return func(message interface{}, header map[string]interface{}) rabbit.HandlerResponse {
		testMessage := message.(Test)
		log.Printf("Id: %d", testMessage.GetId())
		log.Printf("Label: %s", testMessage.GetLabel())
		time.Sleep(500 * time.Millisecond)
		return rabbit.Completed
	}
}
