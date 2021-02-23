package main

import (
	"context"
	"fmt"
	"github.com/taylor-chen/aws-sqs-example/example/consumer"
)

func main() {

	fmt.Println("Serviced Starting")
	handlerFn := consumer.MessageHandler(func(ctx context.Context, msg consumer.Message) error {
		for _, msg := range msg.Messages {
			fmt.Println("Received:" + *msg.MessageId)
		}
		return nil
	})

	c := &consumer.SQSConsumer{
		ID:             "TEST-SERVICE",
		QUEUE:          "taylorc-test",
		ProcessMessage: handlerFn,
	}
	c.Run()
}