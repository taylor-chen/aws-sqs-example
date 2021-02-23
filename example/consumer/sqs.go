package consumer


import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)


type SQSConsumer struct {
	ID             string
	QUEUE          string
	ProcessMessage MessageHandler // The handling function that will be called with each Message
}

type Message struct {
	*sqs.ReceiveMessageOutput
}

// MessageHandler is the function that will be called on all received messages for the configured exchange.
type MessageHandler func(context.Context, Message) error

func run(ctx context.Context, errChan chan error, c *SQSConsumer) {
	waitTime := 10
	defer func() {
		if r := recover(); r != nil {
			// recover the error to pass down the channel
			switch r.(type) {
			case string:
				errChan <- errors.New(r.(string))
			case error:
				errChan <- r.(error)
			default:
				errChan <- errors.New("Unknown panic")
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// terminate if context is closed
			return
		default:
			// main loop

			// create new sqs client
			cfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				panic("configuration error, " + err.Error())
			}

			client := sqs.NewFromConfig(cfg)

			qInput := &sqs.GetQueueUrlInput{
				QueueName: &c.QUEUE,
			}

			result, err := client.GetQueueUrl(ctx, qInput)
			if err != nil {
				fmt.Println("Got an error getting the queue URL:")
				fmt.Println(err)
				return
			}

			queueURL := result.QueueUrl

			// Call ReceiveMessage
			mInput := &sqs.ReceiveMessageInput{
				QueueUrl: queueURL,
				AttributeNames: []types.QueueAttributeName{
					"SentTimestamp",
				},
				MaxNumberOfMessages: 1,
				MessageAttributeNames: []string{
					"All",
				},
				WaitTimeSeconds: int32(waitTime),
			}

			fmt.Println("Call: ReceiveMessage")
			resp, err := client.ReceiveMessage(ctx, mInput)
			if err != nil {
				fmt.Println("Got an error receiving messages:")
				fmt.Println(err)
				return
			}

			// Call the MessageHandler function on messages received
			err = c.ProcessMessage(ctx, Message{ReceiveMessageOutput: resp})
			if err != nil {
				fmt.Println("Got an error processing messages:")
				fmt.Println(err)
				return
			}

			// Delete messages if successful
			for _, msg := range resp.Messages {
				dMInput := &sqs.DeleteMessageInput{
					QueueUrl:      queueURL,
					ReceiptHandle: msg.ReceiptHandle,
				}

				fmt.Println("Call: DeleteMessage")
				_, err = client.DeleteMessage(ctx, dMInput)
				if err != nil {
					fmt.Println("Got an error deleting the message:")
					fmt.Println(err)
					return
				}
			}
		}
	}
}

// Run is the main entrypoint for running a Consumer. It will result in a running consumer until the program receives
// a termination signal or an unknown Fatal error occurs.
func (c *SQSConsumer) Run() {
	// start shutdown listener
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGINT)

	// start `run` routine with a listener for internal failure
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go run(ctx, errChan, c)

	select {
	case <-shutdownChan:
		cancel()
	case err := <-errChan:
		cancel()
		log.Fatal(err)
	}
}
