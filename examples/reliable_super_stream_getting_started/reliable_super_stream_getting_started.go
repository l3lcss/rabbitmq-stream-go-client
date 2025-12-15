package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func main() {
	fmt.Printf("Getting started with Super Stream client for RabbitMQ \n")

	// Create the environment. You can set the log level to DEBUG for more information
	stream.SetLevelInfo(logs.DEBUG)

	urlS := os.Getenv("RABBITMQ_URI")
	url, err := url.Parse(urlS)
	if err != nil {
		log.Fatalf("Error %s", err.Error())
	}
	port, err := strconv.Atoi(url.Port())
	if err != nil {
		log.Fatalf("parsing uri port failed %s", err.Error())
	}

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetUri(url.String()).
			SetTLSConfig(&tls.Config{InsecureSkipVerify: true}).
			SetAddressResolver(stream.AddressResolver{
				Host: url.Hostname(),
				Port: port,
			}).
			SetRequestedHeartbeat(120 * time.Second).
			SetMaxProducersPerClient(10).
			SetMaxConsumersPerClient(10).
			SetRPCTimeout(30 * time.Second))
	if err != nil {
		fmt.Printf("Error creating environment: %v\n", err)
		return
	}

	// Create a super stream
	streamName := "my-super-stream"
	// It is highly recommended to define the stream retention policy
	err = env.DeclareSuperStream(streamName, stream.NewPartitionsOptions(30).
		SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))

	// ignore the error if the stream already exists
	if err != nil && !errors.Is(err, stream.StreamAlreadyExists) {
		fmt.Printf("Error declaring stream: %v\n", err)
		return
	}

	// declare the reliable consumer using the package ha
	consumer, err := ha.NewReliableSuperStreamConsumer(env, streamName,
		// handler where the messages will be processed
		func(_ stream.ConsumerContext, message *amqp.Message) {
			fmt.Printf("Message received: %s\n", message.GetData())
		},
		// start from the beginning of the stream
		stream.NewSuperStreamConsumerOptions().SetOffset(stream.OffsetSpecification{}.First()).SetConsumerName("ha-super-stream-consumer-playground"),
	)

	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}

	// Create the reliable producer using the package ha
	producer, err := ha.NewReliableSuperStreamProducer(env, streamName,
		// we leave the default options
		stream.NewSuperStreamProducerOptions(stream.NewHashRoutingStrategy(func(message message.StreamMessage) string {
			return message.GetMessageProperties().MessageID.(string)
		})).SetClientProvidedName("ha-super-stream-producer-playground"),
		// handler for the confirmation of the messages
		func(messageConfirm []*stream.PartitionPublishConfirm) {
			for _, msgs := range messageConfirm {
				for _, msg := range msgs.ConfirmationStatus {
					if msg.IsConfirmed() {
						fmt.Printf("message %s confirmed \n", msg.GetMessage().GetData())
					} else {
						fmt.Printf("message %s failed \n", msg.GetMessage().GetData())
					}
				}
			}
		})

	if err != nil {
		fmt.Printf("Error creating producer: %v\n", err)
		return
	}

	// Send a message
	for i := range 1000 {
		msg := amqp.NewMessage([]byte(fmt.Sprintf("Hello stream:%d", i)))
		msg.Properties = &amqp.MessageProperties{
			MessageID: fmt.Sprintf("msg-%d", i),
		}
		err = sendWithTimeout(producer, msg, 10*time.Second)
		if err != nil {
			logs.LogDebug("Error sending message: %v\n", err)
			continue
		}
		time.Sleep(1 * time.Second)
	}

	// press any key to exit
	fmt.Printf("Press any close the producer, consumer and environment\n")
	_, _ = fmt.Scanln()

	//// Close the producer
	err = producer.Close()
	if err != nil {
		fmt.Printf("Error closing producer: %v\n", err)
	}

	// Close the consumer
	err = consumer.Close()
	if err != nil {
		fmt.Printf("Error closing consumer: %v\n", err)
	}

	err = env.DeleteSuperStream(streamName)
	if err != nil {
		fmt.Printf("Error deleting stream: %v\n", err)
	}

	// Close the environment
	err = env.Close()
	if err != nil {
		fmt.Printf("Error closing environment: %s\n", err)
	}
}

func sendWithTimeout(producer *ha.ReliableSuperStreamProducer, msg *amqp.AMQP10, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- producer.Send(msg)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return fmt.Errorf("send timeout after %v: %w", timeout, ctx.Err())
	}
}
