package demokafkagoroutine

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func consume(ctx context.Context) {
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
		GroupID: "my-group",
	})

	for {
		// the `ReadMessage` method blocks util we receive the next event
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message" + err.Error())
		}
		// after receiving the message, log its value
		fmt.Println("received: ", string(msg.Value))
	}
}
