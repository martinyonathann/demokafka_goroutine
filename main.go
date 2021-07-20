package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic          = "message-log"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

func produce(ctx context.Context) {
	// fmt.Println("masuk produce")
	//initialize a counter
	i := 0

	// initialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
	})

	for {
		//each kafka message has a key and value. The key is used
		//to decide which partition (and consequently, which broker)
		//the message gets published on
		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			//create an arbitrary message payload for the value
			Value: []byte("this is message" + strconv.Itoa(i)),
		})
		if err != nil {
			panic("could not write message" + err.Error())
		}

		//log a confirmation once the message is written
		fmt.Println("writes:", i)
		i++

		//sleep for a second
		time.Sleep(time.Second)
	}
}

func consume(ctx context.Context) {
	// fmt.Println("masuk consumer")
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

func main() {
	ctx := context.Background()
	go produce(ctx)
	go consume(ctx)
	time.Sleep(1 * time.Second)
}
