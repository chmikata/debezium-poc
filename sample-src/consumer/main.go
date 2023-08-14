package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {

	var kafkaBroker = os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}
	var kafkaTopic = os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "my-topic"
	}
	var consumerGroup = os.Getenv("KAFKA_CONSUMER_GROUP")
	if consumerGroup == "" {
		consumerGroup = "my-topic-consumers"
	}
	var sleep = os.Getenv("SLEEP")
	if sleep == "" {
		sleep = "0"
	}
	sleepSeconds, err := strconv.Atoi(sleep)
	if err != nil {
		sleepSeconds = 0
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  consumerGroup,
		Topic:    kafkaTopic,
		MinBytes: 10e1,
		MaxBytes: 10e6,
	})
	defer r.Close()

	fmt.Printf("Waiting for messages on broker: %v group: %v topic: %v\n", r.Config().Brokers, r.Config().GroupID, r.Config().Topic)
	go func() {
		for {
			fmt.Printf("Waiting for a new message. Current Offset: %d, Lag: %d\n ", r.Offset(), r.Lag())
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				fmt.Printf("Error occured while waiting for message %s", err)
				break
			}
			fmt.Printf("message at topic/partition/offset %v/%v/%v: %v = %v\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			time.Sleep(time.Second * time.Duration(sleepSeconds))
			fmt.Printf("Slept for %d seconds\n", sleepSeconds)
		}
	}()

	quitC := make(chan os.Signal)
	signal.Notify(quitC, syscall.SIGTERM, syscall.SIGINT)

	sig := <-quitC
	fmt.Printf("Got signal: %s\n", sig)

	_, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	} else {
		fmt.Printf("Closed the reader\n")
	}
}
