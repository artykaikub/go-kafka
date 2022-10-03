package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/artykaikub/go-kafka"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()

	SyncProducerSimple()
	// AsyncProducerSimple()
	// ProducerWithCustomPartitioner()
	// ProducerWithSaramaPartitioner()
	// ProducerWithInterceptor()
	// ProducerWithTLSCert()
	// ProducerTLSWithPEMFile()
}

func SyncProducerSimple() {
	producer := kafka.NewSyncProducer(
		kafka.WithProducerOptionsSuccesses,
	)

	if _, _, err := producer.SendMessage(context.Background(), "test_topic", kafka.ProducerMessage{
		Value: []byte("test"),
	}); err != nil {
		panic(err)
	}
}

func AsyncProducerSimple() {
	producer := kafka.NewAsyncProducer(
		kafka.WithProducerOptionsSuccesses,
	)

	wg := &sync.WaitGroup{}
	defer wg.Wait()
	wg.Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bg, errs := producer.Background(ctx)
	go bg()

	go func() {
		defer wg.Done()

		// consume this until library closes it for us, indicating client has shut down
		for err := range errs {
			log.Printf("kafka message error: %s", err)
		}
	}()

	for i := 0; i < 3; i++ {
		if err := producer.Emit(ctx, "test_topic", kafka.ProducerMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("test no %d", i)),
		}); err != nil {
			log.Printf("emit error: %s", err)
			break
		}

		time.Sleep(5 * time.Second)
	}

	log.Printf("run completed, exiting")
}

func ProducerWithInterceptor() {
	producer := kafka.NewSyncProducer(
		kafka.WithProducerOptionsInterceptor([]kafka.ProducerInterceptor{exampleInterceptor{}}),
	)

	if _, _, err := producer.SendMessage(context.Background(), "test_topic", kafka.ProducerMessage{
		Value: []byte("test"),
	}); err != nil {
		panic(err)
	}
}

func ProducerWithCustomPartitioner() {
	producer := kafka.NewSyncProducer(
		kafka.WithProducerOptionsSuccesses,
		kafka.WithProducerOptionsPartitioner(examplePartitionerContainer),
	)

	if _, _, err := producer.SendMessage(context.Background(), "test_topic", kafka.ProducerMessage{
		Value: []byte("test"),
	}); err != nil {
		panic(err)
	}
}

func ProducerWithSaramaPartitioner() {
	producer := kafka.NewSyncProducer(
		kafka.WithProducerOptionsSuccesses,
		kafka.WithProducerOptionsPartitioner(kafka.FromSaramaPartitioner(sarama.NewRandomPartitioner)),
	)

	if _, _, err := producer.SendMessage(context.Background(), "test_topic", kafka.ProducerMessage{
		Value: []byte("test"),
	}); err != nil {
		panic(err)
	}
}

func ProducerWithTLSCert() {
	// Must set environment variables:
	// KAFKA_SASL_ENABLED=true
	// KAFKA_USERNAME=username
	// KAFKA_PASSWORD=password
	producer := kafka.NewSyncProducer(
		kafka.WithProducerOptionsTLSCert("path/to/cert.pem", "path/to/key.pem"),
		kafka.WithProducerOptionsSASLMechanism(kafka.SHA512),
	)

	if _, _, err := producer.SendMessage(context.Background(), "test_topic", kafka.ProducerMessage{
		Value: []byte("test"),
	}); err != nil {
		panic(err)
	}
}

func ProducerTLSWithPEMFile() {
	// Must set environment variables:
	// KAFKA_SASL_ENABLED=true
	// KAFKA_USERNAME=username
	// KAFKA_PASSWORD=password
	producer := kafka.NewSyncProducer(
		kafka.WithProducerOptionsTLSCACert("path/to/ca.pem"),
		kafka.WithProducerOptionsSASLMechanism(kafka.SHA512),
	)

	if _, _, err := producer.SendMessage(context.Background(), "test_topic", kafka.ProducerMessage{
		Value: []byte("test"),
	}); err != nil {
		panic(err)
	}
}

type exampleInterceptor struct{}

func (exampleInterceptor) OnSend(*kafka.ProducerFullMessage) {
	log.Println("interceptor called")
}

func examplePartitionerContainer(topic string) kafka.Partitioner {
	return &examplePartitioner{}
}

type examplePartitioner struct{}

func (*examplePartitioner) Partition(message *kafka.ProducerFullMessage, numPartitions int32) (int32, error) {
	log.Println("partitioner called")
	return 0, nil
}

func (*examplePartitioner) RequiresConsistency() bool {
	log.Println("requires consistency called")
	return false
}
