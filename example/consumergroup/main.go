package main

import (
	"context"
	"log"

	"github.com/artykaikub/go-kafka"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	godotenv.Load()

	ConsumerSimple()
	// ConsumerWithCustomLogger()
	// ConsumerTLSWithPEMFile()
	// ConsumerTLSWithCert()
	// ConsumerWithInterceptor()
}

func ConsumerSimple() {
	consumerGroup := kafka.NewConsumerGroup(
		"group_name",
		kafka.WithConsumerOptionsReturnErrors,
	)

	consumerGroup.Consume(context.Background(), []string{"test_topic"}, exampleHandler{})
}

func ConsumerWithCustomLogger() {
	lg, _ := zap.NewDevelopment()

	consumerGroup := kafka.NewConsumerGroup(
		"group_name",
		kafka.WithConsumerOptionsReturnErrors,
		kafka.WithConsumerOptionsLogger(lg.Sugar()),
	)

	consumerGroup.Consume(context.Background(), []string{"test_topic"}, exampleHandler{})
}

func ConsumerTLSWithPEMFile() {
	// Must set environment variables:
	// KAFKA_SASL_ENABLED=true
	// KAFKA_USERNAME=username
	// KAFKA_PASSWORD=password
	consumerGroup := kafka.NewConsumerGroup(
		"group_name",
		kafka.WithConsumerOptionsReturnErrors,
		kafka.WithConsumerOptionsTLSCACert("path/to/ca.pem"),
		kafka.WithConsumerOptionsSASLMechanism(kafka.SHA512),
	)

	consumerGroup.Consume(context.Background(), []string{"test_topic"}, exampleHandler{})
}

func ConsumerTLSWithCert() {
	// Must set environment variables:
	// KAFKA_SASL_ENABLED=true
	// KAFKA_USERNAME=username
	// KAFKA_PASSWORD=password
	consumerGroup := kafka.NewConsumerGroup(
		"group_name",
		kafka.WithConsumerOptionsReturnErrors,
		kafka.WithConsumerOptionsTLSCert("path/to/cert.pem", "path/to/key.pem"),
		kafka.WithConsumerOptionsSASLMechanism(kafka.SHA512),
	)

	consumerGroup.Consume(context.Background(), []string{"test_topic"}, exampleHandler{})
}

func ConsumerWithInterceptor() {
	consumerGroup := kafka.NewConsumerGroup(
		"group_name",
		kafka.WithConsumerOptionsReturnErrors,
		kafka.WithConsumerOptionsInterceptor([]kafka.ConsumerInterceptor{exampleInterceptor{}}),
	)

	consumerGroup.Consume(context.Background(), []string{"test_topic"}, exampleHandler{})
}

type exampleHandler struct{}

func (exampleHandler) Execute(ctx context.Context, msg kafka.ConsumerMessage) error {
	log.Println("handler called")
	return nil
}

type exampleInterceptor struct{}

func (exampleInterceptor) OnConsume(*kafka.ConsumerMessage) {
	log.Println("interceptor called")
}
