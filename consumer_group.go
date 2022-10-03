package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kelseyhightower/envconfig"
)

type ConsumerGroup interface {
	Consume(ctx context.Context, topics []string, handler Handler)
}

type consumerGroup struct {
	consumer sarama.ConsumerGroup
	ready    chan bool
	handler  Handler

	autoCommit        bool
	rebalancedTimeout time.Duration

	logger Logger
}

func NewConsumerGroup(group string, optFuncs ...func(*ConsumerOptions)) ConsumerGroup {
	var conf config
	envconfig.MustProcess("", &conf)

	defaultOptions := getDefaultConsumerOptions()
	opts := &defaultOptions
	for _, optFunc := range optFuncs {
		optFunc(opts)
	}

	if !conf.Verbose {
		opts.logger = &nullLogger{}
	}

	saramaConf, err := configureConsumer(conf, opts)
	if err != nil {
		log.Fatalf("error configuring consumer: %v", err)
	}

	// config should have a CSV list of brokers
	brokers := strings.Split(conf.Brokers, ",")

	// create consumer group w/underlying managed client.
	// docs recommend 1 client per producer or consumer for throughput
	consumer, err := sarama.NewConsumerGroup(brokers, group, saramaConf)
	if err != nil {
		log.Fatalf("failed to creating consumer group message handler: %v", err)
	}

	return &consumerGroup{
		consumer: consumer,
		ready:    make(chan bool),

		autoCommit:        saramaConf.Consumer.Offsets.AutoCommit.Enable,
		rebalancedTimeout: saramaConf.Consumer.Group.Rebalance.Timeout,

		logger: opts.logger,
	}
}

// caller should run the returned function in a goroutine, and consume
// the returned error channel until it's closed at shutdown.
func (c *consumerGroup) Consume(ctx context.Context, topics []string, handler Handler) {
	defer func() {
		if err := c.consumer.Close(); err != nil {
			c.logger.Panicf("error closing consumer: %v", err)
		}
	}()

	c.handler = handler

	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		// the main consume loop, parent of the ConsumerClaim() partition message loops.
		for ctx.Err() == nil {
			c.logger.Infof("consumer: beginning loop for topic(s): %s", topics)

			// if Consume() returns nil, a rebalance is in progress and it should be called again.
			// if Consume() returns an error, the consumer should break the loop, shutting down.
			if err := c.consumer.Consume(ctx, topics, c); err != nil {
				c.logger.Panicf("error from consumer: %v", err)
			}

			c.ready = make(chan bool)
		}
	}()

	<-c.ready
	c.handleSignal(ctx)

	cancel()
	wg.Wait()
}

// handleSignal wait for shutdown/pause signal and close/pause it gracefully
func (c *consumerGroup) handleSignal(ctx context.Context) {
	var (
		isRunning           = true
		isPausedConsumption = false
	)

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for isRunning {
		select {
		case <-ctx.Done():
			c.logger.Warnf("terminating: context cancelled")
			isRunning = false
		case <-sigterm:
			c.logger.Warnf("terminating: via signal")
			isRunning = false
		case <-sigusr1:
			c.toggleConsumptionFlow(&isPausedConsumption)
		}
	}
}

func (c *consumerGroup) toggleConsumptionFlow(isPaused *bool) {
	if *isPaused {
		c.consumer.ResumeAll()
		c.logger.Warnf("consumer: resuming consumption")
	} else {
		c.consumer.PauseAll()
		c.logger.Warnf("consumer: pausing consumption")
	}

	*isPaused = !*isPaused
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *consumerGroup) Setup(session sarama.ConsumerGroupSession) error {
	// Mark the consumer as readyclose(c.ready)
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *consumerGroup) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
func (c *consumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Infof("start consumer with partition [%d]", claim.Partition())

	for {
		select {
		case message := <-claim.Messages():
			if session.Context().Err() != nil {
				c.logger.Errorf("consumer partition [%d]: %v", claim.Partition(), session.Context().Err())
				c.logger.Warnf("consumer partition [%d]: stopping...", claim.Partition())
				return nil
			}
			c.logger.Infof("consumer: consuming message with \nkey: %s\nvalue: %s\npartition: %d\ntopic: %s", string(message.Key), string(message.Value), message.Partition, message.Topic)

			if err := c.handler.Execute(session.Context(), fromSaramaConsumerMessage(message)); err != nil {
				c.logger.Errorf(fmt.Sprintf("error from executor, err: %v", err))
				return err
			}

			session.MarkMessage(message, "")
			if !c.autoCommit {
				session.Commit()
			}
		case <-session.Context().Done():
			c.logger.Warnf("consumer partition [%d]: stopping...", claim.Partition())
			return nil
		}
	}
}
