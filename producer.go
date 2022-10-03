package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
)

type ProducerHeader struct {
	Key   []byte
	Value []byte
}

func toSaramaRecordHeader(h []ProducerHeader) []sarama.RecordHeader {
	var headers []sarama.RecordHeader
	for _, header := range h {
		headers = append(headers, sarama.RecordHeader{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	return headers
}

func fromSaramaRecordHeader(h []sarama.RecordHeader) []ProducerHeader {
	var headers []ProducerHeader
	for _, header := range h {
		headers = append(headers, ProducerHeader{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	return headers
}

type ProducerMessage struct {
	Key   []byte
	Value []byte
}

type AsyncProducer interface {
	Emit(ctx context.Context, topic string, msg ProducerMessage, optFuncs ...func(*SendMessageOptions)) error
	Background(ctx context.Context) (func(), chan error)
}

type SyncProducer interface {
	SendMessage(ctx context.Context, topic string, msg ProducerMessage, optFuncs ...func(*SendMessageOptions)) (partition int32, offset int64, err error)
}

type producer[T any] struct {
	producer T
	logger   Logger

	errors chan error
}

func newDefaultProducer(optFuncs []func(*ProducerOptions)) (config, *sarama.Config, *ProducerOptions) {
	var conf config
	envconfig.MustProcess("", &conf)

	opts := getDefaultProducerOptions()
	for _, optFunc := range optFuncs {
		optFunc(opts)
	}

	if !conf.Verbose {
		opts.logger = &nullLogger{}
	}

	saramaConf, err := configureProducer(conf, opts)
	if err != nil {
		opts.logger.Fatalf(fmt.Sprintf("%v\n", err))
	}

	return conf, saramaConf, opts
}

func NewSyncProducer(optFuncs ...func(*ProducerOptions)) SyncProducer {
	conf, saramaConf, opts := newDefaultProducer(optFuncs)
	p, err := createSyncProducer(conf, saramaConf, opts)
	if err != nil {
		opts.logger.Fatalf(err.Error())
	}

	return &producer[sarama.SyncProducer]{
		producer: p,
		logger:   opts.logger,
	}
}

func NewAsyncProducer(optFuncs ...func(*ProducerOptions)) AsyncProducer {
	conf, saramaConf, opts := newDefaultProducer(optFuncs)
	p, err := createAsyncProducer(conf, saramaConf, opts)
	if err != nil {
		opts.logger.Fatalf(err.Error())
	}

	return &producer[sarama.AsyncProducer]{
		producer: p,
		logger:   opts.logger,
		errors:   make(chan error, errorQueueSize),
	}
}

func (p *producer[T]) Emit(ctx context.Context, topic string, msg ProducerMessage, optFuncs ...func(*SendMessageOptions)) error {
	kp := any(p.producer).(sarama.AsyncProducer)

	if err := validateMessage(topic, msg); err != nil {
		return err
	}

	opts := getDefaultSendMessageOptions()
	for _, optFunc := range optFuncs {
		optFunc(opts)
	}

	// if shutdown is triggered, drop the message
	select {
	case <-ctx.Done():
		return errors.Wrapf(ctx.Err(), "message lost: shutdown triggered during send")
	case kp.Input() <- &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(msg.Key),
		Value:     sarama.ByteEncoder(msg.Value),
		Headers:   opts.headers,
		Partition: opts.partition,
		Timestamp: opts.timestamp,
	}:
		p.logger.Infof("message sent: topic [%s]", topic)
	}

	return nil
}

// Background caller should run the returned function in a goroutine,
// and consume the returned error channel until it's closed at shutdown.
func (p *producer[T]) Background(ctx context.Context) (func(), chan error) {
	kp := any(p.producer).(sarama.AsyncProducer)
	// proxy all Sarama errors to the caller until Close() drains and closes it
	go func() {
		for err := range kp.Errors() {
			p.errors <- err
		}

		p.logger.Errorf("producer: shutting down error reporter")
	}()

	return func() {
		defer func() {
			if err := kp.Close(); err != nil {
				p.errors <- err
			}
			close(p.errors)
		}()

		for {
			select {
			case successes := <-kp.Successes():
				p.logger.Infof("send message successfully to topic [%s] partition [%d] offset [%d]", successes.Topic, successes.Partition, successes.Offset)
			case <-ctx.Done():
				p.logger.Warnf("producer: shutdown triggered")
				return
			}
		}
	}, p.errors
}

func (p *producer[T]) SendMessage(ctx context.Context, topic string, msg ProducerMessage, optFuncs ...func(*SendMessageOptions)) (partition int32, offset int64, err error) {
	producer := any(p.producer).(sarama.SyncProducer)

	if err := validateMessage(topic, msg); err != nil {
		return 0, 0, err
	}

	opts := getDefaultSendMessageOptions()
	for _, optFunc := range optFuncs {
		optFunc(opts)
	}

	return producer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(msg.Key),
		Value:     sarama.ByteEncoder(msg.Value),
		Headers:   opts.headers,
		Partition: opts.partition,
		Timestamp: opts.timestamp,
	})
}

func validateMessage(topic string, msg ProducerMessage) error {
	if len(topic) == 0 {
		return errors.New("producer: message topic is required")
	}

	if len(msg.Key) == 0 && len(msg.Value) == 0 {
		return errors.New("producer: at least one of message fields key or value is required")
	}

	return nil
}

func createSyncProducer(conf config, saramaConf *sarama.Config, opts *ProducerOptions) (sarama.SyncProducer, error) {
	brokers := strings.Split(conf.Brokers, ",")
	producer, err := sarama.NewSyncProducer(brokers, saramaConf)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create kafka sync producer")
	}

	return producer, nil
}

func createAsyncProducer(conf config, saramaConf *sarama.Config, opts *ProducerOptions) (sarama.AsyncProducer, error) {
	brokers := strings.Split(conf.Brokers, ",")
	producer, err := sarama.NewAsyncProducer(brokers, saramaConf)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create kafka async producer")
	}

	return producer, nil
}
