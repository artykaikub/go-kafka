package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
)

// RecordHeader stores key and value for a record header
type RecordHeader struct {
	Key   []byte
	Value []byte
}

type ConsumerMessage struct {
	Headers        []*RecordHeader
	Timestamp      time.Time
	BlockTimestamp time.Time

	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
}

func fromSaramaConsumerMessage(m *sarama.ConsumerMessage) ConsumerMessage {
	headers := []*RecordHeader{}
	for _, h := range m.Headers {
		headers = append(headers, &RecordHeader{
			Key:   h.Key,
			Value: h.Value,
		})
	}

	return ConsumerMessage{
		Headers:        headers,
		Timestamp:      m.Timestamp,
		BlockTimestamp: m.BlockTimestamp,
		Key:            m.Key,
		Value:          m.Value,
		Topic:          m.Topic,
		Partition:      m.Partition,
		Offset:         m.Offset,
	}
}

// Services consuming from Kafka should meet this contract
type Handler interface {
	Execute(ctx context.Context, msg ConsumerMessage) error
}
