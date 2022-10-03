package kafka

import "github.com/Shopify/sarama"

// PartitionerConstructor is the type for a function capable of constructing new Partitioners.
type PartitionerConstructor func(topic string) Partitioner

type Partitioner interface {
	Partition(message *ProducerFullMessage, numPartitions int32) (int32, error)
	RequiresConsistency() bool
}

type wrapperPartitioner struct {
	partitioner Partitioner
}

func (w *wrapperPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	keyByte, _ := message.Key.Encode()
	valueByte, _ := message.Value.Encode()
	return w.partitioner.Partition(&ProducerFullMessage{
		Topic:     message.Topic,
		Key:       keyByte,
		Value:     valueByte,
		Headers:   fromSaramaRecordHeader(message.Headers),
		Metadata:  message.Metadata,
		Offset:    message.Offset,
		Partition: message.Partition,
		Timestamp: message.Timestamp,
	}, numPartitions)
}

func (w *wrapperPartitioner) RequiresConsistency() bool {
	return w.partitioner.RequiresConsistency()
}

type wrapperSaramaPartitioner struct {
	partitioner sarama.Partitioner
}

func (w *wrapperSaramaPartitioner) Partition(message *ProducerFullMessage, numPartitions int32) (int32, error) {
	return w.partitioner.Partition(&sarama.ProducerMessage{
		Topic:     message.Topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Headers:   toSaramaRecordHeader(message.Headers),
		Metadata:  message.Metadata,
		Offset:    message.Offset,
		Partition: message.Partition,
		Timestamp: message.Timestamp,
	}, numPartitions)
}

func (w *wrapperSaramaPartitioner) RequiresConsistency() bool {
	return w.partitioner.RequiresConsistency()
}

func FromSaramaPartitioner(partitioner sarama.PartitionerConstructor) PartitionerConstructor {
	return func(topic string) Partitioner {
		return &wrapperSaramaPartitioner{
			partitioner: partitioner(topic),
		}
	}
}

func toSaramaPartitioner(partitioner PartitionerConstructor) sarama.PartitionerConstructor {
	return func(topic string) sarama.Partitioner {
		return &wrapperPartitioner{
			partitioner: partitioner(topic),
		}
	}
}
