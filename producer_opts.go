package kafka

import (
	"time"

	"github.com/Shopify/sarama"
)

type RequiredAcks int16

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = RequiredAcks(sarama.NoResponse)
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = RequiredAcks(sarama.WaitForLocal)
	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = RequiredAcks(sarama.WaitForAll)
)

type CompressionCodec int

const (
	// CompressionNone no compression
	CompressionNone CompressionCodec = CompressionCodec(sarama.CompressionNone)
	// CompressionGZIP compression using GZIP
	CompressionGZIP CompressionCodec = CompressionCodec(sarama.CompressionGZIP)
	// CompressionSnappy compression using snappy
	CompressionSnappy CompressionCodec = CompressionCodec(sarama.CompressionSnappy)
	// CompressionLZ4 compression using LZ4
	CompressionLZ4 CompressionCodec = CompressionCodec(sarama.CompressionLZ4)
	// CompressionZSTD compression using ZSTD
	CompressionZSTD CompressionCodec = CompressionCodec(sarama.CompressionZSTD)
)

type ProducerFullMessage struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []ProducerHeader
	Metadata  interface{}
	Offset    int64
	Partition int32
	Timestamp time.Time
}

type ProducerInterceptor interface {
	OnSend(*ProducerFullMessage)
}

type producerOptionsRetry struct {
	max         int
	backoff     time.Duration
	backoffFunc func(retries int, maxRetries int) time.Duration
}

type producerOptionsFlush struct {
	frequency   time.Duration
	bytes       int
	messages    int
	maxMessages int
}

type ProducerOptions struct {
	logger          Logger
	requiredAcks    sarama.RequiredAcks
	returnSuccesses bool
	returnErrors    bool
	idempotent      bool
	maxMessageBytes int
	timeout         time.Duration
	saslMechanism   SASLMechanism
	compression     sarama.CompressionCodec
	interceptors    []sarama.ProducerInterceptor
	tlsOptions      tlsOptions

	retry producerOptionsRetry
	flush producerOptionsFlush

	partitioner sarama.PartitionerConstructor
}

// getDefaultProducerOptions descibes the options that will be used when a value isn't provided.
// Default value it will get from default of sarama
func getDefaultProducerOptions() *ProducerOptions {
	defaultConfig := sarama.NewConfig()

	return &ProducerOptions{
		logger:          &stdDebugLogger{},
		requiredAcks:    defaultConfig.Producer.RequiredAcks,
		returnSuccesses: true,
		returnErrors:    defaultConfig.Producer.Return.Errors,
		idempotent:      defaultConfig.Producer.Idempotent,
		maxMessageBytes: defaultConfig.Producer.MaxMessageBytes,
		timeout:         defaultConfig.Producer.Timeout,
		saslMechanism:   "",
		compression:     defaultConfig.Producer.Compression,
		interceptors:    nil,
		tlsOptions: tlsOptions{
			enabled: false,
		},
		retry: producerOptionsRetry{
			max:         defaultConfig.Producer.Retry.Max,
			backoff:     defaultConfig.Producer.Retry.Backoff,
			backoffFunc: nil,
		},
		flush: producerOptionsFlush{
			frequency:   defaultConfig.Producer.Flush.Frequency,
			bytes:       defaultConfig.Producer.Flush.Bytes,
			messages:    defaultConfig.Producer.Flush.Messages,
			maxMessages: defaultConfig.Producer.Flush.MaxMessages,
		},
		partitioner: defaultConfig.Producer.Partitioner,
	}
}

func WithProducerOptionsLogger(l Logger) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.logger = l
	}
}

func WithProducerOptionsRequiredAcks(ra RequiredAcks) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.requiredAcks = sarama.RequiredAcks(ra)
	}
}

func WithProducerOptionsSuccesses(opts *ProducerOptions) {
	opts.returnSuccesses = true
}

func WithProducerOptionsDisableErrors(opts *ProducerOptions) {
	opts.returnErrors = false
}

func WithProducerOptionsIdempotent(opts *ProducerOptions) {
	opts.idempotent = true
}

func WithProducerOptionsMaxMessageBytes(maxMessageBytes int) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.maxMessageBytes = maxMessageBytes
	}
}

func WithProducerOptionsTimeout(timeout time.Duration) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.timeout = timeout
	}
}

func WithProducerOptionsSASLMechanism(m SASLMechanism) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.saslMechanism = m
	}
}

func WithProducerOptionsCompression(c CompressionCodec) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.compression = sarama.CompressionCodec(c)
	}
}

type wrapperProducerInterceptor struct {
	interceptor ProducerInterceptor
}

func (w *wrapperProducerInterceptor) OnSend(msg *sarama.ProducerMessage) {
	keyByte, _ := msg.Key.Encode()
	valueByte, _ := msg.Value.Encode()
	w.interceptor.OnSend(&ProducerFullMessage{
		Topic:     msg.Topic,
		Key:       keyByte,
		Value:     valueByte,
		Headers:   fromSaramaRecordHeader(msg.Headers),
		Metadata:  msg.Metadata,
		Offset:    msg.Offset,
		Partition: msg.Partition,
		Timestamp: msg.Timestamp,
	})
}

func WithProducerOptionsInterceptor(interceptors []ProducerInterceptor) func(opts *ProducerOptions) {
	var saramaInterceptor []sarama.ProducerInterceptor

	for _, in := range interceptors {
		newInterceptor := wrapperProducerInterceptor{
			interceptor: in,
		}
		saramaInterceptor = append(saramaInterceptor, &newInterceptor)
	}

	return func(opts *ProducerOptions) {
		opts.interceptors = saramaInterceptor
	}
}

func WithProducerOptionsMaxRetry(maxRetry int) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.retry.max = maxRetry
	}
}

func WithProducerOptionsRetryBackoff(t time.Duration) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.retry.backoff = t
	}
}

func WithProducerOptionsRetryBackoffFunction(fn func(retries int, maxRetries int) time.Duration) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.retry.backoffFunc = fn
	}
}

func WithProducerOptionsFlushBytes(bytes int) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.flush.bytes = bytes
	}
}

func WithProducerOptionsFlushMessages(messages int) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.flush.messages = messages
	}
}

func WithProducerOptionsFlushMaxMessages(max int) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.flush.maxMessages = max
	}
}

func WithProducerOptionsFlushFrequency(frequency time.Duration) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.flush.frequency = frequency
	}
}

// WithProducerOptionsTLSCert reads and parses a public/private key pair from a pair of files
func WithProducerOptionsTLSCert(certFile, keyFile string) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.tlsOptions.enabled = true
		opts.tlsOptions.certFile = certFile
		opts.tlsOptions.keyFile = keyFile
	}
}

// WithProducerOptionsTLSCACert set PEM encoded certificates with file location
func WithProducerOptionsTLSCACert(caCertsFile string) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.tlsOptions.enabled = true
		opts.tlsOptions.caCertsFile = caCertsFile
		opts.tlsOptions.caCertsByte = nil
	}
}

// WithProducerOptionsTLSStringCACert set PEM encoded certificates with string
func WithProducerOptionsTLSCACertByte(b []byte) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.tlsOptions.enabled = true
		opts.tlsOptions.caCertsByte = b
		opts.tlsOptions.caCertsFile = ""
	}
}

func WithProducerOptionsPartitioner(partitioner PartitionerConstructor) func(opts *ProducerOptions) {
	return func(opts *ProducerOptions) {
		opts.partitioner = toSaramaPartitioner(partitioner)
	}
}

type SendMessageOptions struct {
	metadata  interface{}
	headers   []sarama.RecordHeader
	partition int32
	offset    int64
	timestamp time.Time
}

func getDefaultSendMessageOptions() *SendMessageOptions {
	return &SendMessageOptions{
		metadata:  nil,
		headers:   nil,
		partition: -1,
		offset:    0,
	}
}

func WithSendMessageOptionsHeaders(h []ProducerHeader) func(opts *SendMessageOptions) {
	return func(opts *SendMessageOptions) {
		var headers []sarama.RecordHeader
		for _, header := range h {
			opts.headers = append(headers, sarama.RecordHeader{
				Key:   header.Key,
				Value: header.Value,
			})
		}
	}
}

func WithSendMessageOptionsPartition(p int32) func(opts *SendMessageOptions) {
	return func(opts *SendMessageOptions) {
		opts.partition = p
	}
}

func WithSendMessageOptionsTimestamp(t time.Time) func(opts *SendMessageOptions) {
	return func(opts *SendMessageOptions) {
		opts.timestamp = t
	}
}
