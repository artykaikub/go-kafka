package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

const errorQueueSize = 32

type config struct {
	// Default
	SASLEnabled bool   `envconfig:"KAFKA_SASL_ENABLED" default:"false"`
	Brokers     string `envconfig:"KAFKA_BROKERS"`
	Verbose     bool   `envconfig:"KAFKA_VERBOSE" default:"true"`
	Username    string `envconfig:"KAFKA_USERNAME"`
	Password    string `envconfig:"KAFKA_PASSWORD"`
	ClientID    string `envconfig:"KAFKA_CLIENT_ID" default:"kafka-common-go"`
}

func configureConsumer(conf config, opts *ConsumerOptions) (*sarama.Config, error) {
	saramaConf := sarama.NewConfig()

	if conf.SASLEnabled {
		if err := configureTLS(opts.tlsOptions, saramaConf); err != nil {
			return nil, err
		}

		configureSASL(saramaConf, conf, opts.saslMechanism)
	}

	saramaConf.ClientID = conf.ClientID
	saramaConf.Consumer.Return.Errors = opts.returnErrors

	saramaConf.Consumer.MaxWaitTime = opts.maxWaitTime
	saramaConf.Consumer.MaxProcessingTime = opts.maxProcessingTime

	saramaConf.Consumer.IsolationLevel = sarama.IsolationLevel(opts.isolationLevel)

	saramaConf.Consumer.Interceptors = opts.interceptors

	saramaConf.Consumer.Offsets.Initial = opts.offsets.initial
	saramaConf.Consumer.Offsets.Retention = opts.offsets.retention
	saramaConf.Consumer.Offsets.Retry.Max = opts.offsets.maxRetry
	saramaConf.Consumer.Offsets.AutoCommit = struct {
		Enable   bool
		Interval time.Duration
	}{
		opts.offsets.autoCommit.enabled,
		opts.offsets.autoCommit.interval,
	}

	saramaConf.Consumer.Group.Rebalance.Timeout = opts.group.rebalance.timeout
	saramaConf.Consumer.Group.Rebalance.Retry.Max = opts.group.rebalance.maxRetry
	saramaConf.Consumer.Group.Rebalance.Retry.Backoff = opts.group.rebalance.retryBackoff
	saramaConf.Consumer.Group.Rebalance.GroupStrategies = opts.group.rebalance.groupStrategies
	saramaConf.Consumer.Group.Rebalance.Strategy = opts.group.rebalance.strategy
	saramaConf.Consumer.Group.Member.UserData = opts.group.memberUserData
	saramaConf.Consumer.Group.Heartbeat.Interval = opts.group.heartbeatInterval
	saramaConf.Consumer.Group.Session.Timeout = opts.group.sessionTimeout

	saramaConf.Consumer.Retry.Backoff = opts.retry.backoff
	saramaConf.Consumer.Retry.BackoffFunc = opts.retry.backoffFunc

	saramaConf.Consumer.Fetch.Default = opts.fetch.defaultBytes
	saramaConf.Consumer.Fetch.Max = opts.fetch.maxBytes
	saramaConf.Consumer.Fetch.Min = opts.fetch.minBytes

	return saramaConf, nil
}

// apply env config properties into a Sarama producer config
func configureProducer(conf config, opts *ProducerOptions) (*sarama.Config, error) {
	saramaConf := sarama.NewConfig()

	if conf.SASLEnabled {
		if err := configureTLS(opts.tlsOptions, saramaConf); err != nil {
			return nil, err
		}

		configureSASL(saramaConf, conf, opts.saslMechanism)
	}

	saramaConf.ClientID = conf.ClientID
	saramaConf.Producer.RequiredAcks = opts.requiredAcks
	saramaConf.Producer.Idempotent = opts.idempotent
	saramaConf.Producer.MaxMessageBytes = opts.maxMessageBytes
	saramaConf.Producer.Timeout = opts.timeout

	saramaConf.Producer.Partitioner = opts.partitioner
	saramaConf.Producer.Interceptors = opts.interceptors

	saramaConf.Producer.Compression = opts.compression

	saramaConf.Producer.Flush.Bytes = opts.flush.bytes
	saramaConf.Producer.Flush.Messages = opts.flush.messages
	saramaConf.Producer.Flush.MaxMessages = opts.flush.maxMessages
	saramaConf.Producer.Flush.Frequency = opts.flush.frequency

	saramaConf.Producer.Retry.Max = opts.retry.max
	saramaConf.Producer.Retry.Backoff = opts.retry.backoff
	saramaConf.Producer.Retry.BackoffFunc = opts.retry.backoffFunc

	saramaConf.Producer.Return.Successes = opts.returnSuccesses
	saramaConf.Producer.Return.Errors = opts.returnErrors

	return saramaConf, nil
}

func configureSASL(saramaConf *sarama.Config, conf config, saslMechanism SASLMechanism) {
	saramaConf.Net.SASL.Enable = true
	saramaConf.Net.SASL.User = conf.Username
	saramaConf.Net.SASL.Password = conf.Password
	saramaConf.Net.SASL.Handshake = true

	if saslMechanism == SHA512 {
		saramaConf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: sha512.New} }
		saramaConf.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	} else if saslMechanism == SHA256 {
		saramaConf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: sha256.New} }
		saramaConf.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
	}
}

type tlsOptions struct {
	enabled     bool
	keyFile     string
	certFile    string
	caCertsFile string
	caCertsByte []byte
}

// side effect TLS setup into Sarama config if env config specifies to do so
func configureTLS(tlsopts tlsOptions, saramaConf *sarama.Config) error {
	if tlsopts.enabled {
		var (
			tlsConf = &tls.Config{
				InsecureSkipVerify: true,
			}
			ca = []byte{}
		)

		if tlsopts.certFile != "" || tlsopts.keyFile != "" {
			cert, err := tls.LoadX509KeyPair(tlsopts.certFile, tlsopts.keyFile)
			if err != nil {
				return errors.Wrapf(err, "failed to load TLS cert(%s) and key(%s)", tlsopts.certFile, tlsopts.keyFile)
			}

			tlsConf.Certificates = []tls.Certificate{cert}
		}

		if tlsopts.caCertsFile != "" {
			b, err := os.ReadFile(tlsopts.caCertsFile)
			if err != nil {
				return errors.Wrapf(err, "failed to load CA cert bundle at: %s", tlsopts.caCertsFile)
			}
			ca = b
		} else if tlsopts.caCertsByte != nil {
			ca = tlsopts.caCertsByte
		}

		if ca != nil {
			pool := x509.NewCertPool()
			if ok := pool.AppendCertsFromPEM(ca); !ok {
				return errors.New("kafka: failed to append CA certs to pool")
			}
			tlsConf.RootCAs = pool
		}

		saramaConf.Net.TLS.Enable = true
		saramaConf.Net.TLS.Config = tlsConf
	}

	return nil
}
