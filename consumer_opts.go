package kafka

import (
	"time"

	"github.com/Shopify/sarama"
)

type RebalanceStrategy string

const (
	Range      RebalanceStrategy = "RANGE"
	RoundRobin RebalanceStrategy = "ROUNDROBIN"
	Sticky     RebalanceStrategy = "STICKY"
)

func (r RebalanceStrategy) ToSarama() sarama.BalanceStrategy {
	switch r {
	case Range:
		return sarama.BalanceStrategyRange
	case RoundRobin:
		return sarama.BalanceStrategyRoundRobin
	case Sticky:
		return sarama.BalanceStrategySticky
	default:
		return sarama.BalanceStrategyRange
	}
}

type IsolationLevel int8

const (
	ReadUncommitted IsolationLevel = IsolationLevel(sarama.ReadUncommitted)
	ReadCommitted   IsolationLevel = IsolationLevel(sarama.ReadCommitted)
)

type ConsumerInterceptor interface {
	// OnConsume is called when the consumed message is intercepted. Please
	// avoid modifying the message until it's safe to do so, as this is _not_ a
	// copy of the message.
	OnConsume(*ConsumerMessage)
}

// getDefaultConsumerOptions descibes the options that will be used when a value isn't provided.
// Default value it will get from default of sarama
func getDefaultConsumerOptions() ConsumerOptions {
	defaultConfig := sarama.NewConfig()
	return ConsumerOptions{
		logger:       &stdDebugLogger{},
		returnErrors: false,
		tlsOptions: tlsOptions{
			enabled: false,
		},
		saslMechanism:     "",
		maxWaitTime:       defaultConfig.Consumer.MaxWaitTime,
		maxProcessingTime: defaultConfig.Consumer.MaxProcessingTime,
		isolationLevel:    ReadUncommitted,
		interceptors:      nil,

		group: consumerOptionsGroup{
			rebalance: consumerOptionsRebalance{
				timeout:         defaultConfig.Consumer.Group.Rebalance.Timeout,
				maxRetry:        defaultConfig.Consumer.Group.Rebalance.Retry.Max,
				retryBackoff:    defaultConfig.Consumer.Group.Rebalance.Retry.Backoff,
				strategy:        defaultConfig.Consumer.Group.Rebalance.Strategy,
				groupStrategies: defaultConfig.Consumer.Group.Rebalance.GroupStrategies,
			},
			heartbeatInterval: defaultConfig.Consumer.Group.Heartbeat.Interval,
			sessionTimeout:    defaultConfig.Consumer.Group.Session.Timeout,
			memberUserData:    defaultConfig.Consumer.Group.Member.UserData,
		},
		offsets: consumerOptionsOffsets{
			initial:   defaultConfig.Consumer.Offsets.Initial,
			retention: defaultConfig.Consumer.Offsets.Retention,
			maxRetry:  defaultConfig.Consumer.Offsets.Retry.Max,
			autoCommit: consumerOptionsAutoCommit{
				enabled:  defaultConfig.Consumer.Offsets.AutoCommit.Enable,
				interval: defaultConfig.Consumer.Offsets.AutoCommit.Interval,
			},
		},
		retry: consumerOptionsRetry{
			backoff:     defaultConfig.Consumer.Retry.Backoff,
			backoffFunc: nil,
		},
		fetch: consumerOptionsFetch{
			minBytes:     defaultConfig.Consumer.Fetch.Min,
			maxBytes:     defaultConfig.Consumer.Fetch.Max,
			defaultBytes: defaultConfig.Consumer.Fetch.Default,
		},
	}
}

type consumerOptionsRebalance struct {
	timeout         time.Duration
	maxRetry        int
	retryBackoff    time.Duration
	strategy        sarama.BalanceStrategy
	groupStrategies []sarama.BalanceStrategy
}

type consumerOptionsGroup struct {
	rebalance         consumerOptionsRebalance
	heartbeatInterval time.Duration
	sessionTimeout    time.Duration
	memberUserData    []byte
}

type consumerOptionsRetry struct {
	backoff     time.Duration
	backoffFunc func(retries int) time.Duration
}

type consumerOptionsAutoCommit struct {
	enabled  bool
	interval time.Duration
}

type consumerOptionsOffsets struct {
	initial    int64
	retention  time.Duration
	maxRetry   int
	autoCommit consumerOptionsAutoCommit
}

type consumerOptionsFetch struct {
	minBytes     int32
	maxBytes     int32
	defaultBytes int32
}

type ConsumerOptions struct {
	logger            Logger
	returnErrors      bool
	tlsOptions        tlsOptions
	saslMechanism     SASLMechanism
	maxWaitTime       time.Duration
	maxProcessingTime time.Duration
	isolationLevel    IsolationLevel
	interceptors      []sarama.ConsumerInterceptor

	group   consumerOptionsGroup
	offsets consumerOptionsOffsets
	retry   consumerOptionsRetry
	fetch   consumerOptionsFetch
}

func WithConsumerOptionsLogger(l Logger) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.logger = l
	}
}

func WithConsumerOptionsReturnErrors(opts *ConsumerOptions) {
	opts.returnErrors = true
}

// WithConsumerOptionsTLSCert reads and parses a public/private key pair from a pair of files
func WithConsumerOptionsTLSCert(certFile, keyFile string) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.tlsOptions.enabled = true
		opts.tlsOptions.certFile = certFile
		opts.tlsOptions.keyFile = keyFile
	}
}

// WithConsumerOptionsTLSCACert set PEM encoded certificates with file location
func WithConsumerOptionsTLSCACert(caCertsFile string) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.tlsOptions.enabled = true
		opts.tlsOptions.caCertsFile = caCertsFile
		opts.tlsOptions.caCertsByte = nil
	}
}

// WithConsumerOptionsTLSStringCACert set PEM encoded certificates with string
func WithConsumerOptionsTLSCACertByte(b []byte) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.tlsOptions.enabled = true
		opts.tlsOptions.caCertsByte = b
		opts.tlsOptions.caCertsFile = ""
	}
}

func WithConsumerOptionsSASLMechanism(m SASLMechanism) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.saslMechanism = m
	}
}

func WithConsumerOptionsMaxWaitTime(t time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.maxWaitTime = t
	}
}

func WithConsumerOptionsMaxProcessingTime(t time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.maxProcessingTime = t
	}
}

func WithConsumerOptionsIsolationLevel(lv IsolationLevel) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.isolationLevel = lv
	}
}

type wrapperConsumerInterceptor struct {
	interceptor ConsumerInterceptor
}

func (w *wrapperConsumerInterceptor) OnConsume(msg *sarama.ConsumerMessage) {
	m := fromSaramaConsumerMessage(msg)
	w.interceptor.OnConsume(&m)
}

func WithConsumerOptionsInterceptor(interceptors []ConsumerInterceptor) func(opts *ConsumerOptions) {
	var saramaInterceptor []sarama.ConsumerInterceptor

	for _, in := range interceptors {
		newInterceptor := wrapperConsumerInterceptor{
			interceptor: in,
		}
		saramaInterceptor = append(saramaInterceptor, &newInterceptor)
	}

	return func(opts *ConsumerOptions) {
		opts.interceptors = saramaInterceptor
	}
}

func WithConsumerOptionsGroupRebalanceTimeout(t time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.group.rebalance.timeout = t
	}
}

func WithConsumerOptionsGroupRebalanceMaxRetry(maxRetry int) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.group.rebalance.maxRetry = maxRetry
	}
}

func WithConsumerOptionsGroupRebalanceRetryBackoff(t time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.group.rebalance.retryBackoff = t
	}
}

func WithConsumerOptionsGroupRebalanceGroupStrategies(rs []RebalanceStrategy) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		for _, r := range rs {
			opts.group.rebalance.groupStrategies = append(opts.group.rebalance.groupStrategies, r.ToSarama())
		}
	}
}

// WithConsumerOptionsGroupRebalanceStrategy strategy for allocating topic partitions to members.
func WithConsumerOptionsGroupRebalanceStrategy(rs RebalanceStrategy) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.group.rebalance.strategy = rs.ToSarama()
	}
}

// HearthbeatInterval is the time between heartbeats to the consumer group coordinator.
func WithConsumerOptionsGroupHeartbeatInterval(t time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.group.heartbeatInterval = t
	}
}

// SessionTimeout is the timeout for a consumer group session.
func WithConsumerOptionsGroupSessionTimeout(t time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.group.sessionTimeout = t
	}
}

func WithConsumerOptionsGroupMemberUserData(data []byte) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.group.memberUserData = data
	}
}

// WithConsumerOptionsOffsetOldest The initial offset to use if no offset was previously committed.
// Defaults to OffsetNewest. If set this options initial offset will change to OffsetOldest
func WithConsumerOptionsOffsetOldest(opts *ConsumerOptions) {
	opts.offsets.initial = sarama.OffsetOldest
}

func WithConsumerOptionsOffsetsRetention(t time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.offsets.retention = t
	}
}

func WithConsumerOptionsOffsetsMaxRetry(maxRetry int) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.offsets.maxRetry = maxRetry
	}
}

func WithConsumerOptionsOffsetsAutoCommit(isEnabled bool, t time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.offsets.autoCommit.enabled = isEnabled
		if isEnabled {
			opts.offsets.autoCommit.interval = t
		}
	}
}

func WithConsumerOptionsRetryBackoff(t time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.retry.backoff = t
	}
}

func WithConsumerOptionsRetryBackoffFunction(fn func(retries int) time.Duration) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.retry.backoffFunc = fn
	}
}

// Fetch

func WithConsumerOptionsFetchDefault(bytes int32) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.fetch.defaultBytes = bytes
	}
}

func WithConsumerOptionsFetchMin(bytes int32) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.fetch.minBytes = bytes
	}
}

func WithConsumerOptionsFetchMax(bytes int32) func(opts *ConsumerOptions) {
	return func(opts *ConsumerOptions) {
		opts.fetch.maxBytes = bytes
	}
}
