package errors

import "github.com/pkg/errors"

// 错误定义
var (
	// ErrNoKafkaServerAddress 没有可用的kafka服务地址
	ErrNoKafkaServerAddress = errors.New("no kafka server address")
	// ErrNoCanConsumerTopic 没有可消费的同topic
	ErrNoCanConsumerTopic = errors.New("no can consumer topic")
	// ErrNoKafkaConsumerGroup 没有消费组
	ErrNoKafkaConsumerGroup = errors.New("no kafka consumer group")
	// ErrConsumerConfigIsNil 消费者配置错误
	ErrConsumerConfigIsNil = errors.New("consumer config is nil")
	// ErrProducerConfigIsNil 生产者配置错误
	ErrProducerConfigIsNil = errors.New("producer config is nil")
	// ErrConsumerHandlerTimeout 消费超时
	ErrConsumerHandlerTimeout = errors.New("consumer handler timeout")
)
