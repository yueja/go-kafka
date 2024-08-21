package producer

import (
	"github.com/IBM/sarama"
	pErr "github.com/pkg/errors"
	"github.com/yueja/cf-kafka/errors"
	"github.com/yueja/go-kafka/config"
)

// SyncProducer 生产者
type SyncProducer struct {
	producer sarama.SyncProducer // 生产者
}

// Config 生产者配置
type Config struct {
	Addresses       []string                      // kafka连接地址
	MaxMessageBytes int                           // 单条消息最大限制，单位byte，默认10M
	Partitioner     sarama.PartitionerConstructor // 自定义分区策略，默认轮训
}

var syncProducer *SyncProducer

// GetSyncProducer 获取生产者实例, version 默认使用V1_0_0_0
func GetSyncProducer() *SyncProducer {
	return syncProducer
}

// InitSyncProducer 初始化生产者
func InitSyncProducer(c *Config) (err error) {
	syncProducer, err = NewSyncProducer(c)
	return
}

// NewSyncProducer 构造新的同步生产者
func NewSyncProducer(c *Config) (p *SyncProducer, err error) {
	if c == nil {
		err = pErr.WithStack(errors.ErrProducerConfigIsNil)
		return
	}

	// 获取默认配置
	conf := config.DefaultProducerConfig()
	if c.MaxMessageBytes > 0 {
		conf.Producer.MaxMessageBytes = c.MaxMessageBytes
	}
	if c.Partitioner != nil {
		conf.Producer.Partitioner = c.Partitioner
	}

	producer, err := sarama.NewSyncProducer(c.Addresses, conf)
	if err != nil {
		err = pErr.WithStack(err)
		return
	}

	p = &SyncProducer{producer: producer}
	return
}

// SendEvent 循环发送多条事件
func (p *SyncProducer) SendEvent(event ...*sarama.ProducerMessage) (err error) {
	for k := range event {
		_, _, err = p.producer.SendMessage(event[k])
		if err != nil {
			err = pErr.WithStack(err)
			return err
		}
	}
	return err
}

// SendSingleEvent 发送单条事件
func (p *SyncProducer) SendSingleEvent(msg *sarama.ProducerMessage) (err error) {
	_, _, err = p.producer.SendMessage(msg)
	err = pErr.WithStack(err)
	return err
}

// SendMultipleEvent 批量发送多条事件
func (p *SyncProducer) SendMultipleEvent(msgList []*sarama.ProducerMessage) (err error) {
	return pErr.WithStack(p.producer.SendMessages(msgList))
}

// Close 注销生产者
func (p *SyncProducer) Close() error {
	return pErr.WithStack(p.producer.Close())
}
