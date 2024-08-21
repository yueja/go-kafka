package consumer

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/yueja/go-kafka/config"
	"github.com/yueja/go-kafka/errors"
	"log"
	"sync"
)

// Consumer 消费者结构
type Consumer struct {
	Address []string
	Config  *sarama.Config
	Topic   string
	Group   string
	Handler sarama.ConsumerGroupHandler
	Version sarama.KafkaVersion
}

// Config 消费者配置
type Config struct {
	Addresses []string                    // kafka连接地址
	Topic     string                      // Topic信息
	Group     string                      // 消费者组
	Handler   sarama.ConsumerGroupHandler // 消费业务函数
}

// NewConsumer 创建消费者
func NewConsumer(c *Config) *Consumer {
	if len(c.Addresses) == 0 {
		log.Printf("address err:%+v", errors.ErrNoKafkaServerAddress)
		panic(errors.ErrNoKafkaServerAddress)
	}

	if c.Topic == "" {
		log.Printf("topic err:%+v", errors.ErrNoCanConsumerTopic)
		panic(errors.ErrNoCanConsumerTopic)
	}

	if c.Group == "" {
		log.Printf("group err:%+v", errors.ErrNoKafkaConsumerGroup)
		panic(errors.ErrNoKafkaConsumerGroup)
	}
	conf := config.DefaultConsumerConfig()

	consumer := &Consumer{
		Address: c.Addresses,
		Config:  conf,
		Topic:   c.Topic,
		Group:   c.Group,
		Handler: c.Handler,
	}
	return consumer
}

// Consume 进行消费 ctx context.Context, wg *sync.WaitGroup
func (c *Consumer) Consume(superCtx context.Context, superWg *sync.WaitGroup) {
	if superWg != nil {
		defer superWg.Done()
	}
	ctx, cancel := context.WithCancel(superCtx)
	cg, err := sarama.NewConsumerGroup(c.Address, c.Group, c.Config)
	if err != nil {
		log.Printf("consume err:%+v", err)
		panic(err)
	}
	consumerWg := &sync.WaitGroup{}
	consumerWg.Add(1)
	go func() {
		defer func() {
			consumerWg.Done()
			if e := recover(); e != nil {
				log.Printf("consume recover e:%+v", e)
			}
		}()
		for {
			// 触发reBalance时重连
			if err = cg.Consume(ctx, []string{c.Topic}, c.Handler); err != nil {
				log.Printf("cg.consume  consume:%+v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				log.Printf("terminating：拉取下来的信息处理完成")
				return
			}
		}
	}()

	<-superCtx.Done()
	log.Printf("terminating: %s context cancelled", c.Topic)
	// 有中断信号，就关闭当前的上下文，通知当前消费者停止拉取
	cancel()
	// 等待消费者处理完已拉取下来的信息
	consumerWg.Wait()
	if err = cg.Close(); err != nil {
		log.Printf("cg.Close:%+v", err)
	}
	log.Printf("terminating: %s context cancelled:%+v", c.Topic, c.Topic)
}
