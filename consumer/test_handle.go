package consumer

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/yueja/go-kafka/event"
	"time"
)

type TestEventHandler struct {
}

func (handler *TestEventHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (handler *TestEventHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (handler *TestEventHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("%d Message topic:%q partition:%d offset:%d value:%s ", time.Now().UnixNano()/1e6, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		msgEvent := event.Event{}
		err := json.Unmarshal(msg.Value, &msgEvent)
		if err != nil {
			fmt.Print(fmt.Errorf("parse kafka message errors: %v", err))
		}
		err = handler.HandleKafkaMsg(&msgEvent)
		if err != nil {
			fmt.Println(err)
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}

func (handler *TestEventHandler) HandleKafkaMsg(message *event.Event) (err error) {
	switch message.Type {
	case "test":
		fmt.Println("deal message!")
		time.Sleep(500 * time.Millisecond)
	default:
		fmt.Println("not defined event:" + message.Type)
	}
	return
}
