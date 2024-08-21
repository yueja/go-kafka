package consumer

import (
	"context"
	"testing"
)

var address = []string{"127.0.0.1:9193", "127.0.0.1:9194", "127.0.0.1:9195"}

func TestSyncConsumer_ConsumerEvent(t *testing.T) {
	consumer := NewConsumer(&Config{
		Addresses: address,
		Topic:     "topic_test_01",
		Group:     "group_test_01",
		Handler:   new(TestEventHandler),
	})
	consumer.Consume(context.Background(), nil)
}
