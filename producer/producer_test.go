package producer

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/yueja/go-kafka/event"
	"testing"
	"time"
)

var address = []string{"127.0.0.1:9193", "127.0.0.1:9194", "127.0.0.1:9195"}

func TestInitSyncProducer(t *testing.T) {
	type args struct {
		addresses []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test_01",
			args: args{
				addresses: address,
			},
			wantErr: true,
		},
		{
			name: "test_02",
			args: args{
				addresses: []string{"127.0.0.1:9194"},
			},
			wantErr: false,
		},
		{
			name: "test_03",
			args: args{
				addresses: []string{"127.0.0.1:9095"},
			},
			wantErr: true,
		},
		{
			name: "test_04",
			args: args{
				addresses: []string{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "test_04" {
				if err := InitSyncProducer(nil); (err != nil) != tt.wantErr {
					t.Errorf("InitSyncProducer() name= %v, error = %v, wantErr %v", tt.name, err, tt.wantErr)
				}
			} else {
				if err := InitSyncProducer(&Config{
					Addresses:       tt.args.addresses,
					MaxMessageBytes: 0,
				}); (err != nil) != tt.wantErr {
					t.Errorf("InitSyncProducer() name= %v, error = %v, wantErr %v", tt.name, err, tt.wantErr)
				}
			}
		})
	}
}

func TestSyncProducer_SendEvent(t *testing.T) {
	producer, err := NewSyncProducer(&Config{
		Addresses:       address,
		MaxMessageBytes: 100,
	})
	if err != nil {
		panic(err)
	}

	header := []sarama.RecordHeader{
		{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}
	for i := 0; i < 100; i++ {
		value := event.Event{
			Type: "test",
			Data: fmt.Sprintf("我是第%d条消息", i),
		}
		var body []byte
		if body, err = json.Marshal(value); err != nil {
			return
		}
		message := &sarama.ProducerMessage{
			Topic:     "topic_test_01",
			Key:       nil,
			Value:     sarama.ByteEncoder(body),
			Headers:   header,
			Metadata:  nil,
			Timestamp: time.Now(),
		}
		if err = producer.SendSingleEvent(message); err != nil {
			panic(err)
		}
		fmt.Printf("我是第%d条消息\n", i)
		time.Sleep(1 * time.Second)
	}
}
