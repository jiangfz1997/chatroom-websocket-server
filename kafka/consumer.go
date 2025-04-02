package kafka

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	Group   sarama.ConsumerGroup
	Handler *MessageHandler
}

type MessageHandler struct {
	OnMessage func(*sarama.ConsumerMessage)
}

func (h *MessageHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *MessageHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *MessageHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if h.OnMessage != nil {
			h.OnMessage(msg) // ✅ 调用你注入的回调
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

func StartKafkaConsumer(brokers []string, topic string, groupID string, onMessage func(*sarama.ConsumerMessage)) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("❌ Kafka 消費者初始化失敗: %v", err)
	}

	go func() {
		for {
			err := group.Consume(context.Background(), []string{topic}, &MessageHandler{OnMessage: onMessage})
			if err != nil {
				log.Printf("⚠️ Kafka 消費異常: %v", err)
			}
		}
	}()
	log.Println("✅ Kafka 消費者已啟動，訂閱 topic:", topic)

}
