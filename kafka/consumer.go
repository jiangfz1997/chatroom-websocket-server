package kafka

import (
	"context"
	"log"
	"time"

	"github.com/IBM/sarama"
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
		//log.Fatalf("Kafka customer init failed: %v", err)
		log.Printf("⚠️ Kafka customer init failed (non-fatal): %v", err)
		return
	}

	//go func() {
	//	for {
	//		err := group.Consume(context.Background(), []string{topic}, &MessageHandler{OnMessage: onMessage})
	//		if err != nil {
	//			log.Printf("⚠️ Kafka consumer err: %v", err)
	//		}
	//	}
	//}()

	go func() {
		retries := 0
		for {
			if retries > 10 {
				log.Println("❌ Kafka consumer 重試次數過多，放棄連接")
				break
			}

			err := group.Consume(context.Background(), []string{topic}, &MessageHandler{OnMessage: onMessage})
			if err != nil {
				log.Printf("⚠️ Kafka 消費異常: %v", err)
				retries++
				time.Sleep(5 * time.Second) // 🔁 避免 log 洪水
			}
		}
	}()
	log.Println("✅ Kafka consumer's up，subscribing topic:", topic)

}
