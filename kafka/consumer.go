package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"time"
	log "websocket_server/logger"
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
			log.Log.Debugf("Kafka 收到消息: topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)
			h.OnMessage(msg) // 调用你注入的回调
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
		log.Log.Errorf("Kafka customer init failed (non-fatal)：%v", err)
		return
	}

	//go func() {
	//	for {
	//		err := group.Consume(context.Background(), []string{topic}, &MessageHandler{OnMessage: onMessage})
	//		if err != nil {
	//			log.Printf("Kafka consumer err: %v", err)
	//		}
	//	}
	//}()

	go func() {
		retries := 0
		for {
			if retries > 10 {
				log.Log.Error("Kafka consumer 重試次數超過上限，終止連線")
				break
			}

			err := group.Consume(context.Background(), []string{topic}, &MessageHandler{OnMessage: onMessage})
			if err != nil {
				log.Log.Warnf("Kafka 消費異常：%v（第 %d 次重試）", err, retries+1)
				retries++
				time.Sleep(5 * time.Second) // 🔁 避免 log 洪水
			}
		}
	}()
	log.Log.Infof("Kafka consumer's up，subscribing topic:%s，groupID：%s", topic, groupID)

}
