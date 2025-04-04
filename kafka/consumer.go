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
			log.Log.Debugf("Kafka recv msg: topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)
			h.OnMessage(msg)
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
		log.Log.Fatalf("Kafka customer init failed (fatal)：%v", err)
		return
	}

	go func() {
		retries := 0
		for {
			if retries > 10 {
				log.Log.Error("Kafka consumer failed after 10 retries")
				break
			}

			err := group.Consume(context.Background(), []string{topic}, &MessageHandler{OnMessage: onMessage})
			if err != nil {
				log.Log.Warnf("Kafka consumer failed ：%v（attempt: %d）", err, retries+1)
				retries++
				time.Sleep(5 * time.Second)
			}
		}
	}()
	log.Log.Infof("Kafka consumer's up，subscribing topic:%s，groupID：%s", topic, groupID)

}
