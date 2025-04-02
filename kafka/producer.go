// kafka/producer.go
package kafka

import (
	"github.com/Shopify/sarama"
	"log"
)

var Producer sarama.SyncProducer

func InitKafkaProducer(brokers []string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	var err error
	Producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Kafka 生產者初始化失敗: %v", err)
	}

	log.Println("Kafka 生產者就緒")
}
