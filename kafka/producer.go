package kafka

import (
	"github.com/IBM/sarama"
	"time"
	log "websocket_server/logger"
)

var Producer sarama.SyncProducer

func InitKafkaProducer(brokers []string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	log.Log.Infof("Initializing Kafka producer with brokers:%v", brokers)
	var err error
	maxRetries := 10
	for i := 1; i <= maxRetries; i++ {
		Producer, err = sarama.NewSyncProducer(brokers, config)
		if err == nil {
			log.Log.Info("Kafka producer initialized successfully")
			return
		}
		log.Log.Warnf("Kafka producer init failed (attempt %d/%d)：%v", i, maxRetries, err)
		time.Sleep(3 * time.Second)
	}

	log.Log.Fatalf("Kafka producer failed after %d attempts: %v", maxRetries, err)
}
