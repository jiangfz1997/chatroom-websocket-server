package main

import (
	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
	"log"
	"websocket_server/config"
	"websocket_server/dynamodb"
	"websocket_server/kafka"
	"websocket_server/ws"
)

var port = ":8081"

func main() {
	config.InitConfig()
	dynamodb.InitDB()
	kafka.InitKafkaProducer([]string{"localhost:9092"})
	kafka.StartKafkaConsumer(
		[]string{"localhost:9092"},
		"chat_messages",
		"ws-consumer-group"+port,
		func(msg *sarama.ConsumerMessage) {
			ws.GlobalHub.BroadcastFromKafka(msg)
		},
	)
	ws.Init_redis()
	r := gin.Default()
	r.GET("/ws/:roomId", ws.ServeWs)
	go ws.StartRedisToDBSyncLoop()
	log.Println("✅ WebSocket Server 启动于 " + port)
	err := r.Run(port)
	if err != nil {
		return
	}
}
