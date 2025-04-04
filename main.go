package main

import (
	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"websocket_server/redis"

	//"log"
	"math/rand"
	"os"
	"strings"
	"time"
	"websocket_server/config"
	"websocket_server/dynamodb"
	"websocket_server/kafka"
	"websocket_server/logger"
	"websocket_server/ws"
)

var port = ":8081"

func main() {
	logger.InitLogger()
	log := logger.Log
	log.Info("Server starting...")
	config.InitConfig()
	_ = godotenv.Load(".env")

	dynamodb.InitDB()

	kafka.InitKafkaProducer(strings.Split(os.Getenv("KAFKA_BROKERS"), ","))
	kafka.StartKafkaConsumer(
		strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		os.Getenv("KAFKA_TOPIC"),
		os.Getenv("SERVER_ID"),
		func(msg *sarama.ConsumerMessage) {
			ws.GlobalHub.BroadcastFromKafka(msg)
		},
	)
	redis.Init_redis()
	ws.GlobalHub.ServerID = setupServerID()
	r := gin.Default()
	r.GET("/ws/:roomId", ws.ServeWs)

	p := os.Getenv("PORT")
	if p == "" {
		log.Info("Port not set, using default 8081")
		p = "8081"
	}
	port = ":" + p
	log.Info("WebSocket Server starting on " + port)
	err := r.Run(port)
	if err != nil {
		return
	}
}

// setupServerID initializes the server ID based on environment variables
func setupServerID() string {
	baseID := os.Getenv("SERVER_ID")
	if baseID == "" {
		baseID = "ws-dev"
	}

	if strings.HasPrefix(baseID, "ws-local") {
		suffix := randSuffix()
		finalID := baseID + "-" + suffix
		logger.Log.Infof("📡 Using local random ServerID: %s", finalID)
		return finalID
	}

	logger.Log.Infof("📡Using ServerID from .env: %s", baseID)
	return baseID
}

// generate a random suffix
func randSuffix() string {
	rand.Seed(time.Now().UnixNano())
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	s := make([]byte, 6)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
