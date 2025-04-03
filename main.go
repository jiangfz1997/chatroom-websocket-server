package main

import (
	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"log"
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
	logger.InitLogger() // 初始化日志系统
	log := logger.Log   // 使用自定义 logrus 实例
	log.Info("服务器启动流程开始")
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
	ws.Init_redis()
	ws.GlobalHub.ServerID = setupServerID()
	r := gin.Default()
	r.GET("/ws/:roomId", ws.ServeWs)

	p := os.Getenv("PORT")
	if p == "" {
		log.Info("未设置端口，使用默认端口 8081")
		p = "8081"
	}
	port = ":" + p
	log.Info("✅ WebSocket Server starting on " + port)
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
		log.Println("📡 Using local random ServerID:", finalID)
		return finalID
	}

	log.Println("📡Using ServerID from .env:", baseID)
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
