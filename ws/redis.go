package ws

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"strconv"
	"time"
	"websocket_server/config"
)

var Rdb *redis.Client
var ctx = context.Background()

func Init_redis() {
	addr := os.Getenv("REDIS_ADDR")
	password := os.Getenv("REDIS_PASSWORD")
	db, _ := strconv.Atoi(os.Getenv("REDIS_DB"))
	timeoutSec, _ := strconv.Atoi(os.Getenv("REDIS_TIMEOUT"))
	if addr == "" {
		addr = "redis:6379" // ✅ K3s 內網地址（Cluster DNS）
		log.Println("Redis env not set redis:6379")
	} else {
		log.Println("Redis env REDIS_ADDR:", addr)
	}
	Rdb = redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    password,
		DB:          db,
		DialTimeout: time.Duration(timeoutSec) * time.Second,
	})
	if err := Rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	} else {
		log.Println("Redis connection successful")
	}
}

var ttl = config.GetRedisExpireSeconds("chat_message")

// SaveMessageToRedis stores a new chat message in Redis (as raw json)
func SaveMessageToRedis(roomID string, message []byte) error {
	msgKey := "room:" + roomID + ":messages"
	persistKey := "room:" + roomID + ":to_persist"

	pipe := Rdb.Pipeline()

	pipe.LPush(ctx, msgKey, message)
	pipe.LTrim(ctx, msgKey, 0, 49)
	pipe.Expire(ctx, msgKey, time.Duration(ttl)*time.Second) // expire time
	// ✅ 同时写入待入库队列
	pipe.RPush(ctx, persistKey, message)
	pipe.SAdd(ctx, "rooms:active", roomID) // ✅ 注册活跃房间

	_, err := pipe.Exec(ctx)
	return err
}

// GetRecentMessages returns the latest 50 messages from a room
func GetRecentMessages(roomID string) ([]string, error) {
	key := "room:" + roomID + ":messages"
	return Rdb.LRange(ctx, key, 0, 49).Result()
}
