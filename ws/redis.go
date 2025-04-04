package ws

import (
	"context"
	"github.com/redis/go-redis/v9"
	"os"
	"strconv"
	"time"
	"websocket_server/config"
	log "websocket_server/logger"
)

var Rdb *redis.Client
var ctx = context.Background()

func Init_redis() {
	addr := os.Getenv("REDIS_ADDR")
	password := os.Getenv("REDIS_PASSWORD")
	db, _ := strconv.Atoi(os.Getenv("REDIS_DB"))
	timeoutSec, _ := strconv.Atoi(os.Getenv("REDIS_TIMEOUT"))
	if addr == "" {
		addr = "redis:6379" // K3s 內網地址（Cluster DNS）
		log.Log.Warn("Redis env not set redis:6379")
	} else {
		log.Log.Infof("Redis env REDIS_ADDR:", addr)
	}
	Rdb = redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    password,
		DB:          db,
		DialTimeout: time.Duration(timeoutSec) * time.Second,
	})
	if err := Rdb.Ping(ctx).Err(); err != nil {
		log.Log.Fatalf("Redis 连接失败：%v", err)
	} else {
		log.Log.Info("Redis 连接成功")
	}
}

var ttl = config.GetRedisExpireSeconds("chat_message")

// SaveMessageToRedis stores a new chat message in Redis (as raw json)
func SaveMessageToRedis(roomID string, message []byte) error {
	msgKey := "room:" + roomID + ":messages"
	persistKey := "room:" + roomID + ":to_persist"

	log.Log.Debugf("保存消息到 Redis 房间 [%s]，设置 TTL: %ds", roomID, ttl)

	pipe := Rdb.Pipeline()

	pipe.LPush(ctx, msgKey, message)
	pipe.LTrim(ctx, msgKey, 0, 49)
	pipe.Expire(ctx, msgKey, time.Duration(ttl)*time.Second) // expire time
	// 同时写入待入库队列
	pipe.RPush(ctx, persistKey, message)
	pipe.SAdd(ctx, "rooms:active", roomID) // 注册活跃房间

	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Log.Errorf("Redis 执行 pipeline 操作失败（房间: %s）: %v", roomID, err)
	}
	return err
}

// GetRecentMessages returns the latest 50 messages from a room
func GetRecentMessages(roomID string) ([]string, error) {
	key := "room:" + roomID + ":messages"
	log.Log.Debugf("从 Redis 获取房间 [%s] 的最近消息", roomID)
	return Rdb.LRange(ctx, key, 0, 49).Result()
}
