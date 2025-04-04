package redis

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
		addr = "redis:6379"
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
		log.Log.Fatalf("Redis connect failed：%v", err)
	} else {
		log.Log.Info("Redis connect successfully")
	}
}

var ttl = config.GetRedisExpireSeconds("chat_message")

// SaveMessageToRedis stores a new chat message in Redis (as raw json)
func SaveMessageToRedis(roomID string, timestamp string, message []byte) error {
	dedupKey := "dedup:room:" + roomID

	ok, err := Rdb.SAdd(ctx, dedupKey, timestamp).Result()
	if err != nil {
		log.Log.Errorf("SADD failed: %v", err)
		return err
	}
	if ok == 0 {
		log.Log.Infof("Msg duplicated，skip write to Redis。room=%s timestamp=%d", roomID, timestamp)
		return nil
	}

	msgKey := "room:" + roomID + ":messages"
	persistKey := "room:" + roomID + ":to_persist"

	pipe := Rdb.Pipeline()
	pipe.LPush(ctx, msgKey, message)
	pipe.LTrim(ctx, msgKey, 0, 49)
	pipe.Expire(ctx, msgKey, time.Duration(ttl)*time.Second)
	pipe.RPush(ctx, persistKey, message)
	pipe.SAdd(ctx, "rooms:active", roomID)

	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Log.Errorf("Redis pipeline failed (room: %s): %v", roomID, err)
	}
	return err
}

// GetRecentMessages returns the latest 50 messages from a room
func GetRecentMessages(roomID string) ([]string, error) {
	key := "room:" + roomID + ":messages"
	log.Log.Debugf("Get message of room [%s] from redis", roomID)
	return Rdb.LRange(ctx, key, 0, 49).Result()
}

func SetUserToken(token string, username string) error {
	key := "token:" + token
	err := Rdb.Set(context.Background(), key, username, 24*time.Hour).Err()
	if err != nil {
		log.Log.Errorf("Failed to set token in Redis: %v", err)
		return err
	}
	log.Log.Infof("Token saved to Redis: %s -> %s", key, username)
	return nil
}
