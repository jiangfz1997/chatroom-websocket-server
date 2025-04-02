package ws

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
	"websocket_server/config"
	//"time"
)

var Rdb *redis.Client
var ctx = context.Background()

func Init_redis() {

	// 连接 Redis
	conf := config.GetRedisConfig()
	Rdb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		Password: conf.Password,
		DB:       conf.DB,
	})
}

//var conf = config.GetRedisConfig()
//
//var ctx = context.Background()
//
//var Rdb = redis.NewClient(&redis.Options{
//	Addr:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
//	Password: conf.Password,
//	DB:       conf.DB,
//})

var ttl = config.GetRedisExpireSeconds("chat_message")

// SaveMessageToRedis stores a new chat message in Redis (as raw json)
func SaveMessageToRedis(roomID string, message []byte) error {
	msgKey := "room:" + roomID + ":messages"
	persistKey := "room:" + roomID + ":to_persist"

	pipe := Rdb.Pipeline()

	// ✅ 写入前端展示缓存
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
