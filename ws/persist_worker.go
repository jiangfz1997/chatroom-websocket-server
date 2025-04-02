package ws

import (
	//"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
	"websocket_server/dynamodb"
)

var persistTickerInterval = 10 * time.Second

// 启动 Redis -> DB 的持久化后台任务
func StartRedisToDBSyncLoop() {
	ticker := time.NewTicker(persistTickerInterval)
	log.Println("🌀 持久化任务启动，每", persistTickerInterval)
	for range ticker.C {
		//log.Println("🔁 正在同步 Redis 数据")
		syncAllRooms()
	}
}

// 扫描所有房间的 to_persist 队列（你可以改为从 Redis 获取活跃房间）
func syncAllRooms() {
	roomIDs := getAllRoomIDs() // 暂时可以硬编码测试
	for _, roomID := range roomIDs {
		syncRoomMessages(roomID)
	}
}

func syncRoomMessages(roomID string) {
	key := "room:" + roomID + ":to_persist"
	for i := 0; i < 100; i++ { // 最多拉取 100 条，防止过载
		msg, err := Rdb.LPop(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			//log.Printf("💤 没有消息可同步: %s", roomID)
			break
		}
		if err != nil {
			log.Printf("❌ Redis LPOP 出错: %v", err)
			break
		}

		saveToDatabase(roomID, msg)
	}
}

func saveToDatabase(roomID string, rawMsg string) {
	var data struct {
		Sender string `json:"sender"`
		Text   string `json:"text"`
	}
	if err := json.Unmarshal([]byte(rawMsg), &data); err != nil {
		log.Println("⚠️ JSON 解析失败:", err)
		return
	}

	//msg := dynamodb.Message{
	//	RoomID:    roomID,
	//	Sender:    data.Sender,
	//	Text:      data.Text,
	//	Timestamp: time.Now().Format(time.RFC3339),
	//}
	msg := dynamodb.NewMessage(roomID, data.Sender, data.Text)

	log.Printf("💾 正在写入 DynamoDB: room=%s sender=%s", roomID, data.Sender)
	if err := dynamodb.SaveMessage(msg); err != nil {
		log.Printf("❌ DynamoDB 存储失败: %v", err)
	} else {
		log.Printf("✅ 成功写入 DynamoDB: [%s] %s", data.Sender, data.Text)
	}
}

// 获取你希望处理的房间 ID（可以用 Redis 的 keys 或硬编码测试）
func getAllRoomIDs() []string {
	roomIDs, err := Rdb.SMembers(ctx, "rooms:active").Result()
	if err != nil {
		log.Printf("❌ 无法获取活跃房间列表: %v", err)
		return []string{}
	}
	return roomIDs
}
