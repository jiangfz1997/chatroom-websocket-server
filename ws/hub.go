package ws

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"log"
	"sync"
)

// 客户端结构
type Client struct {
	Conn     *websocket.Conn
	Username string
	RoomID   string
	Send     chan []byte
	Hub      *Hub
}

// 房间结构
type Room struct {
	ID      string
	Clients map[*Client]bool
	Lock    sync.Mutex
}

// 中心 Hub：管理所有房间
type Hub struct {
	Rooms    map[string]*Room
	Lock     sync.Mutex
	ServerID string
}

func NewHub() *Hub {
	return &Hub{
		Rooms:    make(map[string]*Room),
		ServerID: uuid.New().String(), // ✅ 自動生成唯一 Server ID
	}
}

var GlobalHub = NewHub()

// 添加客户端到房间
func (h *Hub) JoinRoom(roomID string, client *Client) {
	h.Lock.Lock()
	room, exists := h.Rooms[roomID]
	if !exists {
		room = &Room{
			ID:      roomID,
			Clients: make(map[*Client]bool),
		}
		h.Rooms[roomID] = room
	}
	h.Lock.Unlock()

	room.Lock.Lock()
	room.Clients[client] = true
	room.Lock.Unlock()
}

// 从房间移除客户端
func (h *Hub) LeaveRoom(roomID string, client *Client) {
	h.Lock.Lock()
	room, exists := h.Rooms[roomID]
	h.Lock.Unlock()
	if !exists {
		return
	}
	room.Lock.Lock()
	delete(room.Clients, client)
	room.Lock.Unlock()
}

// 向房间广播消息
func (h *Hub) Broadcast(roomID string, message []byte) {

	h.Lock.Lock()
	room, exists := h.Rooms[roomID]
	h.Lock.Unlock()
	if !exists {
		return
	}
	if err := SaveMessageToRedis(roomID, message); err != nil {
		log.Printf("❌ 保存 Redis 消息失败: %v", err)
	}

	room.Lock.Lock()
	defer room.Lock.Unlock()

	for client := range room.Clients {
		select {
		case client.Send <- message:
		default:
			close(client.Send)
			delete(room.Clients, client)
		}
	}
}

func (h *Hub) BroadcastFromKafka(kafkaMsg *sarama.ConsumerMessage) {
	log.Printf("🔁 Kafka 消息同步")

	var senderServerID string
	for _, header := range kafkaMsg.Headers {
		if string(header.Key) == "serverID" {
			senderServerID = string(header.Value)
			break
		}
	}
	if senderServerID == h.ServerID {
		log.Printf("🔁 Kafka 消息來自自己的服務器，忽略")
		return
	}

	var parsed struct {
		RoomID string `json:"roomId"`
	}
	log.Printf("🔁 Kafka 消息同步來自 %s，轉發到房間 %s", senderServerID, parsed.RoomID)

	_ = json.Unmarshal(kafkaMsg.Value, &parsed)
	log.Printf("🔁 Kafka 消息解析成功，房間 ID: %s", parsed.RoomID)
	h.Broadcast(parsed.RoomID, kafkaMsg.Value)
}
