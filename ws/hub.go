package ws

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
	"sync"
	log "websocket_server/logger"
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
		ServerID: "",
	}
}

var GlobalHub = NewHub()

func (h *Hub) JoinRoom(roomID string, client *Client) {
	h.Lock.Lock()
	room, exists := h.Rooms[roomID]
	if !exists {
		room = &Room{
			ID:      roomID,
			Clients: make(map[*Client]bool),
		}
		h.Rooms[roomID] = room
		log.Log.Infof("新房间 [%s] 已创建", roomID)
	}
	h.Lock.Unlock()

	room.Lock.Lock()
	room.Clients[client] = true
	room.Lock.Unlock()

	log.Log.Infof("用户 [%s] 加入房间 [%s]", client.Username, roomID)
}

// 从房间移除客户端
func (h *Hub) LeaveRoom(roomID string, client *Client) {
	h.Lock.Lock()
	room, exists := h.Rooms[roomID]
	h.Lock.Unlock()
	if !exists {
		log.Log.Warnf("无法从不存在的房间 [%s] 移除用户 [%s]", roomID, client.Username)
		return
	}
	room.Lock.Lock()
	delete(room.Clients, client)
	room.Lock.Unlock()

	log.Log.Infof("用户 [%s] 离开房间 [%s]", client.Username, roomID)
}

// 向房间广播消息
func (h *Hub) Broadcast(roomID string, message []byte) {

	h.Lock.Lock()
	room, exists := h.Rooms[roomID]
	h.Lock.Unlock()
	if !exists {
		log.Log.Warnf("广播失败：房间 [%s] 不存在", roomID)
		return
	}
	if err := SaveMessageToRedis(roomID, message); err != nil {
		log.Log.Errorf("保存 Redis 消息失败（房间: %s）: %v", roomID, err)
	} else {
		log.Log.Debugf("Redis 成功缓存房间 [%s] 的消息", roomID)
	}

	room.Lock.Lock()
	defer room.Lock.Unlock()

	for client := range room.Clients {
		select {
		case client.Send <- message:
			log.Log.Debugf("向用户 [%s] 推送消息成功（房间: %s）", client.Username, roomID)
		default:
			close(client.Send)
			delete(room.Clients, client)
			log.Log.Warnf("用户 [%s] 推送失败，连接被移除（房间: %s）", client.Username, roomID)
		}
	}
}

func (h *Hub) BroadcastFromKafka(kafkaMsg *sarama.ConsumerMessage) {
	log.Log.Debug("Kafka 消息同步息")

	var senderServerID string
	for _, header := range kafkaMsg.Headers {
		if string(header.Key) == "serverID" {
			senderServerID = string(header.Value)
			break
		}
	}
	if senderServerID == h.ServerID {
		log.Log.Debugf("🔁 Kafka 消息来自当前服务器 [%s]，忽略", h.ServerID)
		return
	}

	var parsed struct {
		RoomID string `json:"roomId"`
	}
	log.Log.Infof("🔁 Kafka 消息同步來自 %s，轉發到房間 %s", senderServerID, parsed.RoomID)

	_ = json.Unmarshal(kafkaMsg.Value, &parsed)
	log.Log.Infof("🔁 Kafka 消息解析成功，房間 ID: %s", parsed.RoomID)
	h.Broadcast(parsed.RoomID, kafkaMsg.Value)
}
