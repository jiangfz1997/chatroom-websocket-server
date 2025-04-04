package ws

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
	"sync"
	log "websocket_server/logger"
	"websocket_server/redis"
)

type Client struct {
	Conn     *websocket.Conn
	Username string
	RoomID   string
	Send     chan []byte
	Hub      *Hub
}

type Room struct {
	ID      string
	Clients map[*Client]bool
	Lock    sync.Mutex
}

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
		log.Log.Infof("New room [%s] created", roomID)
	}
	h.Lock.Unlock()

	room.Lock.Lock()
	room.Clients[client] = true
	room.Lock.Unlock()

	log.Log.Infof("User [%s] entered room [%s]", client.Username, roomID)
}

func (h *Hub) LeaveRoom(roomID string, client *Client) {
	h.Lock.Lock()
	room, exists := h.Rooms[roomID]
	h.Lock.Unlock()
	if !exists {
		log.Log.Warnf("Cannot remove user [%s] from non-existent room [%s]", client.Username, roomID)
		return
	}
	room.Lock.Lock()
	delete(room.Clients, client)
	room.Lock.Unlock()

	log.Log.Infof("User [%s] exit room [%s]", client.Username, roomID)
}

func (h *Hub) Broadcast(roomID string, message []byte) {

	h.Lock.Lock()
	room, exists := h.Rooms[roomID]
	h.Lock.Unlock()
	if !exists {
		log.Log.Warnf("Broadcast failed：room [%s] not exist", roomID)
		return
	}
	//if err := SaveMessageToRedis(roomID, message); err != nil {
	//	log.Log.Errorf("Save Redis message failed（room: %s）: %v", roomID, err)
	//} else {
	//	log.Log.Debugf("Redis saved message from [%s]", roomID)
	//}

	room.Lock.Lock()
	defer room.Lock.Unlock()

	for client := range room.Clients {
		select {
		case client.Send <- message:
			log.Log.Debugf("Successfully pushed message to user [%s] (Room: %s)", client.Username, roomID)
		default:
			close(client.Send)
			delete(room.Clients, client)
			log.Log.Warnf("Failed to push message to user [%s]; connection removed (Room: %s)", client.Username, roomID)
		}
	}
}

func (h *Hub) BroadcastFromKafka(kafkaMsg *sarama.ConsumerMessage) {
	log.Log.Debug("Kafka message received, processing...")
	var senderServerID string
	for _, header := range kafkaMsg.Headers {
		if string(header.Key) == "serverID" {
			senderServerID = string(header.Value)
			break
		}
	}
	log.Log.Infof("Kafka message received, processing... %v", kafkaMsg.Value)

	var parsed struct {
		RoomID    string `json:"roomID"`
		TimeStamp string `json:"sentAt"`
	}
	_ = json.Unmarshal(kafkaMsg.Value, &parsed)
	log.Log.Infof("Kafka message parsed successfully, Room ID: %s", parsed.RoomID)
	log.Log.Infof("Kafka message sync from %s, forwarding to room %s", senderServerID, parsed.RoomID)

	if err := redis.SaveMessageToRedis(parsed.RoomID, parsed.TimeStamp, kafkaMsg.Value); err != nil {
		log.Log.Errorf("Save Redis message failed（room: %s）: %v", parsed.RoomID, err)
	} else {
		log.Log.Infof("Redis saved message from [%s]", parsed.RoomID)
	}

	if senderServerID == h.ServerID {
		log.Log.Debugf("Kafka message from current service [%s]，pass", h.ServerID)
		return
	}

	h.Broadcast(parsed.RoomID, kafkaMsg.Value)

}
