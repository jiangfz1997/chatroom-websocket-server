package ws

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func ServeWs(c *gin.Context) {
	roomID := c.Param("roomId")
	username := c.Query("username")

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("WebSocket 升级失败:", err)
		return
	}

	client := &Client{
		Conn:     conn,
		Username: username,
		RoomID:   roomID,
		Send:     make(chan []byte, 256),
		Hub:      GlobalHub,
	}

	GlobalHub.JoinRoom(roomID, client)
	log.Printf("✅ 用户 %s 加入房间 %s", username, roomID)

	go client.WritePump()

	// ✅ 从 Redis 拉历史消息并发送
	recent, err := GetRecentMessages(roomID)
	if err != nil {
		log.Printf("⚠️ 无法读取 Redis 消息缓存: %v", err)
	} else {
		for i := len(recent) - 1; i >= 0; i-- {
			client.Send <- []byte(recent[i])
		}
	}

	go client.ReadPump()
}
