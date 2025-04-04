package ws

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	log "websocket_server/logger"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func ServeWs(c *gin.Context) {
	roomID := c.Param("roomId")
	username := c.Query("username")

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Log.Errorf("WebSocket 升级失败（用户: %s，房间: %s）: %v", username, roomID, err)
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
	log.Log.Infof("用户 [%s] 加入房间 [%s]，连接建立成功", username, roomID)

	go client.WritePump()

	//从 Redis 拉历史消息并发送
	recent, err := GetRecentMessages(roomID)
	if err != nil {
		log.Log.Warnf("无法读取 Redis 消息缓存（房间: %s，用户: %s）: %v", roomID, username, err)
	} else {
		log.Log.Infof("从 Redis 读取 %d 条历史消息推送给用户 [%s]", len(recent), username)
		for i := len(recent) - 1; i >= 0; i-- {
			client.Send <- []byte(recent[i])
		}
	}

	go client.ReadPump()
}
