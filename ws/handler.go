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
		log.Log.Errorf("WebSocket upgrade failed（user: %s，room: %s）: %v", username, roomID, err)
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
	log.Log.Infof("User [%s] entered room [%s]，conneciton established successfully", username, roomID)

	go client.WritePump()

	recent, err := GetRecentMessages(roomID)
	if err != nil {
		log.Log.Warnf("Cannot get message from Redis （room: %s，user: %s）: %v", roomID, username, err)
	} else {
		log.Log.Infof("Read %d historical messages from Redis and sent to user [%s]", len(recent), username)
		for i := len(recent) - 1; i >= 0; i-- {
			client.Send <- []byte(recent[i])
		}
	}

	go client.ReadPump()
}
