package ws

import (
	"net/http"
	"websocket_server/auth"
	"websocket_server/redis"

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
	token := c.Query("token")
	username, err := auth.ValidateRedisToken(token)
	if err != nil {
		log.Log.Warnf("WebSocket token auth failed: %v", err)
		c.Status(http.StatusUnauthorized)
		return
	}
	ok, err := auth.IsUserInRoom(roomID, username)
	if err != nil {
		log.Log.Errorf("Failed to check user's room!（user: %s, room: %s）: %v", username, roomID, err)
		c.Status(http.StatusInternalServerError)
		return
	}
	if !ok {
		log.Log.Warnf("User [%s] is not in room [%s]，refuse connection", username, roomID)
		c.Status(http.StatusForbidden)
		return
	} else {
		redisKey := "token:" + token
		err := redis.SetUserToken(redisKey, username)
		if err != nil {
			log.Log.Errorf("Redis token set failed: %v", err)
		}
	}

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

	recent, err := redis.GetRecentMessages(roomID)
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
