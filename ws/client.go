package ws

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gorilla/websocket"
	"time"
	"websocket_server/dynamodb"
	"websocket_server/kafka"
	log "websocket_server/logger"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

func (c *Client) ReadPump() {
	defer func() {
		c.Hub.LeaveRoom(c.RoomID, c)
		c.Conn.Close()
		log.Log.Infof("User [%s] connection close，leaving room [%s]", c.Username, c.RoomID)
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			log.Log.Warnf("Read message error: %v", err)
			break
		}
		log.Log.Debugf("Receive user [%s] original message: %s", c.Username, string(message))
		c.HandleMessage(message)
	}
}

// Send message to the client
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
		log.Log.Infof("Close user [%s] write connection", c.Username)
	}()

	for {
		select {
		case msg, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			err := c.Conn.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Log.Warnf("Write message err:", err)
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) HandleMessage(msg []byte) {
	var base struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(msg, &base); err != nil {
		log.Log.Warnf("Cannot parse message: %v", err)
		return
	}

	switch base.Type {
	case "fetch_history":
		c.handleFetchHistory(msg)
	case "message":
		c.handleBroadcastMessage(msg)
	default:
		log.Log.Warnf("Unknow message type: %v", base.Type)
	}
}

func (c *Client) handleBroadcastMessage(msg []byte) {
	var incoming struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(msg, &incoming); err != nil {
		log.Log.Errorf("Failed to parse text message: %v", err)
		return
	}

	out := map[string]string{
		"type":   "message",
		"sender": c.Username,
		"text":   incoming.Text,
		"roomID": c.RoomID,
		"sentAt": time.Now().UTC().Format(time.RFC3339Nano),
	}
	log.Log.Infof("Received message from user %s via WebSocket. Forwarding to local room and pushing to Kafka.", c.Username)

	jsonMsg, _ := json.Marshal(out)

	c.Hub.Broadcast(c.RoomID, jsonMsg)
	kafkaMsg := &sarama.ProducerMessage{
		Topic: "chat_messages",
		Key:   sarama.StringEncoder(c.RoomID),
		Value: sarama.ByteEncoder(jsonMsg),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("serverID"),
				Value: []byte(c.Hub.ServerID),
			},
		},
	}
	_, _, err := kafka.Producer.SendMessage(kafkaMsg)
	if err != nil {
		log.Log.Errorf("Kafka send failed: %v", err)
	}
	//if err := SaveMessageToRedis(c.RoomID, jsonMsg); err != nil {
	//	log.Log.Errorf("Save Redis message failed（room: %s）: %v", c.RoomID, err)
	//} else {
	//	log.Log.Debugf("Redis saved message from [%s]", c.RoomID)
	//}

}

func (c *Client) handleFetchHistory(msg []byte) {
	var req struct {
		Type   string `json:"type"`
		RoomID string `json:"roomID"`
		Before string `json:"before"`
		Limit  int    `json:"limit"`
	}
	if err := json.Unmarshal(msg, &req); err != nil {
		log.Log.Errorf("fetch_history message parse failed %v:", err)
		return
	}

	beforeTime := time.Now().UTC()
	if req.Before != "" {
		parsedTime, err := time.Parse(time.RFC3339Nano, req.Before)
		if err != nil {
			log.Log.Errorf("Timestamp format error: %v", err)
			return
		}
		beforeTime = parsedTime
	}

	messages, err := getMessagesFromDynamo(req.RoomID, beforeTime.Format(time.RFC3339Nano), req.Limit)
	if err != nil {
		log.Log.Errorf("Failed to fetch historical messages from DynamoDB: %v", err)
		return
	}

	lastTime := ""
	if len(messages) > 0 {
		lastTime = messages[len(messages)-1].Timestamp
	}

	resp := map[string]interface{}{
		"type":            "history_result",
		"roomID":          req.RoomID,
		"messages":        messages,
		"hasMore":         len(messages) == req.Limit,
		"lastMessageTime": lastTime,
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Log.Errorf("JSON encoding failed:", err)
		return
	}

	c.Send <- respBytes
}

func getMessagesFromDynamo(roomID string, beforeTime string, limit int) ([]dynamodb.Message, error) {
	log.Log.Infof("Preparing to fetch messages from DynamoDB | Table: messages | RoomID: %s | Before: %s | Limit: %d", roomID, beforeTime, limit)
	input := &ddb.QueryInput{
		TableName: aws.String("messages"),
		KeyConditions: map[string]types.Condition{
			"room_id": {
				ComparisonOperator: types.ComparisonOperatorEq,
				AttributeValueList: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: roomID},
				},
			},
			"timestamp": {
				ComparisonOperator: types.ComparisonOperatorLt,
				AttributeValueList: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: beforeTime},
				},
			},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	}

	resp, err := dynamodb.DB.Query(context.TODO(), input)
	if err != nil {
		log.Log.Errorf("DynamoDB lookup falied: %v", err)
		return nil, err
	}

	var result []dynamodb.Message
	for _, item := range resp.Items {
		var msg dynamodb.Message
		if err := attributevalue.UnmarshalMap(item, &msg); err != nil {
			log.Log.Errorf("Decode message failed: %v", err)
			continue
		}
		result = append(result, msg)
	}
	log.Log.Info("Get MessagesFromDynamo: %d messages", len(result))
	return result, nil
}
