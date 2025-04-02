package ws

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gorilla/websocket"
	"log"
	"time"
	"websocket_server/dynamodb"
	"websocket_server/kafka"
)

// 心跳超时设置
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
			log.Println("读消息错误:", err)
			break
		}

		c.HandleMessage(message)
		//var incoming map[string]string
		//if err := json.Unmarshal(message, &incoming); err != nil {
		//	log.Println("解析前端消息失败:", err)
		//	continue
		//}
		//
		//msg := map[string]string{
		//	"sender": c.Username,
		//	"text":   incoming["text"],
		//}
		//jsonMsg, err := json.Marshal(msg)
		//if err != nil {
		//	log.Println("JSON 编码失败:", err)
		//	continue
		//}
		//
		//c.Hub.Broadcast(c.RoomID, jsonMsg)
	}
}

// 将消息发送到客户端
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// channel 关闭
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			err := c.Conn.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Println("写消息错误:", err)
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
		log.Println("⚠️ 无法解析消息类型:", err)
		return
	}

	switch base.Type {
	case "fetch_history":
		c.handleFetchHistory(msg)
	case "message":
		c.handleBroadcastMessage(msg)
	default:
		log.Println("⚠️ 未知消息类型:", base.Type)
	}
}

func (c *Client) handleBroadcastMessage(msg []byte) {
	var incoming struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(msg, &incoming); err != nil {
		log.Println("⚠️ 文本消息解析失败:", err)
		return
	}

	out := map[string]string{
		"type":   "message",
		"sender": c.Username,
		"text":   incoming.Text,
		"roomID": c.RoomID,
		"sentAt": time.Now().UTC().Format(time.RFC3339Nano),
	}
	log.Printf("📥 WebSocket 收到來自用戶 %s 的消息，將轉發給本地房間並推送 Kafka", c.Username)

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
		log.Printf("⚠️ Kafka 發送失敗: %v", err)
	}
}

func (c *Client) handleFetchHistory(msg []byte) {
	// Step 1: 解析请求
	var req struct {
		Type   string `json:"type"`
		RoomID string `json:"roomID"`
		Before string `json:"before"`
		Limit  int    `json:"limit"`
	}
	if err := json.Unmarshal(msg, &req); err != nil {
		log.Println("⚠️ fetch_history 消息解析失败:", err)
		return
	}

	// Step 2: 解析时间戳
	beforeTime := time.Now().UTC() // 默认当前时间
	if req.Before != "" {
		parsedTime, err := time.Parse(time.RFC3339Nano, req.Before)
		if err != nil {
			log.Printf("⚠️ 时间戳格式错误: %v", err)
			return
		}
		beforeTime = parsedTime
	}

	// Step 3: 拉取历史消息
	messages, err := getMessagesFromDynamo(req.RoomID, beforeTime.Format(time.RFC3339Nano), req.Limit)
	if err != nil {
		log.Printf("⚠️ 获取 DynamoDB 历史消息失败: %v", err)
		return
	}

	// Step 4: 取出最后一条消息时间（用于前端翻页）
	lastTime := ""
	if len(messages) > 0 {
		lastTime = messages[len(messages)-1].Timestamp
	}

	// Step 5: 构造响应
	resp := map[string]interface{}{
		"type":            "history_result",
		"roomID":          req.RoomID,
		"messages":        messages, // 👈 结构体数组，前端能直接读取 msg.text
		"hasMore":         len(messages) == req.Limit,
		"lastMessageTime": lastTime,
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Println("⚠️ JSON 编码失败:", err)
		return
	}

	// Step 6: 推送给当前客户端
	c.Send <- respBytes
}

func getMessagesFromDynamo(roomID string, beforeTime string, limit int) ([]dynamodb.Message, error) {
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
		log.Printf("⚠️ DynamoDB 查询失败: %v", err)
		return nil, err
	}

	var result []dynamodb.Message
	for _, item := range resp.Items {
		var msg dynamodb.Message
		if err := attributevalue.UnmarshalMap(item, &msg); err != nil {
			log.Printf("⚠️ 解码消息失败: %v", err)
			continue
		}
		result = append(result, msg)
	}
	log.Printf("Get MessagesFromDynamo: %d 条消息", len(result))
	return result, nil
}
