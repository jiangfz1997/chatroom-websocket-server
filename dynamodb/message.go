package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"time"
)

type Message struct {
	//ID        string `dynamodbav:"id"`
	RoomID    string `json:"room_id" dynamodbav:"room_id"`
	Timestamp string `json:"timestamp" dynamodbav:"timestamp"`
	Sender    string `json:"sender" dynamodbav:"sender"`
	Text      string `json:"text" dynamodbav:"text"`
}

func NewMessage(roomID, sender, text string) Message {
	return Message{
		//ID:        uuid.New().String(),
		RoomID:    roomID,
		Sender:    sender,
		Text:      text,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func SaveMessage(msg Message) error {
	item, err := attributevalue.MarshalMap(msg)
	if err != nil {
		return err
	}

	_, err = DB.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("messages"),
		Item:      item,
	})
	return err
}

//package dynamodb
//
//import "time"
//
//type Message struct {
//	ID        int       `gorm:"primaryKey" json:"id"`
//	RoomID    string    `gorm:"index" json:"room_id"`
//	Sender    string    `json:"sender"`
//	Text      string    `json:"text"`
//	Timestamp time.Time `gorm:"autoCreateTime" json:"timestamp"`
//}

//
//// 保存消息到数据库
//func SaveMessage(roomID, sender, text string) error {
//	log.Println("保存消息：", sender, "->", roomID, ":", text)
//	_, err := database.DB.Exec(`
//		INSERT INTO messages (room_id, sender, text, timestamp)
//		VALUES (?, ?, ?, ?)
//	`, roomID, sender, text, time.Now())
//	return err
//}
//
//// 查询历史消息（支持分页）
//func GetMessagesByRoom(roomID string, limit, offset int) ([]Message, error) {
//	rows, err := database.DB.Query(`
//		SELECT id, room_id, sender, text, timestamp
//		FROM messages
//		WHERE room_id = ?
//		ORDER BY timestamp DESC
//		LIMIT ? OFFSET ?
//	`, roomID, limit, offset)
//	if err != nil {
//		return nil, err
//	}
//	defer rows.Close()
//
//	var messages []Message
//	for rows.Next() {
//		var msg Message
//		err := rows.Scan(&msg.ID, &msg.RoomID, &msg.Sender, &msg.Text, &msg.Timestamp)
//		if err != nil {
//			continue
//		}
//		messages = append(messages, msg)
//	}
//	return messages, nil
//}
//
//// 获取某个用户加入前一小时起点的消息（加入时间应通过其他表来查询）
//func GetMessagesSince(roomID string, since time.Time, limit int) ([]Message, error) {
//	rows, err := database.DB.Query(`
//		SELECT id, room_id, sender, text, timestamp
//		FROM messages
//		WHERE room_id = ? AND timestamp >= ?
//		ORDER BY timestamp ASC
//		LIMIT ?
//	`, roomID, since, limit)
//	if err != nil {
//		return nil, err
//	}
//	defer rows.Close()
//
//	var messages []Message
//	for rows.Next() {
//		var msg Message
//		err := rows.Scan(&msg.ID, &msg.RoomID, &msg.Sender, &msg.Text, &msg.Timestamp)
//		if err != nil {
//			continue
//		}
//		messages = append(messages, msg)
//	}
//	return messages, nil
//}
//
//// 根据用户加入时间，向前推一小时作为最早可见消息时间，分页查询
//func GetMessagesWithJoinLimit(roomID, username, before string, limit int) ([]Message, error) {
//	// 查询用户加入聊天室时间
//	//var joinedAt string
//	var joinedAt time.Time // 此处有修改：改为 time.Time 类型
//	err := database.DB.QueryRow(`
//		SELECT joined_at FROM user_chatroom
//		JOIN users ON user_chatroom.user_id = users.id
//		WHERE user_chatroom.chatroom_id = ? AND users.username = ?
//	`, roomID, username).Scan(&joinedAt)
//	if err != nil {
//		return nil, err
//	}
//
//	// 无需再手动解析时间
//	joinedTime := joinedAt // 此处有修改
//
//	//joinedTime, err := time.Parse("2006-01-02 15:04:05", joinedAt)
//	//if err != nil {
//	//	return nil, err
//	//}
//
//	// 用户最多能看到“加入前1小时起”到当前为止的记录
//	earliest := joinedTime.Add(-1 * time.Hour)
//
//	query := `
//		SELECT id, room_id, sender, text, timestamp FROM messages
//		WHERE room_id = ? AND timestamp >= ?
//	`
//	args := []interface{}{roomID, earliest.Format("2006-01-02 15:04:05")}
//
//	if before != "" {
//		beforeTime, err := time.Parse(time.RFC3339, before)
//		if err == nil {
//			query += " AND timestamp < ?"
//			args = append(args, beforeTime.Format("2006-01-02 15:04:05"))
//		} else {
//			fmt.Println("before 参数解析失败:", err)
//		}
//	}
//
//	query += " ORDER BY timestamp DESC LIMIT ?"
//	args = append(args, limit)
//
//	raws, err := database.DB.Query(query, args...)
//	if err != nil {
//		fmt.Println("数据库查询失败:", err)
//		return nil, err
//	}
//	defer raws.Close()
//
//	var messages []Message
//	for raws.Next() {
//		var msg Message
//		err := raws.Scan(&msg.ID, &msg.RoomID, &msg.Sender, &msg.Text, &msg.Timestamp)
//		if err != nil {
//			continue
//		}
//		messages = append(messages, msg)
//	}
//	if messages == nil {
//		messages = []Message{} // 防止返回 null
//	}
//	return messages, nil
//}
