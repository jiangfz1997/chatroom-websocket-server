package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"time"
)

type Message struct {
	RoomID    string `json:"room_id" dynamodbav:"room_id"`
	Timestamp string `json:"timestamp" dynamodbav:"timestamp"`
	Sender    string `json:"sender" dynamodbav:"sender"`
	Text      string `json:"text" dynamodbav:"text"`
}

func NewMessage(roomID, sender, text string) Message {
	return Message{
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
