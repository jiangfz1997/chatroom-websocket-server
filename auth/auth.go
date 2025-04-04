package auth

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"time"
	"websocket_server/dynamodb"
	log "websocket_server/logger"
	"websocket_server/redis"
	"websocket_server/utils"
)

type ChatRoom struct {
	RoomID string   `dynamodbav:"room_id"`
	Users  []string `dynamodbav:"users"`
}

var chatroom ChatRoom

func ValidateRedisToken(token string) (string, error) {
	username, err := utils.ParseToken(token)
	if err != nil {
		log.Log.Warnf("Token parsing failed: %v", err)
		return "", errors.New("invalid or expired token")
	}

	// Check if the token exists in Redis
	redisKey := "token:" + token
	val, err := redis.Rdb.Get(context.Background(), redisKey).Result()
	if err != nil {
		log.Log.Warnf("Redis token lookup failed: %v", err)
		return "", errors.New("token not found or expired")
	}
	if val != username {
		log.Log.Warnf("Username mismatch: token username=%s, redis value=%s", username, val)
		return "", errors.New("token-user mismatch")
	}

	redis.Rdb.Expire(context.Background(), redisKey, 24*time.Hour)

	log.Log.Infof("Token validated successfully: %s", username)
	return username, nil
}

func IsUserInRoom(roomID, username string) (bool, error) {
	log.Log.Infof("Checking if user [%s] is in room [%s] via DynamoDB", username, roomID)

	input := &ddb.GetItemInput{
		TableName: aws.String("chatrooms"),
		Key: map[string]types.AttributeValue{
			"room_id": &types.AttributeValueMemberS{Value: roomID},
		},
		ProjectionExpression: aws.String("#users"),
		ExpressionAttributeNames: map[string]string{
			"#users": "users",
		},
	}

	resp, err := dynamodb.DB.GetItem(context.TODO(), input)
	if err != nil {
		log.Log.Errorf("DynamoDB GetItem error: %v", err)
		return false, err
	}

	if resp.Item == nil {
		log.Log.Warnf("Room [%s] not found in DynamoDB", roomID)
		return false, nil
	}

	err = attributevalue.UnmarshalMap(resp.Item, &chatroom)
	if err != nil {
		log.Log.Errorf("Unmarshal DynamoDB room item failed: %v", err)
		return false, err
	}

	for _, user := range chatroom.Users {
		log.Log.Infof("Checking user [%s] in room [%s]", user, roomID)
		if user == username {
			log.Log.Infof("User [%s] IS in room [%s]", username, roomID)
			return true, nil
		}
	}

	log.Log.Infof("User [%s] is NOT in room [%s]", username, roomID)
	return false, nil

}
