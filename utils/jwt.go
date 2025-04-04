package utils

import (
	"errors"
	"github.com/golang-jwt/jwt/v5"
	"os"
	"time"
	log "websocket_server/logger"
)

var jwtSecret = []byte(os.Getenv("JWT_SECRET"))

func GenerateToken(username string) (string, error) {
	log.Log.Infof("Generate Token request: username=%s", username)
	claims := jwt.MapClaims{
		"username": username,
		"exp":      time.Now().Add(24 * time.Hour).Unix(), // Token expiration time: 24 hours.
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	log.Log.Infof("Token generarted successfully: username=%s", username)
	return token.SignedString(jwtSecret)
}

// parse Token, return username
func ParseToken(tokenString string) (string, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Verify the signature
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			log.Log.Warn("unexpected signing method")
			return nil, errors.New("unexpected signing method")
		}
		return jwtSecret, nil
	})

	if err != nil || !token.Valid {
		log.Log.Warnf("invalid token: %v", err)
		return "", errors.New("invalid token")
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		username, ok := claims["username"].(string)
		if !ok {
			log.Log.Warn("username not found in token")
			return "", errors.New("username not found in token")
		}
		log.Log.Infof("Token successfully parsed: username=%s", username)
		return username, nil
	}

	return "", errors.New("failed to parse token claims")
}
