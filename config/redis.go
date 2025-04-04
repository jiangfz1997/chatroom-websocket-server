package config

import (
	"github.com/spf13/viper"
)

// RedisConf Redis connection configuration
type RedisConf struct {
	Host     string
	Port     int
	Password string
	DB       int
	Timeout  int
}

func GetRedisConfig() RedisConf {
	return RedisConf{
		Host:     viper.GetString("connection.addr"),
		Port:     viper.GetInt("connection.port"),
		Password: viper.GetString("connection.password"),
		DB:       viper.GetInt("connection.db"),
		Timeout:  viper.GetInt("connection.timeout"),
	}
}

func GetRedisExpireSeconds(keyType string) int {
	key := "key_expirations." + keyType
	if viper.IsSet(key) {
		return viper.GetInt(key)
	}
	return 120
}
