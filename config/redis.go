package config

import (
	"github.com/spf13/viper"
)

// RedisConf 结构体，用于连接信息
type RedisConf struct {
	Host     string // 拆开用 host 和 port 更语义化
	Port     int
	Password string
	DB       int
	Timeout  int
}

// GetRedisConfig 获取 Redis 连接信息
func GetRedisConfig() RedisConf {
	return RedisConf{
		Host:     viper.GetString("connection.addr"),
		Port:     viper.GetInt("connection.port"),
		Password: viper.GetString("connection.password"),
		DB:       viper.GetInt("connection.db"),
		Timeout:  viper.GetInt("connection.timeout"),
	}
}

// GetRedisExpireSeconds 获取某类 Redis key 的过期秒数（默认 120 秒）
func GetRedisExpireSeconds(keyType string) int {
	key := "key_expirations." + keyType
	if viper.IsSet(key) {
		return viper.GetInt(key)
	}
	return 120
}
