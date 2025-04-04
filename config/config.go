package config

import (
	"fmt"
	"log"

	"github.com/spf13/viper"
)

func InitConfig() {
	viper.SetConfigType("json")

	viper.SetConfigName("base")
	viper.AddConfigPath("./config/")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("加载 base.json 失败: %v", err)
	}

	// 合并 redis.json
	loadAdditionalConfig("redis")

	// 合并 dynamodb.json
	loadAdditionalConfig("dynamodb")

	// 合并当前环境配置（比如 local.json 或 prod.json）
	//loadAdditionalConfig("local")

	fmt.Println("所有配置文件加载成功")
}

func loadAdditionalConfig(name string) {
	subViper := viper.New()
	subViper.SetConfigName(name)
	subViper.SetConfigType("json")
	subViper.AddConfigPath("./config/")

	if err := subViper.ReadInConfig(); err != nil {
		log.Printf("跳过 %s.json: %v", name, err)
		return
	}

	err := viper.MergeConfigMap(subViper.AllSettings())
	if err != nil {
		log.Printf("合并 %s.json 失败: %v", name, err)
	}
}
