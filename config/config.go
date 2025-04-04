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
		log.Fatalf("Load base.json failed: %v", err)
	}

	loadAdditionalConfig("redis")

	loadAdditionalConfig("dynamodb")

	fmt.Println("All configurations loaded successfully")
}

func loadAdditionalConfig(name string) {
	subViper := viper.New()
	subViper.SetConfigName(name)
	subViper.SetConfigType("json")
	subViper.AddConfigPath("./config/")

	if err := subViper.ReadInConfig(); err != nil {
		log.Printf("Skip %s.json: %v", name, err)
		return
	}

	err := viper.MergeConfigMap(subViper.AllSettings())
	if err != nil {
		log.Printf("Merge %s.json failed: %v", name, err)
	}
}
