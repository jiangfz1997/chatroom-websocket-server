package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var DB *ddb.Client

func InitDB() {
	env := os.Getenv("DYNAMODB_ENV")
	if env == "" {
		env = "local" // 默认环境
	}

	region := "us-west-2"
	var cfg aws.Config
	var err error

	if env == "local" {
		endpoint := "http://localhost:8000" // 本地 DynamoDB
		log.Println("🌱 连接本地 DynamoDB (local mode)")

		// 设置本地模拟器的 endpoint
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if service == ddb.ServiceID {
				return aws.Endpoint{
					URL:           endpoint,
					SigningRegion: region,
				}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
		})

		// 加载配置，添加本地用的 dummy 凭证
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
			config.WithEndpointResolverWithOptions(customResolver),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "dummy")),
		)
		if err != nil {
			log.Fatal("❌ 加载本地 DynamoDB 配置失败:", err)
		}

	} else if env == "aws" {
		log.Println("🚀 连接 AWS DynamoDB（真实云服务）")
		// 加载默认配置，依赖环境变量或 IAM 角色
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
		)
		if err != nil {
			log.Fatal("❌ 加载 AWS 配置失败:", err)
		}
	} else {
		log.Fatalf("❌ 未知 DYNAMODB_ENV：%s", env)
	}

	// 创建 DynamoDB 客户端
	DB = ddb.NewFromConfig(cfg)
	log.Println("✅ 已连接到 DynamoDB")
}
