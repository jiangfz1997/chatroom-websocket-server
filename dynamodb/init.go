package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"os"
	log "websocket_server/logger"
)

var DB *ddb.Client

func InitDB() {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT") // 本地模式會設這個
	region := os.Getenv("DYNAMODB_REGION")
	if region == "" {
		region = "us-west-2" // fallback
		log.Log.Warn("⚠️ DYNAMODB_REGION 未设置，默认使用 us-west-2")
	} else {
		log.Log.Infof("🌐 DYNAMODB_REGION = %s", region)
	}
	var cfg aws.Config
	var err error

	if endpoint != "" {
		log.Log.Info("🧪 连接本地 DynamoDB (local mode)")
		log.Log.Infof("🔌 使用 endpoint: %s", endpoint)

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
			log.Log.Fatalf("Failed to load DynamoDB(local) config:", err)
		}

	} else {
		log.Log.Infof("Connecting AWS DynamoDB")
		// 加载默认配置，依赖环境变量或 IAM 角色
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
		)
		if err != nil {
			log.Log.Fatalf("Failed to load aws config: ", err)
		}
	}

	// 创建 DynamoDB 客户端
	DB = ddb.NewFromConfig(cfg)
	log.Log.Infof("🔗 DynamoDB 客户端初始化成功")

	// 可选：列出当前表名，确认连接成功
	resp, err := DB.ListTables(context.TODO(), &ddb.ListTablesInput{})
	if err != nil {
		log.Log.Errorf("⚠️ 无法列出表，连接可能有误: %v", err)
	} else {
		log.Log.Infof("📋 当前 DynamoDB 表: %v", resp.TableNames)
	}
}
