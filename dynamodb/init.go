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
	endpoint := os.Getenv("DYNAMODB_ENDPOINT") // for local mode
	region := os.Getenv("DYNAMODB_REGION")
	if region == "" {
		region = "us-east-2"
		log.Log.Warn("DYNAMODB_REGION not set，using default us-east-2")
	} else {
		log.Log.Infof("DYNAMODB_REGION = %s", region)
	}
	var cfg aws.Config
	var err error

	if endpoint != "" {
		log.Log.Info("Connecting Local DynamoDB (local mode)")
		log.Log.Infof("Using endpoint: %s", endpoint)

		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if service == ddb.ServiceID {
				return aws.Endpoint{
					URL:           endpoint,
					SigningRegion: region,
				}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
		})

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
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
		)
		if err != nil {
			log.Log.Fatalf("Failed to load aws config: ", err)
		}
	}

	DB = ddb.NewFromConfig(cfg)
	log.Log.Infof("DynamoDB client created successfully")

	resp, err := DB.ListTables(context.TODO(), &ddb.ListTablesInput{})
	if err != nil {
		log.Log.Errorf("Cannot get table list, connection might be down: %v", err)
	} else {
		log.Log.Infof("Current DynamoDB table list: %v", resp.TableNames)
	}
}
