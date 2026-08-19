package config

import (
	"os"
	"time"
)

type Config struct {
	PostgresDSN string

	KafkaBrokers  string
	KafkaTopic    string
	KafkaGroup    string
	MaxRetries    int
	ParseDelay    time.Duration

	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket    string

	APIGRPCAddr string
}

func FromEnv() Config {
	return Config{
		PostgresDSN:    env("POSTGRES_DSN", "postgres://mls:mls@localhost:5432/mls?sslmode=disable"),
		KafkaBrokers:   env("KAFKA_BROKERS", "localhost:19092"),
		KafkaTopic:     env("KAFKA_TOPIC", "jobs"),
		KafkaGroup:     env("KAFKA_GROUP", "workers"),
		MaxRetries:     3,
		ParseDelay:     duration("PARSE_DELAY", 2*time.Second),
		MinioEndpoint:  env("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKey: env("MINIO_ACCESS_KEY", "minioadmin"),
		MinioSecretKey: env("MINIO_SECRET_KEY", "minioadmin"),
		MinioBucket:    env("MINIO_BUCKET", "jobs"),
		APIGRPCAddr:    env("API_GRPC_ADDR", "localhost:9090"),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func duration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
