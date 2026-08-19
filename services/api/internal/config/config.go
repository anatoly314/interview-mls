package config

import "os"

type Config struct {
	HTTPAddr string
	GRPCAddr string

	PostgresDSN string

	KafkaBrokers string
	KafkaTopic   string

	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket    string
}

func FromEnv() Config {
	return Config{
		HTTPAddr:       env("HTTP_ADDR", ":8080"),
		GRPCAddr:       env("GRPC_ADDR", ":9090"),
		PostgresDSN:    env("POSTGRES_DSN", "postgres://mls:mls@localhost:5432/mls?sslmode=disable"),
		KafkaBrokers:   env("KAFKA_BROKERS", "localhost:19092"),
		KafkaTopic:     env("KAFKA_TOPIC", "jobs"),
		MinioEndpoint:  env("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKey: env("MINIO_ACCESS_KEY", "minioadmin"),
		MinioSecretKey: env("MINIO_SECRET_KEY", "minioadmin"),
		MinioBucket:    env("MINIO_BUCKET", "jobs"),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
