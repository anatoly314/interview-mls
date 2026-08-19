package config

import (
	"os"
	"time"
)

type Config struct {
	PostgresDSN string

	// MaxRetries bounds attempt; LeaseDuration must outlast a normal job, and
	// PollInterval bounds how long a lost notification can delay a job.
	MaxRetries      int
	LeaseDuration   time.Duration
	PollInterval    time.Duration
	JanitorInterval time.Duration
	ParseDelay      time.Duration

	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket    string

	APIGRPCAddr string
}

func FromEnv() Config {
	return Config{
		PostgresDSN:     env("POSTGRES_DSN", "postgres://mls:mls@localhost:5432/mls?sslmode=disable"),
		MaxRetries:      3,
		LeaseDuration:   duration("LEASE_DURATION", 60*time.Second),
		PollInterval:    duration("POLL_INTERVAL", 5*time.Second),
		JanitorInterval: duration("JANITOR_INTERVAL", 10*time.Second),
		ParseDelay:      duration("PARSE_DELAY", 2*time.Second),
		MinioEndpoint:   env("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKey:  env("MINIO_ACCESS_KEY", "minioadmin"),
		MinioSecretKey:  env("MINIO_SECRET_KEY", "minioadmin"),
		MinioBucket:     env("MINIO_BUCKET", "jobs"),
		APIGRPCAddr:     env("API_GRPC_ADDR", "localhost:9090"),
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
