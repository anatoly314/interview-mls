package app

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobsv1 "github.com/anatolyt/interview-mls/proto/gen/jobsv1"
	"github.com/anatolyt/interview-mls/services/worker/internal/config"
)

type App struct {
	cfg    config.Config
	log    *slog.Logger
	db     *pgxpool.Pool
	s3     *minio.Client
	kafka  *kgo.Client
	events jobsv1.JobEventsClient
}

func New(cfg config.Config, log *slog.Logger) *App {
	return &App{cfg: cfg, log: log}
}

func (a *App) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	defer a.close()
	if err := a.connect(ctx); err != nil {
		return err
	}

	a.log.Info("worker ready", "group", a.cfg.KafkaGroup, "topic", a.cfg.KafkaTopic)

	<-ctx.Done()
	a.log.Info("shutting down")
	return nil
}

func (a *App) connect(ctx context.Context) error {
	db, err := pgxpool.New(ctx, a.cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("postgres pool: %w", err)
	}
	if err := db.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping: %w", err)
	}
	a.db = db

	s3, err := minio.New(a.cfg.MinioEndpoint, &minio.Options{
		Creds: credentials.NewStaticV4(a.cfg.MinioAccessKey, a.cfg.MinioSecretKey, ""),
	})
	if err != nil {
		return fmt.Errorf("minio client: %w", err)
	}
	a.s3 = s3

	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(a.cfg.KafkaBrokers),
		kgo.ConsumerGroup(a.cfg.KafkaGroup),
		kgo.ConsumeTopics(a.cfg.KafkaTopic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	if err := kafka.Ping(ctx); err != nil {
		return fmt.Errorf("kafka ping: %w", err)
	}
	a.kafka = kafka

	conn, err := grpc.NewClient(a.cfg.APIGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc client: %w", err)
	}
	a.events = jobsv1.NewJobEventsClient(conn)

	return nil
}

func (a *App) close() {
	if a.kafka != nil {
		a.kafka.Close()
	}
	if a.db != nil {
		a.db.Close()
	}
}
