package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/anatolyt/interview-mls/services/worker/internal/config"
	"github.com/anatolyt/interview-mls/services/worker/internal/events"
	"github.com/anatolyt/interview-mls/services/worker/internal/processor"
	"github.com/anatolyt/interview-mls/services/worker/internal/store"
)

type App struct {
	cfg  config.Config
	log  *slog.Logger
	db   *pgxpool.Pool
	s3   *minio.Client
	conn *grpc.ClientConn
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

	// The hostname is the worker id: it is stable for the life of the process
	// (which is all the api's routing table needs) and unique per container.
	workerID, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("hostname: %w", err)
	}

	a.log.Info("worker ready", "worker_id", workerID, "lease", a.cfg.LeaseDuration,
		"poll", a.cfg.PollInterval, "max_attempts", a.cfg.MaxRetries)

	st := store.New(a.db, a.cfg.LeaseDuration, a.cfg.MaxRetries)

	// The processor and the stream client are mutually referential: events go
	// up through the client, cancel commands come back down into the processor.
	// The client is built around a callback so the cycle stays one-directional
	// at construction time.
	var proc *processor.Processor
	ev := events.NewClient(a.log, a.conn, workerID, func(jobID string) { proc.Cancel(jobID) })
	proc = processor.New(a.log, st, a.s3, a.cfg.MinioBucket, ev,
		a.cfg.MaxRetries, a.cfg.ParseDelay)
	go ev.Run(ctx)

	a.run(ctx, st, proc)

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

	conn, err := grpc.NewClient(a.cfg.APIGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc client: %w", err)
	}
	a.conn = conn

	return nil
}

func (a *App) close() {
	if a.db != nil {
		a.db.Close()
	}
	if a.conn != nil {
		_ = a.conn.Close()
	}
}
