package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"

	jobsv1 "github.com/anatolyt/interview-mls/proto/gen/jobsv1"
	"github.com/anatolyt/interview-mls/services/api/internal/config"
	"github.com/anatolyt/interview-mls/services/api/internal/events"
	"github.com/anatolyt/interview-mls/services/api/internal/httpapi"
	"github.com/anatolyt/interview-mls/services/api/internal/hub"
	"github.com/anatolyt/interview-mls/services/api/internal/store"
)

// The jobs table is both the record and the queue: attempt, lease_token and
// lease_expires_at are what make a row claimable work rather than just state.
// The partial index is the claim query's access path -- it covers exactly the
// live rows and stays small no matter how much finished history piles up.
const migration = `
CREATE TABLE IF NOT EXISTS jobs (
	id               UUID PRIMARY KEY,
	filename         TEXT NOT NULL,
	pdf_key          TEXT NOT NULL,
	csv_key          TEXT,
	status           TEXT NOT NULL DEFAULT 'queued',
	error            TEXT,
	attempt          INT  NOT NULL DEFAULT 0,
	lease_token      UUID,
	lease_expires_at TIMESTAMPTZ,
	created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The queue columns are added separately so a volume carrying a pre-queue jobs
-- table gets upgraded instead of skipped: CREATE TABLE IF NOT EXISTS is a no-op
-- on an existing table, which would leave the workers crash-looping on columns
-- that never appeared. Columns from older shapes are left alone; they're inert.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS attempt          INT NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_token      UUID;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS jobs_ready_idx ON jobs (created_at)
	WHERE status IN ('queued', 'processing');
`

type App struct {
	cfg config.Config
	log *slog.Logger
	db  *pgxpool.Pool
	s3  *minio.Client
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

	h := hub.New(a.log)
	go h.Run()
	defer h.Close()

	evSrv := events.NewServer(a.log, h)
	grpcSrv := grpc.NewServer()
	jobsv1.RegisterJobEventsServer(grpcSrv, evSrv)

	grpcLn, err := net.Listen("tcp", a.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	handler := httpapi.New(a.log, store.New(a.db), a.s3, a.cfg.MinioBucket, h, evSrv)
	httpSrv := &http.Server{Addr: a.cfg.HTTPAddr, Handler: handler.Routes()}

	errCh := make(chan error, 2)
	go func() { errCh <- grpcSrv.Serve(grpcLn) }()
	go func() { errCh <- httpSrv.ListenAndServe() }()

	a.log.Info("api ready", "http", a.cfg.HTTPAddr, "grpc", a.cfg.GRPCAddr)

	select {
	case <-ctx.Done():
		a.log.Info("shutting down")
	case err := <-errCh:
		return err
	}

	// GracefulStop ignores contexts and can block on a held connection;
	// race it against its own budget and force-close on timeout.
	grpcCtx, grpcCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer grpcCancel()
	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-grpcCtx.Done():
		grpcSrv.Stop()
	}

	httpCtx, httpCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer httpCancel()
	if err := httpSrv.Shutdown(httpCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
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
	if _, err := db.Exec(ctx, migration); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	a.db = db

	s3, err := minio.New(a.cfg.MinioEndpoint, &minio.Options{
		Creds: credentials.NewStaticV4(a.cfg.MinioAccessKey, a.cfg.MinioSecretKey, ""),
	})
	if err != nil {
		return fmt.Errorf("minio client: %w", err)
	}
	exists, err := s3.BucketExists(ctx, a.cfg.MinioBucket)
	if err != nil {
		return fmt.Errorf("minio bucket check: %w", err)
	}
	if !exists {
		if err := s3.MakeBucket(ctx, a.cfg.MinioBucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("minio make bucket: %w", err)
		}
	}
	a.s3 = s3

	return nil
}

func (a *App) close() {
	if a.db != nil {
		a.db.Close()
	}
}
