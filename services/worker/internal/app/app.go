package app

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobsv1 "github.com/anatolyt/interview-mls/proto/gen/jobsv1"
	"github.com/anatolyt/interview-mls/services/worker/internal/config"
	"github.com/anatolyt/interview-mls/services/worker/internal/processor"
	"github.com/anatolyt/interview-mls/services/worker/internal/store"
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

	proc := processor.New(a.log, store.New(a.db), a.s3, a.cfg.MinioBucket,
		a.kafka, a.events, a.cfg.MaxRetries, a.cfg.ParseDelay)
	a.consume(ctx, proc)

	a.log.Info("shutting down")
	return nil
}

// stallBackoff paces the loop after a record we refused to commit, so a
// persistent outage doesn't turn into a hot spin.
const stallBackoff = 2 * time.Second

// consume runs the at-least-once loop: the offset is committed only after
// Handle reports the record is safe to forget -- terminal db state recorded,
// successfully requeued, or deliberately dropped.
func (a *App) consume(ctx context.Context, proc *processor.Processor) {
	for {
		fetches := a.kafka.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			a.log.Error("fetch", "topic", topic, "partition", partition, "err", err)
		})

		if processBatch(ctx, a.log, fetches, proc.Handle, a.kafka.CommitRecords) {
			select {
			case <-time.After(stallBackoff):
			case <-ctx.Done():
				return
			}
		}
	}
}

// topicPartition identifies one commit stream. Offsets are per topic-partition,
// so stalling is tracked at that granularity and not by partition number alone.
type topicPartition struct {
	topic     string
	partition int32
}

// processBatch handles every record in a fetch and reports whether any
// topic-partition stalled.
//
// A commit is a high-water mark on its own topic-partition, so once a record
// there is left uncommitted, no later record on that same partition may be
// committed either -- it would silently commit past the stranded one. That
// reasoning is strictly per-partition though: a fetch spans every partition
// assigned to this worker, and records on healthy partitions must still be
// processed. So we skip only the stalled partitions and keep going.
//
// Note franz-go will not redeliver a record already fetched in this session,
// so a stalled job actually waits for a rebalance or a worker restart --
// acceptable for a demo, and the alternative (seeking the partition back to
// the offset) is more machinery than this is worth.
func processBatch(
	ctx context.Context,
	log *slog.Logger,
	fetches kgo.Fetches,
	handle func(context.Context, *kgo.Record) bool,
	commit func(context.Context, ...*kgo.Record) error,
) (stalledAny bool) {
	stalled := map[topicPartition]bool{}

	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()
		tp := topicPartition{rec.Topic, rec.Partition}
		if stalled[tp] {
			continue
		}

		if !handle(ctx, rec) {
			log.Error("record left uncommitted: job is non-terminal with no message in flight",
				"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
			stalled[tp] = true
			continue
		}
		if err := commit(ctx, rec); err != nil {
			log.Error("commit", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
			stalled[tp] = true
		}
	}

	return len(stalled) > 0
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
		// the same client produces the requeue messages for bounded retries
		kgo.DefaultProduceTopic(a.cfg.KafkaTopic),
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
