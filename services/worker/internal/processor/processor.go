// Package processor executes one job end to end: claim, fetch pdf, parse,
// upload csv, record the terminal state, notify the api.
package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"

	jobsv1 "github.com/anatolyt/interview-mls/proto/gen/jobsv1"
	"github.com/anatolyt/interview-mls/services/worker/internal/parser"
	"github.com/anatolyt/interview-mls/services/worker/internal/store"
)

// Message is the kafka payload; the api produces the same shape.
type Message struct {
	JobID string `json:"job_id"`
}

type Processor struct {
	log        *slog.Logger
	store      *store.Store
	s3         *minio.Client
	bucket     string
	producer   *kgo.Client // requeue channel for bounded retries
	events     jobsv1.JobEventsClient
	maxRetries int
	parseDelay time.Duration
}

func New(log *slog.Logger, st *store.Store, s3 *minio.Client, bucket string,
	producer *kgo.Client, events jobsv1.JobEventsClient, maxRetries int, parseDelay time.Duration) *Processor {
	return &Processor{
		log: log, store: st, s3: s3, bucket: bucket,
		producer: producer, events: events,
		maxRetries: maxRetries, parseDelay: parseDelay,
	}
}

// Handle processes one kafka record and reports whether the record is safe to
// forget. It returns true only when the job reached a terminal db state, was
// successfully requeued (db update *and* replacement message both landed), or
// was deliberately dropped. A false means the job is still non-terminal with
// no replacement message in flight, so the offset must not be committed.
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) (commit bool) {
	var msg Message
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		p.log.Error("poison message: bad json, dropping", "value", string(rec.Value), "err", err)
		return true
	}
	if _, err := uuid.Parse(msg.JobID); err != nil {
		p.log.Error("poison message: bad job id, dropping", "job_id", msg.JobID)
		return true
	}
	log := p.log.With("job_id", msg.JobID)

	claim, skip, err := p.store.Claim(ctx, msg.JobID)
	if err != nil {
		// db unreachable: nothing was recorded, so the message must survive.
		log.Error("claim failed", "err", err)
		return false
	}
	if skip {
		log.Info("job missing or already terminal, skipping")
		return true
	}
	log = log.With("attempt", claim.Attempt)

	if claim.Attempt > p.maxRetries {
		log.Warn("retries exhausted, failing job")
		return p.finish(ctx, log, msg.JobID, jobsv1.JobStatus_JOB_STATUS_FAILED, "",
			fmt.Sprintf("failed after %d attempts", p.maxRetries))
	}

	p.notify(ctx, log, msg.JobID, jobsv1.JobStatus_JOB_STATUS_PROCESSING, "")

	csvKey, err := p.process(ctx, msg.JobID, claim.PdfKey)
	if err != nil {
		log.Error("processing failed", "err", err)
		if claim.Attempt >= p.maxRetries {
			return p.finish(ctx, log, msg.JobID, jobsv1.JobStatus_JOB_STATUS_FAILED, "", err.Error())
		}
		return p.requeue(ctx, log, msg.JobID, err.Error())
	}

	if !p.finish(ctx, log, msg.JobID, jobsv1.JobStatus_JOB_STATUS_DONE, csvKey, "") {
		return false
	}
	log.Info("job done", "csv_key", csvKey)
	return true
}

func (p *Processor) process(ctx context.Context, jobID, pdfKey string) (string, error) {
	// artificial delay so the "processing" state is visible in a demo
	if p.parseDelay > 0 {
		select {
		case <-time.After(p.parseDelay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	obj, err := p.s3.GetObject(ctx, p.bucket, pdfKey, minio.GetObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("get %s: %w", pdfKey, err)
	}
	defer obj.Close()
	var raw bytes.Buffer
	if _, err := raw.ReadFrom(obj); err != nil {
		return "", fmt.Errorf("read %s: %w", pdfKey, err)
	}

	txs, err := parser.Parse(raw.Bytes())
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	csvBytes, err := parser.ToCSV(txs)
	if err != nil {
		return "", fmt.Errorf("render csv: %w", err)
	}

	csvKey := fmt.Sprintf("jobs/%s/result.csv", jobID)
	_, err = p.s3.PutObject(ctx, p.bucket, csvKey, bytes.NewReader(csvBytes), int64(len(csvBytes)),
		minio.PutObjectOptions{ContentType: "text/csv"})
	if err != nil {
		return "", fmt.Errorf("put %s: %w", csvKey, err)
	}
	return csvKey, nil
}

// finish records the terminal state. It reports false when the db write
// failed: the job is still 'processing' and only a redelivery can resolve it.
func (p *Processor) finish(ctx context.Context, log *slog.Logger, jobID string, st jobsv1.JobStatus, csvKey, errMsg string) bool {
	var err error
	if st == jobsv1.JobStatus_JOB_STATUS_DONE {
		err = p.store.SetDone(ctx, jobID, csvKey)
	} else {
		err = p.store.SetFailed(ctx, jobID, errMsg)
	}
	if err != nil {
		log.Error("record terminal state", "err", err)
		return false
	}
	p.notify(ctx, log, jobID, st, errMsg)
	return true
}

// requeue hands the job to a future delivery. It reports true only when both
// halves landed; a produce failure after the db update would otherwise strand
// the job at 'queued' with no message anywhere.
func (p *Processor) requeue(ctx context.Context, log *slog.Logger, jobID, lastErr string) bool {
	if err := p.store.Requeue(ctx, jobID, lastErr); err != nil {
		log.Error("requeue db update", "err", err)
		return false
	}
	payload, _ := json.Marshal(Message{JobID: jobID})
	res := p.producer.ProduceSync(ctx, &kgo.Record{Key: []byte(jobID), Value: payload})
	if err := res.FirstErr(); err != nil {
		log.Error("requeue produce", "err", err)
		return false
	}
	p.notify(ctx, log, jobID, jobsv1.JobStatus_JOB_STATUS_QUEUED, lastErr)
	log.Info("job requeued for retry")
	return true
}

// notify is best-effort: a job's fate lives in the db, the push channel is
// only a hint for connected UIs.
func (p *Processor) notify(ctx context.Context, log *slog.Logger, jobID string, st jobsv1.JobStatus, errMsg string) {
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := p.events.NotifyStatus(callCtx, &jobsv1.NotifyStatusRequest{
		JobId:  jobID,
		Status: st,
		Error:  errMsg,
	})
	if err != nil {
		log.Warn("notify api failed", "status", st.String(), "err", err)
	}
}
