// Package processor executes one claimed job end to end: fetch pdf, parse,
// upload csv, record the terminal state, notify the api.
package processor

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"

	jobsv1 "github.com/anatolyt/interview-mls/proto/gen/jobsv1"
	"github.com/anatolyt/interview-mls/services/worker/internal/parser"
	"github.com/anatolyt/interview-mls/services/worker/internal/store"
)

// canceledError is the error text a user-canceled job carries. Cancel reuses
// the 'failed' terminal state rather than introducing a status value: the
// schema stays as it is and the distinction lives in text the UI shows anyway.
const canceledError = "canceled by user"

// emitter is the status channel the processor writes to (the gRPC stream
// client). It is deliberately fire-and-forget.
type emitter interface {
	Send(jobID string, status jobsv1.JobStatus, errMsg string)
}

// run is the cancel handle for one in-flight job.
type run struct {
	cancel   context.CancelFunc
	canceled bool
}

type Processor struct {
	log        *slog.Logger
	store      *store.Store
	s3         *minio.Client
	bucket     string
	events     emitter
	maxRetries int
	parseDelay time.Duration

	mu      sync.Mutex
	running map[string]*run
}

func New(log *slog.Logger, st *store.Store, s3 *minio.Client, bucket string,
	events emitter, maxRetries int, parseDelay time.Duration) *Processor {
	return &Processor{
		log: log, store: st, s3: s3, bucket: bucket, events: events,
		maxRetries: maxRetries, parseDelay: parseDelay,
		running: make(map[string]*run),
	}
}

// Cancel interrupts a job this worker is currently processing. A command for a
// job we don't hold is a no-op: the api routes to the last worker that reported
// PROCESSING, and losing that race just means nobody gets canceled.
func (p *Processor) Cancel(jobID string) {
	p.mu.Lock()
	r, ok := p.running[jobID]
	if ok {
		r.canceled = true
	}
	p.mu.Unlock()
	if !ok {
		p.log.Info("cancel ignored, job not held by this worker", "job_id", jobID)
		return
	}
	r.cancel()
}

// ProcessJob runs one claimed job. Every state write is fenced on the claim's
// lease token, so losing the lease mid-job costs a log line, never a bogus
// terminal state.
func (p *Processor) ProcessJob(ctx context.Context, c store.Claim) {
	log := p.log.With("job_id", c.JobID, "attempt", c.Attempt)
	log.Info("claimed job")

	// The job's own context is what a cancel command interrupts; the terminal
	// write afterwards deliberately uses the parent, which is still alive.
	jobCtx, cancel := p.begin(ctx, c.JobID)
	defer p.end(c.JobID, cancel)

	p.notify(log, c.JobID, jobsv1.JobStatus_JOB_STATUS_PROCESSING, "")

	csvKey, err := p.process(jobCtx, c.JobID, c.PdfKey)
	if err == nil {
		p.finish(ctx, log, c, jobsv1.JobStatus_JOB_STATUS_DONE, csvKey, "")
		return
	}

	// A canceled job is terminal on the spot -- retrying something a user just
	// asked us to stop would be absurd.
	if p.wasCanceled(c.JobID) {
		log.Info("job canceled by user")
		p.finish(ctx, log, c, jobsv1.JobStatus_JOB_STATUS_FAILED, "", canceledError)
		return
	}

	log.Error("processing failed", "err", err)
	if c.Attempt >= p.maxRetries {
		p.finish(ctx, log, c, jobsv1.JobStatus_JOB_STATUS_FAILED, "", err.Error())
		return
	}

	ok, rerr := p.store.Requeue(ctx, c.JobID, c.LeaseToken, err.Error())
	switch {
	case rerr != nil:
		// Nothing recorded, but the lease still expires -- the claim query
		// picks the job back up on its own. No requeue path needed.
		log.Error("requeue", "err", rerr)
	case !ok:
		log.Warn("lost lease before requeue, another worker owns this job")
	default:
		p.notify(log, c.JobID, jobsv1.JobStatus_JOB_STATUS_QUEUED, err.Error())
		log.Info("job requeued for retry")
	}
}

func (p *Processor) begin(ctx context.Context, jobID string) (context.Context, context.CancelFunc) {
	jobCtx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	p.running[jobID] = &run{cancel: cancel}
	p.mu.Unlock()
	return jobCtx, cancel
}

func (p *Processor) end(jobID string, cancel context.CancelFunc) {
	p.mu.Lock()
	delete(p.running, jobID)
	p.mu.Unlock()
	cancel()
}

func (p *Processor) wasCanceled(jobID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.running[jobID]
	return ok && r.canceled
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

// finish records the terminal state under the fence. A write that fails or
// finds no row leaves the job at 'processing' with an expiring lease, which
// the claim query and the janitor between them always resolve.
func (p *Processor) finish(ctx context.Context, log *slog.Logger, c store.Claim, st jobsv1.JobStatus, csvKey, errMsg string) {
	var (
		ok  bool
		err error
	)
	if st == jobsv1.JobStatus_JOB_STATUS_DONE {
		ok, err = p.store.SetDone(ctx, c.JobID, c.LeaseToken, csvKey)
	} else {
		ok, err = p.store.SetFailed(ctx, c.JobID, c.LeaseToken, errMsg)
	}
	switch {
	case err != nil:
		log.Error("record terminal state", "status", st.String(), "err", err)
	case !ok:
		log.Warn("lost lease before terminal write, another worker owns this job", "status", st.String())
	default:
		log.Info("job "+terminalWord(st), "csv_key", csvKey, "error", errMsg)
		p.notify(log, c.JobID, st, errMsg)
	}
}

func terminalWord(st jobsv1.JobStatus) string {
	if st == jobsv1.JobStatus_JOB_STATUS_DONE {
		return "done"
	}
	return "failed"
}

// notify is best-effort and never blocks: a job's fate lives in the db, the
// push channel is only a hint for connected UIs. Send queues onto the stream
// client's buffer, so an unreachable api costs nothing here.
func (p *Processor) notify(log *slog.Logger, jobID string, st jobsv1.JobStatus, errMsg string) {
	log.Debug("status event", "status", st.String())
	p.events.Send(jobID, st, errMsg)
}
