// Package events owns the worker's single long-lived gRPC stream to the api:
// status events go up, cancel commands come down.
package events

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	jobsv1 "github.com/anatolyt/interview-mls/proto/gen/jobsv1"
)

const (
	// outBuffer absorbs a burst of status events while the stream is down.
	// Overflow drops the oldest event: the db holds the job's real fate, so a
	// stale intermediate status is the cheapest thing to throw away.
	outBuffer = 64
	minDelay  = time.Second
	maxDelay  = 30 * time.Second
)

// Client keeps one stream open for the life of the process, reconnecting
// forever. Send never blocks, so job processing is never coupled to the api
// being reachable.
type Client struct {
	log      *slog.Logger
	workerID string
	rpc      jobsv1.JobEventsClient
	out      chan *jobsv1.WorkerEvent
	onCancel func(jobID string)
}

// NewClient wires the stream to a cancel callback -- the processor's, which
// interrupts the job if this worker is the one holding it.
func NewClient(log *slog.Logger, conn grpc.ClientConnInterface, workerID string,
	onCancel func(jobID string)) *Client {
	return &Client{
		log:      log,
		workerID: workerID,
		rpc:      jobsv1.NewJobEventsClient(conn),
		out:      make(chan *jobsv1.WorkerEvent, outBuffer),
		onCancel: onCancel,
	}
}

// Send queues a status event. Dropping is deliberate: the queue exists so a
// slow or absent api can never stall a job.
func (c *Client) Send(jobID string, status jobsv1.JobStatus, errMsg string) {
	ev := &jobsv1.WorkerEvent{
		JobId:    jobID,
		Status:   status,
		Error:    errMsg,
		WorkerId: c.workerID,
	}
	select {
	case c.out <- ev:
		return
	default:
	}
	// Full: evict the oldest and retry once. Racing producers can steal the
	// freed slot, in which case this event is the one that goes.
	select {
	case <-c.out:
	default:
	}
	select {
	case c.out <- ev:
		c.log.Warn("event buffer full, dropped oldest status event", "job_id", jobID)
	default:
		c.log.Warn("event buffer full, dropped status event", "job_id", jobID,
			"status", status.String())
	}
}

// Run reconnects with capped backoff until the context ends.
func (c *Client) Run(ctx context.Context) {
	delay := minDelay
	for ctx.Err() == nil {
		if err := c.session(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("event stream lost, reconnecting", "err", err, "in", delay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			if delay *= 2; delay > maxDelay {
				delay = maxDelay
			}
			continue
		}
		delay = minDelay
	}
}

// session runs one stream: a receive goroutine for commands, the send loop
// here. Either direction failing tears the whole session down so Run can
// rebuild it.
func (c *Client) session(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := c.rpc.Connect(ctx)
	if err != nil {
		return err
	}
	c.log.Info("event stream open", "worker_id", c.workerID)

	recvErr := make(chan error, 1)
	go func() { recvErr <- c.recv(stream) }()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-recvErr:
			return err
		case ev := <-c.out:
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}

func (c *Client) recv(stream grpc.BidiStreamingClient[jobsv1.WorkerEvent, jobsv1.Command]) error {
	for {
		cmd, err := stream.Recv()
		if err != nil {
			// EOF is returned, not swallowed: a server that closed the stream
			// still means reconnect, and Run's backoff is what keeps that from
			// becoming a spin loop.
			return err
		}
		if cmd.GetType() != jobsv1.CommandType_COMMAND_TYPE_CANCEL {
			continue
		}
		// The api routes to whoever last reported PROCESSING; if the lease has
		// moved on since, this is a no-op and the real holder gets its own
		// command or the job simply finishes.
		c.log.Info("cancel command received", "job_id", cmd.GetJobId())
		c.onCancel(cmd.GetJobId())
	}
}
