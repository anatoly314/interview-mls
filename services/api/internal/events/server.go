// Package events serves the workers' bidirectional gRPC stream: status events
// come up and get fanned out to websocket clients, cancel commands go down to
// the one worker that holds the job's lease.
package events

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"

	jobsv1 "github.com/anatolyt/interview-mls/proto/gen/jobsv1"
	"github.com/anatolyt/interview-mls/services/api/internal/hub"
)

// commandBuffer bounds the per-worker outbound queue. Commands are rare
// (a human clicking Cancel), so anything past this means the stream is wedged.
const commandBuffer = 16

// worker is one connected stream. Sends go through the channel because a gRPC
// stream is not safe for concurrent Send; one goroutine per stream owns it.
type worker struct {
	id   string
	send chan *jobsv1.Command
}

type Server struct {
	jobsv1.UnimplementedJobEventsServer

	log *slog.Logger
	hub *hub.Hub

	mu      sync.Mutex
	workers map[string]*worker
	// leases maps job id -> the connection that reported PROCESSING. This is
	// how a cancel finds its target: pg_notify broadcasts, but a cancel has to
	// reach exactly the lease holder.
	//
	// It holds the *worker, not the worker id, because a reconnecting worker
	// keeps its id: keying by id lets the old stream's teardown delete leases
	// the new stream just recorded. Pointer identity makes every mutation
	// specific to one connection.
	leases map[string]*worker
}

func NewServer(log *slog.Logger, h *hub.Hub) *Server {
	return &Server{
		log:     log,
		hub:     h,
		workers: make(map[string]*worker),
		leases:  make(map[string]*worker),
	}
}

// Connect runs for the life of a worker process. The receive loop stays on
// this goroutine (gRPC requires it); a sender goroutine drains the command
// channel until the stream context ends.
func (s *Server) Connect(stream jobsv1.JobEvents_ConnectServer) error {
	var w *worker
	defer func() {
		if w != nil {
			s.unregister(w)
			s.log.Info("worker stream closed", "worker_id", w.id)
		}
	}()

	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if w != nil {
				s.log.Warn("worker stream error", "worker_id", w.id, "err", err)
			}
			return err
		}

		// The worker id only arrives with the first event, so registration
		// happens here rather than at stream setup. That is harmless: the only
		// thing routing needs is the PROCESSING event, which registers the
		// worker before any cancel for that job could be issued.
		if w == nil {
			w = s.register(ev.GetWorkerId())
			go s.sendLoop(stream, w)
			s.log.Info("worker stream open", "worker_id", w.id)
		}

		s.track(w, ev)
		s.hub.Broadcast(hub.Event{
			JobID:  ev.GetJobId(),
			Status: statusString(ev.GetStatus()),
			Error:  ev.GetError(),
		})
	}
}

func (s *Server) sendLoop(stream jobsv1.JobEvents_ConnectServer, w *worker) {
	for {
		select {
		case <-stream.Context().Done():
			return
		case cmd := <-w.send:
			if err := stream.Send(cmd); err != nil {
				s.log.Warn("send command", "worker_id", w.id, "job_id", cmd.GetJobId(), "err", err)
				return
			}
		}
	}
}

// Cancel routes a cancel command to the worker holding the job's lease and
// reports whether there was one to route to.
func (s *Server) Cancel(jobID string) bool {
	s.mu.Lock()
	w := s.leases[jobID]
	s.mu.Unlock()
	if w == nil {
		return false
	}
	select {
	case w.send <- &jobsv1.Command{JobId: jobID, Type: jobsv1.CommandType_COMMAND_TYPE_CANCEL}:
		return true
	default:
		s.log.Warn("worker command queue full", "worker_id", w.id, "job_id", jobID)
		return false
	}
}

func (s *Server) register(id string) *worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A reconnecting worker keeps its id; replacing the entry drops the stale
	// stream's queue, which is what we want -- its commands can't be delivered.
	w := &worker{id: id, send: make(chan *jobsv1.Command, commandBuffer)}
	s.workers[id] = w
	return w
}

func (s *Server) unregister(w *worker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workers[w.id] == w {
		delete(s.workers, w.id)
	}
	// Only this connection's leases: a reconnected stream with the same worker
	// id may already own some of these entries.
	for jobID, holder := range s.leases {
		if holder == w {
			delete(s.leases, jobID)
		}
	}
}

// track keeps the job -> lease holder map in step with the event stream. Every
// mutation is scoped to the reporting connection, so a stale stream can only
// ever clear entries it set itself.
func (s *Server) track(w *worker, ev *jobsv1.WorkerEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ev.GetStatus() == jobsv1.JobStatus_JOB_STATUS_PROCESSING {
		s.leases[ev.GetJobId()] = w
		return
	}
	// QUEUED (retry), DONE and FAILED all end this connection's hold.
	if s.leases[ev.GetJobId()] == w {
		delete(s.leases, ev.GetJobId())
	}
}

// statusString turns JOB_STATUS_PROCESSING into "processing" -- the same
// lowercase vocabulary the jobs table and GET /jobs use.
func statusString(st jobsv1.JobStatus) string {
	return strings.ToLower(strings.TrimPrefix(st.String(), "JOB_STATUS_"))
}
