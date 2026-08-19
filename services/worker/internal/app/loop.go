package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/anatolyt/interview-mls/services/worker/internal/processor"
	"github.com/anatolyt/interview-mls/services/worker/internal/store"
)

// notifyChannel is the wake-up channel; the api fires it inside the same
// transaction that inserts the job row.
const notifyChannel = "jobs_new"

// listenBackoff paces reconnects of the LISTEN connection. It can be generous
// because the poll fallback keeps the worker correct while the listener is down.
const listenBackoff = 2 * time.Second

// run is the whole worker loop: drain every ready job, then sleep until a
// notification or the poll timeout, then drain again.
func (a *App) run(ctx context.Context, st *store.Store, proc *processor.Processor) {
	l := &listener{log: a.log, dsn: a.cfg.PostgresDSN, poll: a.cfg.PollInterval}
	defer l.close()

	go a.janitor(ctx, st)

	for ctx.Err() == nil {
		a.drain(ctx, st, proc)
		l.wait(ctx)
	}
}

// drain claims and runs jobs until the queue has nothing ready for us. Claiming
// in a loop rather than once per notification is what makes NOTIFY optional:
// a burst of inserts that produced a single wake-up still gets fully drained.
func (a *App) drain(ctx context.Context, st *store.Store, proc *processor.Processor) {
	for ctx.Err() == nil {
		claim, ok, err := st.Claim(ctx)
		if err != nil {
			if ctx.Err() == nil {
				a.log.Error("claim", "err", err)
			}
			return
		}
		if !ok {
			return
		}
		proc.ProcessJob(ctx, claim)
	}
}

// janitor retires jobs whose lease expired with no attempts left. One UPDATE on
// a ticker, idempotent, so every worker runs its own -- no leader election, no
// singleton container.
func (a *App) janitor(ctx context.Context, st *store.Store) {
	t := time.NewTicker(a.cfg.JanitorInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := st.FailExhausted(ctx)
			if err != nil {
				if ctx.Err() == nil {
					a.log.Error("janitor", "err", err)
				}
				continue
			}
			if n > 0 {
				a.log.Warn("janitor failed abandoned jobs", "count", n)
			}
		}
	}
}

// listener owns the LISTEN connection.
//
// LISTEN registrations are connection-scoped, so this has to be a standalone
// pgx.Conn: a pooled connection would carry the registration until the pool
// recycled it under some unrelated query, and PgBouncer in transaction mode
// drops LISTEN outright. Notifications are also undelivered to whoever isn't
// connected at the moment they fire, with no replay -- which is why wait()
// always has a poll timeout. Losing every notification costs latency and never
// costs a job; correctness lives entirely in the claim query.
type listener struct {
	log  *slog.Logger
	dsn  string
	poll time.Duration
	conn *pgx.Conn
}

// wait blocks until a notification arrives or the poll interval elapses.
func (l *listener) wait(ctx context.Context) {
	if l.conn == nil && !l.connect(ctx) {
		l.sleep(ctx, listenBackoff)
		return
	}

	waitCtx, cancel := context.WithTimeout(ctx, l.poll)
	defer cancel()
	_, err := l.conn.WaitForNotification(waitCtx)
	switch {
	case err == nil, pgconn.Timeout(err):
		// A deadline hit while waiting leaves the connection usable; pgx only
		// tears one down on errors that aren't timeouts.
	case ctx.Err() != nil:
	default:
		l.log.Error("listen connection lost, falling back to polling", "err", err)
		l.close()
	}
}

func (l *listener) connect(ctx context.Context) bool {
	conn, err := pgx.Connect(ctx, l.dsn)
	if err != nil {
		if ctx.Err() == nil {
			l.log.Error("listener connect", "err", err)
		}
		return false
	}
	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		if ctx.Err() == nil {
			l.log.Error("listen", "err", err)
		}
		conn.Close(ctx)
		return false
	}
	l.conn = conn
	l.log.Info("listening for job notifications", "channel", notifyChannel, "poll", l.poll)
	return true
}

func (l *listener) close() {
	if l.conn == nil {
		return
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = l.conn.Close(closeCtx)
	l.conn = nil
}

func (l *listener) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
