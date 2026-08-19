// Package store owns the worker's half of the job queue: the claim, the
// terminal transitions, and the janitor sweep. The jobs table is the queue --
// there is no second system to keep in sync with it.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db          *pgxpool.Pool
	lease       time.Duration
	maxAttempts int
}

func New(db *pgxpool.Pool, lease time.Duration, maxAttempts int) *Store {
	return &Store{db: db, lease: lease, maxAttempts: maxAttempts}
}

// Claim is one job leased to this worker. LeaseToken fences every later write
// against it: a worker that lost its lease can no longer touch the row.
type Claim struct {
	JobID      string
	PdfKey     string
	Attempt    int
	LeaseToken string
}

// Claim takes the oldest ready job -- queued, or processing with an expired
// lease -- and leases it to this worker.
//
// FOR UPDATE SKIP LOCKED gives mutual exclusion between workers, but only for
// the microseconds this statement runs: the row lock is gone at commit, long
// before the pdf is parsed. Exclusion for the actual work is carried by the
// lease_expires_at *value*, not by a held lock -- holding a transaction open
// for the length of a job would pin a connection, block vacuum and bloat the
// table. Crash recovery falls out of the same query: a worker killed mid-parse
// leaves a row at 'processing' whose lease simply expires, and the OR branch
// picks it back up. Returns ok=false when nothing is ready.
func (s *Store) Claim(ctx context.Context) (Claim, bool, error) {
	var c Claim
	err := s.db.QueryRow(ctx, `
		UPDATE jobs SET
			status           = 'processing',
			attempt          = attempt + 1,
			lease_token      = gen_random_uuid(),
			lease_expires_at = now() + make_interval(secs => $1),
			updated_at       = now()
		WHERE id = (
			SELECT id FROM jobs
			WHERE (status = 'queued' OR (status = 'processing' AND lease_expires_at < now()))
			  AND attempt < $2
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id::text, pdf_key, attempt, lease_token::text`,
		s.lease.Seconds(), s.maxAttempts,
	).Scan(&c.JobID, &c.PdfKey, &c.Attempt, &c.LeaseToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, false, nil
	}
	if err != nil {
		return Claim{}, false, fmt.Errorf("claim: %w", err)
	}
	return c, true, nil
}

// SetDone records success. Fenced: see fenced().
func (s *Store) SetDone(ctx context.Context, jobID, leaseToken, csvKey string) (bool, error) {
	return s.fenced(ctx, `
		UPDATE jobs SET
			status = 'done', csv_key = $3, error = NULL,
			lease_token = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND lease_token = $2::uuid`, jobID, leaseToken, csvKey)
}

// SetFailed records the terminal failure. Fenced: see fenced().
func (s *Store) SetFailed(ctx context.Context, jobID, leaseToken, msg string) (bool, error) {
	return s.fenced(ctx, `
		UPDATE jobs SET
			status = 'failed', error = $3,
			lease_token = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND lease_token = $2::uuid`, jobID, leaseToken, msg)
}

// Requeue returns a transiently failed job to the queue with attempts left.
// The pg_notify rides the same commit as the UPDATE, so another worker wakes
// immediately instead of waiting out the poll interval. Fenced: see fenced().
func (s *Store) Requeue(ctx context.Context, jobID, leaseToken, lastErr string) (bool, error) {
	return s.fenced(ctx, `
		WITH requeued AS (
			UPDATE jobs SET
				status = 'queued', error = $3,
				lease_token = NULL, lease_expires_at = NULL, updated_at = now()
			WHERE id = $1 AND lease_token = $2::uuid
			RETURNING id
		)
		SELECT pg_notify('jobs_new', id::text) FROM requeued`, jobID, leaseToken, lastErr)
}

// fenced runs a write gated on the lease token and reports whether it landed.
//
// A worker that is merely partitioned from the db, not dead, doesn't know its
// lease expired -- so a second worker legitimately claims the same job and
// both run. Gating the terminal write on the lease token means the stale
// worker matches zero rows and learns it lost the race, instead of stamping
// 'done' over the live attempt. The duplicate MinIO write costs nothing: the
// csv key is derived from the job id, so it's an idempotent overwrite.
func (s *Store) fenced(ctx context.Context, sql string, args ...any) (bool, error) {
	tag, err := s.db.Exec(ctx, sql, args...)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// FailExhausted retires jobs that ran out of attempts while leased. Without it
// they would sit at 'processing' with an expired lease forever -- invisible to
// the claim query (attempt >= max) and never reaching a terminal state. It is
// idempotent, so every worker can run it on a ticker.
func (s *Store) FailExhausted(ctx context.Context) (int64, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE jobs SET
			status = 'failed',
			error = 'abandoned: lease expired after max attempts',
			lease_token = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE status = 'processing' AND lease_expires_at < now() AND attempt >= $1`,
		s.maxAttempts)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
