// Package store owns the worker's job-state transitions in Postgres.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// Claim marks the job as processing and bumps its attempt counter in one
// atomic statement, so a redelivered message for an already-finished job is
// a no-op. Returns skip=true when the job is missing or already terminal.
type Claim struct {
	PdfKey  string
	Attempt int
}

func (s *Store) Claim(ctx context.Context, jobID string) (c Claim, skip bool, err error) {
	err = s.db.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'processing', retry_count = retry_count + 1, updated_at = now()
		WHERE id = $1 AND status NOT IN ('done', 'failed')
		RETURNING pdf_key, retry_count`, jobID).Scan(&c.PdfKey, &c.Attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, true, nil
	}
	if err != nil {
		return Claim{}, false, fmt.Errorf("claim job %s: %w", jobID, err)
	}
	return c, false, nil
}

// Requeue puts a failed attempt back to queued so a redelivered message
// gets processed again.
func (s *Store) Requeue(ctx context.Context, jobID, lastErr string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE jobs SET status = 'queued', error = $2, updated_at = now()
		WHERE id = $1`, jobID, lastErr)
	return err
}

func (s *Store) SetDone(ctx context.Context, jobID, csvKey string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE jobs SET status = 'done', csv_key = $2, error = NULL, updated_at = now()
		WHERE id = $1`, jobID, csvKey)
	return err
}

func (s *Store) SetFailed(ctx context.Context, jobID, msg string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE jobs SET status = 'failed', error = $2, updated_at = now()
		WHERE id = $1`, jobID, msg)
	return err
}
