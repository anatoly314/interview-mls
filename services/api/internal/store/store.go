// Package store owns the api's half of the jobs table: enqueue, list, lookup
// and the queued-job cancel. The worker's half (claim, lease, terminal writes)
// lives in the worker module -- the table is shared, the queries are not.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a job id doesn't exist.
var ErrNotFound = errors.New("job not found")

type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// Job is the list/detail projection. HasCSV rather than the key itself: the
// UI only needs to know whether the download link is live.
type Job struct {
	ID        string    `json:"id"`
	Filename  string    `json:"filename"`
	Status    string    `json:"status"`
	Error     *string   `json:"error"`
	Attempt   int       `json:"attempt"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	HasCSV    bool      `json:"has_csv"`
}

// Enqueue inserts the job row and fires the wake-up in one transaction.
//
// pg_notify inside the transaction is what removes the dual write: Postgres
// holds the notification until commit and discards it on rollback, so there is
// no window where the row exists without a signal or the other way round.
func (s *Store) Enqueue(ctx context.Context, id, filename, pdfKey string) (time.Time, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var createdAt time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (id, filename, pdf_key, status)
		VALUES ($1::uuid, $2, $3, 'queued')
		RETURNING created_at`, id, filename, pdfKey).Scan(&createdAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("insert job: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify('jobs_new', $1)`, id); err != nil {
		return time.Time{}, fmt.Errorf("notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, fmt.Errorf("commit: %w", err)
	}
	return createdAt, nil
}

// List returns every job, newest first. This is the UI's reconciliation call:
// websocket events are a hint, this is the truth.
func (s *Store) List(ctx context.Context) ([]Job, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, filename, status, error, attempt, created_at, updated_at,
		       csv_key IS NOT NULL
		FROM jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Filename, &j.Status, &j.Error, &j.Attempt,
			&j.CreatedAt, &j.UpdatedAt, &j.HasCSV); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Detail carries the fields the download and cancel handlers need.
type Detail struct {
	ID       string
	Filename string
	Status   string
	CSVKey   *string
}

func (s *Store) Get(ctx context.Context, id string) (Detail, error) {
	var d Detail
	err := s.db.QueryRow(ctx, `
		SELECT id::text, filename, status, csv_key FROM jobs WHERE id = $1::uuid`, id).
		Scan(&d.ID, &d.Filename, &d.Status, &d.CSVKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return Detail{}, ErrNotFound
	}
	if err != nil {
		return Detail{}, fmt.Errorf("get job: %w", err)
	}
	return d, nil
}

// CancelQueued fails a job that hasn't been claimed yet. No fence is needed or
// possible: a queued row has no lease holder, and the WHERE clause on status
// *is* the guard. If a worker claimed the row a moment ago the UPDATE matches
// zero rows and the caller falls through to routing a cancel command instead.
func (s *Store) CancelQueued(ctx context.Context, id, msg string) (bool, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE jobs SET
			status = 'failed', error = $2,
			lease_token = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1::uuid AND status = 'queued'`, id, msg)
	if err != nil {
		return false, fmt.Errorf("cancel queued: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
