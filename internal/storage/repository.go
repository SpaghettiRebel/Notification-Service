// Package storage keeps the processing state of each event in PostgreSQL.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

type AcquisitionState int

const (
	Acquired AcquisitionState = iota
	AlreadyCompleted
	Busy
)

// Acquisition is the outcome of trying to take the processing lease of an event.
type Acquisition struct {
	State       AcquisitionState
	LockedUntil *time.Time
}

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// acquireLockSQL creates the row for a new event, or takes over a failed
// event or one whose processing lease has expired.
const acquireLockSQL = `
INSERT INTO notification_log (event_id, status, locked_until, attempt_count)
VALUES ($1, 'pending', $2, 1)
ON CONFLICT (event_id) DO UPDATE SET
    status        = 'pending',
    locked_until  = EXCLUDED.locked_until,
    attempt_count = notification_log.attempt_count + 1,
    last_error    = NULL,
    updated_at    = $3
WHERE notification_log.status = 'failed'
   OR (notification_log.status = 'pending'
       AND (notification_log.locked_until IS NULL OR notification_log.locked_until <= $3))
RETURNING event_id`

// AcquireLock tries to take the processing lease of eventID for lease.
func (r *Repository) AcquireLock(ctx context.Context, eventID string, lease time.Duration) (Acquisition, error) {
	now := time.Now().UTC()
	lockedUntil := now.Add(lease)

	var acquiredID string
	err := r.pool.QueryRow(ctx, acquireLockSQL, eventID, lockedUntil, now).Scan(&acquiredID)
	if err == nil {
		return Acquisition{State: Acquired, LockedUntil: &lockedUntil}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Acquisition{}, fmt.Errorf("acquire lock for %s: %w", eventID, err)
	}

	var status Status
	var existingLockedUntil *time.Time
	err = r.pool.QueryRow(ctx,
		`SELECT status, locked_until FROM notification_log WHERE event_id = $1`, eventID,
	).Scan(&status, &existingLockedUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return Acquisition{}, fmt.Errorf("notification row %s disappeared during lock acquisition", eventID)
	}
	if err != nil {
		return Acquisition{}, fmt.Errorf("read lock state for %s: %w", eventID, err)
	}
	if status == StatusCompleted {
		return Acquisition{State: AlreadyCompleted}, nil
	}
	return Acquisition{State: Busy, LockedUntil: existingLockedUntil}, nil
}

// UpdateStatus records the final status of eventID and releases its lease.
func (r *Repository) UpdateStatus(ctx context.Context, eventID string, status Status, lastError *string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE notification_log
		SET status = $2, locked_until = NULL, last_error = $3, updated_at = now()
		WHERE event_id = $1`,
		eventID, status, lastError,
	)
	if err != nil {
		return fmt.Errorf("update status of %s: %w", eventID, err)
	}
	if n := tag.RowsAffected(); n != 1 {
		return fmt.Errorf("notification status update affected %d rows", n)
	}
	return nil
}
