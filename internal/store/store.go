package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Task struct {
	ID           uuid.UUID
	TaskType     string
	Payload      []byte
	Status       string
	Priority     int
	MaxRetries   int
	RetryCount   int
	ScheduledAt  *time.Time
	StartedAt    *time.Time
	CompletedAt  *time.Time
	ErrorMessage string
	WorkerID     string
	CreatedAt    time.Time
}

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Create(ctx context.Context, t *Task) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tasks (id, task_type, payload, status, priority, max_retries, scheduled_at)
		VALUES ($1,$2,$3,'pending',$4,$5,$6)`,
		t.ID, t.TaskType, t.Payload, t.Priority, t.MaxRetries, t.ScheduledAt)
	return err
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (*Task, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, task_type, payload, status, priority, max_retries, retry_count,
		       scheduled_at, started_at, completed_at,
		       COALESCE(error_message,''), COALESCE(worker_id,''), created_at
		FROM tasks WHERE id=$1`, id)
	var t Task
	err := row.Scan(&t.ID, &t.TaskType, &t.Payload, &t.Status, &t.Priority,
		&t.MaxRetries, &t.RetryCount, &t.ScheduledAt, &t.StartedAt,
		&t.CompletedAt, &t.ErrorMessage, &t.WorkerID, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &t, err
}

func (s *Store) MarkRunning(ctx context.Context, id uuid.UUID, workerID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status='running', worker_id=$2, started_at=now(), updated_at=now()
		WHERE id=$1`, id, workerID)
	return err
}

func (s *Store) MarkCompleted(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status='completed', completed_at=now(), updated_at=now()
		WHERE id=$1`, id)
	return err
}

func (s *Store) MarkFailed(ctx context.Context, id uuid.UUID, errMsg string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var retryCount, maxRetries int
	if err = tx.QueryRow(ctx,
		`SELECT retry_count, max_retries FROM tasks WHERE id=$1 FOR UPDATE`, id).
		Scan(&retryCount, &maxRetries); err != nil {
		return false, err
	}

	retry := retryCount < maxRetries
	if retry {
		_, err = tx.Exec(ctx, `
			UPDATE tasks SET status='retry', retry_count=retry_count+1,
			       error_message=$2, updated_at=now() WHERE id=$1`, id, errMsg)
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE tasks SET status='dead', error_message=$2, updated_at=now()
			WHERE id=$1`, id, errMsg)
	}
	if err != nil {
		return false, err
	}
	return retry, tx.Commit(ctx)
}

func (s *Store) ResetToPending(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status='pending', retry_count=0, error_message='',
		       worker_id='', started_at=NULL, completed_at=NULL, updated_at=now()
		WHERE id=$1`, id)
	return err
}

func (s *Store) List(ctx context.Context, status string, limit, offset int) ([]Task, int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, task_type, payload, status, priority, max_retries, retry_count,
		       scheduled_at, started_at, completed_at,
		       COALESCE(error_message,''), COALESCE(worker_id,''), created_at
		FROM tasks
		WHERE ($1='' OR status=$1)
		ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
		status, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.TaskType, &t.Payload, &t.Status, &t.Priority,
			&t.MaxRetries, &t.RetryCount, &t.ScheduledAt, &t.StartedAt,
			&t.CompletedAt, &t.ErrorMessage, &t.WorkerID, &t.CreatedAt); err != nil {
			return nil, 0, err
		}
		tasks = append(tasks, t)
	}
	var total int
	_ = s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE ($1='' OR status=$1)`, status).Scan(&total)
	return tasks, total, nil
}

func (s *Store) RecoverStale(ctx context.Context, timeout time.Duration) (int, error) {
	res, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status='pending', updated_at=now()
		WHERE status='running' AND updated_at < now() - $1::interval`,
		timeout.String())
	if err != nil {
		return 0, err
	}
	return int(res.RowsAffected()), nil
}
