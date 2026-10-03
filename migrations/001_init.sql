CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tasks (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type     TEXT NOT NULL,
    payload       BYTEA NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    priority      INT NOT NULL DEFAULT 0,
    max_retries   INT NOT NULL DEFAULT 3,
    retry_count   INT NOT NULL DEFAULT 0,
    scheduled_at  TIMESTAMPTZ,
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    error_message TEXT,
    worker_id     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_priority
    ON tasks (status, priority DESC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_tasks_scheduled
    ON tasks (scheduled_at)
    WHERE scheduled_at IS NOT NULL AND status = 'pending';

CREATE TABLE IF NOT EXISTS task_events (
    id         BIGSERIAL PRIMARY KEY,
    task_id    UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    event      TEXT NOT NULL,
    worker_id  TEXT,
    message    TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id, created_at);
