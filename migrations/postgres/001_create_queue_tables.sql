CREATE TABLE amp_queue_jobs (
    id BIGSERIAL PRIMARY KEY,
    queue_name VARCHAR(190) NOT NULL,
    type VARCHAR(190) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until TIMESTAMPTZ NULL,
    locked_by VARCHAR(190) NULL,
    last_error JSONB NULL,
    idempotency_key VARCHAR(255) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    CONSTRAINT amp_queue_jobs_status_check
        CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'cancelled'))
);

CREATE UNIQUE INDEX amp_queue_jobs_idempotency_unique
    ON amp_queue_jobs (queue_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX amp_queue_jobs_reserve_idx
    ON amp_queue_jobs (queue_name, status, available_at, priority DESC, id ASC);

CREATE INDEX amp_queue_jobs_locked_until_idx
    ON amp_queue_jobs (locked_until)
    WHERE status = 'processing';

CREATE TABLE amp_queue_failed_jobs (
    id BIGSERIAL PRIMARY KEY,
    original_job_id BIGINT NOT NULL,
    queue_name VARCHAR(190) NOT NULL,
    type VARCHAR(190) NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    last_error JSONB NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL
);
