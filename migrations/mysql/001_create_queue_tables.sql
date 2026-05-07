CREATE TABLE amp_queue_jobs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    queue_name VARCHAR(190) NOT NULL,
    type VARCHAR(190) NOT NULL,
    payload JSON NOT NULL,
    status ENUM('pending', 'processing', 'completed', 'failed', 'cancelled') NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    locked_until DATETIME(6) NULL,
    locked_by VARCHAR(190) NULL,
    last_error JSON NULL,
    idempotency_key VARCHAR(255) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at DATETIME(6) NULL,
    UNIQUE KEY amp_queue_jobs_idempotency_unique (queue_name, idempotency_key),
    KEY amp_queue_jobs_reserve_idx (queue_name, status, available_at, priority, id),
    KEY amp_queue_jobs_locked_until_idx (status, locked_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE amp_queue_failed_jobs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    original_job_id BIGINT UNSIGNED NOT NULL,
    queue_name VARCHAR(190) NOT NULL,
    type VARCHAR(190) NOT NULL,
    payload JSON NOT NULL,
    attempts INT NOT NULL,
    max_attempts INT NOT NULL,
    last_error JSON NOT NULL,
    failed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    created_at DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
