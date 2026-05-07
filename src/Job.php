<?php

declare(strict_types=1);

namespace AmpSqlQueue;

use AmpSqlQueue\Exception\InvalidArgumentException;

final readonly class Job
{
    /**
     * @param array<string, mixed> $payload
     * @param array<string, mixed>|null $lastError
     */
    public function __construct(
        public int|string $id,
        public string $queueName,
        public string $type,
        public array $payload,
        public string $status,
        public int $priority,
        public int $attempts,
        public int $maxAttempts,
        public \DateTimeImmutable $availableAt,
        public ?\DateTimeImmutable $lockedUntil,
        public ?string $lockedBy,
        public ?array $lastError,
        public ?string $idempotencyKey,
        public \DateTimeImmutable $createdAt,
        public \DateTimeImmutable $updatedAt,
        public ?\DateTimeImmutable $completedAt,
    ) {
        if (!\in_array($this->status, JobStatus::all(), true)) {
            throw new InvalidArgumentException('Unsupported job status: ' . $this->status);
        }

        if ($this->attempts < 0) {
            throw new InvalidArgumentException('Attempts must be greater than or equal to zero.');
        }

        if ($this->maxAttempts < 1) {
            throw new InvalidArgumentException('Max attempts must be greater than or equal to one.');
        }
    }
}
