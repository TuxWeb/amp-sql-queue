<?php

declare(strict_types=1);

namespace AmpSqlQueue;

final readonly class DispatchResult
{
    public function __construct(
        public int|string|null $jobId,
        public string $queueName,
        public string $type,
        public bool $inserted,
        public ?string $idempotencyKey,
    ) {
    }
}
