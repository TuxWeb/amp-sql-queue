<?php

declare(strict_types=1);

namespace AmpSqlQueue\Storage;

final readonly class StoredJobEnvelope
{
    public function __construct(
        public string $queueName,
        public string $type,
        public string $payload,
        public int $delaySeconds,
        public int $priority,
        public int $maxAttempts,
        public ?string $idempotencyKey,
    ) {
    }
}
