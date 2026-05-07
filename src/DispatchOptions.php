<?php

declare(strict_types=1);

namespace AmpSqlQueue;

use AmpSqlQueue\Exception\InvalidArgumentException;

final readonly class DispatchOptions
{
    public function __construct(
        public int $delaySeconds = 0,
        public int $priority = 0,
        public int $maxAttempts = 3,
        public ?string $idempotencyKey = null,
    ) {
        if ($this->delaySeconds < 0) {
            throw new InvalidArgumentException('Dispatch delay must be greater than or equal to zero.');
        }

        if ($this->maxAttempts < 1) {
            throw new InvalidArgumentException('Max attempts must be greater than or equal to one.');
        }

        if ($this->idempotencyKey === '') {
            throw new InvalidArgumentException('Idempotency key cannot be empty.');
        }
    }
}
