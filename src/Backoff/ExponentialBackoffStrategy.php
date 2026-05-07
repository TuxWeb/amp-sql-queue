<?php

declare(strict_types=1);

namespace AmpSqlQueue\Backoff;

use AmpSqlQueue\Exception\InvalidArgumentException;

final readonly class ExponentialBackoffStrategy implements BackoffStrategyInterface
{
    public function __construct(
        private int $baseDelaySeconds = 1,
        private int $maxDelaySeconds = 300,
        private float $multiplier = 2.0,
        private float $jitterRatio = 0.0,
    ) {
        if ($this->baseDelaySeconds < 0) {
            throw new InvalidArgumentException('Base delay must be greater than or equal to zero.');
        }

        if ($this->maxDelaySeconds < 0) {
            throw new InvalidArgumentException('Max delay must be greater than or equal to zero.');
        }

        if ($this->multiplier < 1.0) {
            throw new InvalidArgumentException('Backoff multiplier must be greater than or equal to one.');
        }

        if ($this->jitterRatio < 0.0 || $this->jitterRatio > 1.0) {
            throw new InvalidArgumentException('Jitter ratio must be between zero and one.');
        }
    }

    public function delaySeconds(int $attempts, int $maxAttempts, ?\Throwable $error = null): int
    {
        if ($attempts < 1 || $attempts >= $maxAttempts) {
            return 0;
        }

        $delay = (int) \round($this->baseDelaySeconds * ($this->multiplier ** ($attempts - 1)));
        $delay = \min($delay, $this->maxDelaySeconds);

        if ($delay > 0 && $this->jitterRatio > 0.0) {
            $jitter = (int) \round($delay * $this->jitterRatio);
            $delay += \random_int(-$jitter, $jitter);
        }

        return \max(0, $delay);
    }
}
