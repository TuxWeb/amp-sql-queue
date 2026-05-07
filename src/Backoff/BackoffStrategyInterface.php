<?php

declare(strict_types=1);

namespace AmpSqlQueue\Backoff;

interface BackoffStrategyInterface
{
    public function delaySeconds(int $attempts, int $maxAttempts, ?\Throwable $error = null): int;
}
