<?php

declare(strict_types=1);

namespace AmpSqlQueue\Tests\Unit;

use AmpSqlQueue\Backoff\ExponentialBackoffStrategy;
use PHPUnit\Framework\TestCase;

final class ExponentialBackoffStrategyTest extends TestCase
{
    public function testComputesBoundedExponentialDelay(): void
    {
        $strategy = new ExponentialBackoffStrategy(baseDelaySeconds: 2, maxDelaySeconds: 20, multiplier: 3.0);

        self::assertSame(2, $strategy->delaySeconds(attempts: 1, maxAttempts: 5));
        self::assertSame(6, $strategy->delaySeconds(attempts: 2, maxAttempts: 5));
        self::assertSame(18, $strategy->delaySeconds(attempts: 3, maxAttempts: 5));
        self::assertSame(20, $strategy->delaySeconds(attempts: 4, maxAttempts: 5));
    }
}
