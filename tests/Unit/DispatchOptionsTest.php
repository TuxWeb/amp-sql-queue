<?php

declare(strict_types=1);

namespace AmpSqlQueue\Tests\Unit;

use AmpSqlQueue\DispatchOptions;
use AmpSqlQueue\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class DispatchOptionsTest extends TestCase
{
    public function testRejectsNegativeDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new DispatchOptions(delaySeconds: -1);
    }

    public function testRejectsZeroMaxAttempts(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new DispatchOptions(maxAttempts: 0);
    }
}
