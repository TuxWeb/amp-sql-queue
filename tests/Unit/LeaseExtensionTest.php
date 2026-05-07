<?php

declare(strict_types=1);

namespace AmpSqlQueue\Tests\Unit;

use AmpSqlQueue\Job;
use PHPUnit\Framework\TestCase;

final class LeaseExtensionTest extends TestCase
{
    public function testStorageCanExtendProcessingJobLease(): void
    {
        $job = new Job(
            id: 99,
            queueName: 'workflow',
            type: 'workflow.start',
            payload: [],
            status: 'processing',
            priority: 0,
            attempts: 1,
            maxAttempts: 3,
            availableAt: new \DateTimeImmutable(),
            lockedUntil: new \DateTimeImmutable('+60 seconds'),
            lockedBy: 'worker-1',
            lastError: null,
            idempotencyKey: null,
            createdAt: new \DateTimeImmutable(),
            updatedAt: new \DateTimeImmutable(),
            completedAt: null,
        );

        $storage = new RecordingStorage();
        $storage->extendLease($job, 120)->await();

        self::assertSame([[99, 120]], $storage->extendedLeases);
    }
}
