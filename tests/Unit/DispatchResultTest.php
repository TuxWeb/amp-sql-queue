<?php

declare(strict_types=1);

namespace AmpSqlQueue\Tests\Unit;

use AmpSqlQueue\DispatchOptions;
use AmpSqlQueue\DispatchResult;
use AmpSqlQueue\Queue;
use PHPUnit\Framework\TestCase;

final class DispatchResultTest extends TestCase
{
    public function testDispatchReturnsStorageResult(): void
    {
        $storage = new RecordingStorage();
        $storage->dispatchResult = new DispatchResult(
            jobId: 42,
            queueName: 'workflow',
            type: 'workflow.start',
            inserted: true,
            idempotencyKey: 'workflow:42',
        );

        $queue = new Queue($storage);

        $result = $queue->dispatch(
            queue: 'workflow',
            type: 'workflow.start',
            payload: ['workflow_id' => 42],
            options: new DispatchOptions(idempotencyKey: 'workflow:42'),
        )->await();

        self::assertSame(42, $result->jobId);
        self::assertSame('workflow', $result->queueName);
        self::assertSame('workflow.start', $result->type);
        self::assertTrue($result->inserted);
        self::assertSame('workflow:42', $result->idempotencyKey);
    }
}
