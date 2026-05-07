<?php

declare(strict_types=1);

namespace AmpSqlQueue\Tests\Unit;

use Amp\Future;
use AmpSqlQueue\DispatchResult;
use AmpSqlQueue\DispatchOptions;
use AmpSqlQueue\Internal\FutureFactory;
use AmpSqlQueue\Queue;
use AmpSqlQueue\Storage\QueueStorageInterface;
use AmpSqlQueue\Storage\StoredJobEnvelope;
use PHPUnit\Framework\TestCase;

final class QueueTest extends TestCase
{
    public function testDispatchSerializesAndDelegatesToStorage(): void
    {
        $storage = new RecordingStorage();
        $queue = new Queue($storage);

        $queue->dispatch(
            queue: 'ocr',
            type: 'document.ocr',
            payload: ['project_file_id' => 123],
            options: new DispatchOptions(priority: 10, maxAttempts: 5, idempotencyKey: 'ocr:123'),
        )->await();

        self::assertInstanceOf(StoredJobEnvelope::class, $storage->dispatched);
        self::assertSame('ocr', $storage->dispatched->queueName);
        self::assertSame('document.ocr', $storage->dispatched->type);
        self::assertSame('{"project_file_id":123}', $storage->dispatched->payload);
        self::assertSame(10, $storage->dispatched->priority);
        self::assertSame(5, $storage->dispatched->maxAttempts);
        self::assertSame('ocr:123', $storage->dispatched->idempotencyKey);
    }
}

class RecordingStorage implements QueueStorageInterface
{
    use FutureFactory;

    public ?StoredJobEnvelope $dispatched = null;
    public DispatchResult $dispatchResult;
    /** @var list<array{0:int,1:int}> */
    public array $extendedLeases = [];

    public function __construct()
    {
        $this->dispatchResult = new DispatchResult(
            jobId: 1,
            queueName: 'queue',
            type: 'type',
            inserted: true,
            idempotencyKey: null,
        );
    }

    public function dispatch(StoredJobEnvelope $job): Future
    {
        return $this->future(function () use ($job): DispatchResult {
            $this->dispatched = $job;

            return $this->dispatchResult;
        });
    }

    public function reserve(string $queueName, string $workerId, int $leaseSeconds): Future
    {
        return $this->future(static fn (): null => null);
    }

    public function ack(\AmpSqlQueue\Job $job): Future
    {
        return $this->future(static fn (): null => null);
    }

    public function fail(\AmpSqlQueue\Job $job, \Throwable $error, int $delaySeconds): Future
    {
        return $this->future(static fn (): null => null);
    }

    public function release(\AmpSqlQueue\Job $job, int $delaySeconds): Future
    {
        return $this->future(static fn (): null => null);
    }

    public function cancel(\AmpSqlQueue\Job $job): Future
    {
        return $this->future(static fn (): null => null);
    }

    public function extendLease(\AmpSqlQueue\Job $job, int $leaseSeconds): Future
    {
        return $this->future(function () use ($job, $leaseSeconds): null {
            $this->extendedLeases[] = [(int) $job->id, $leaseSeconds];

            return null;
        });
    }

    public function waitForJob(string $queueName, float $timeoutSeconds): Future
    {
        return $this->future(static fn (): null => null);
    }
}
