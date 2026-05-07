<?php

declare(strict_types=1);

namespace AmpSqlQueue\Tests\Unit;

use Amp\Future;
use AmpSqlQueue\DispatchResult;
use AmpSqlQueue\Backoff\ExponentialBackoffStrategy;
use AmpSqlQueue\Job;
use AmpSqlQueue\Internal\FutureFactory;
use AmpSqlQueue\QueueInterface;
use AmpSqlQueue\Storage\QueueStorageInterface;
use AmpSqlQueue\Worker;
use PHPUnit\Framework\TestCase;
use function Amp\async;

final class WorkerTest extends TestCase
{
    public function testWorkerAcksSuccessfulJob(): void
    {
        $job = self::job(id: 1, attempts: 1);
        $storage = new WorkerStorage([$job]);
        $queue = new WorkerQueue($storage);

        $worker = new Worker(
            queue: $queue,
            queueName: 'ocr',
            concurrency: 1,
            leaseSeconds: 60,
            handler: static fn (Job $job): Future => async(static fn () => null),
            stopWhenIdle: true,
        );

        $worker->run();

        self::assertSame([1], $storage->ackedIds);
    }

    public function testWorkerFailsJobWithBackoffDelay(): void
    {
        $job = self::job(id: 2, attempts: 2);
        $storage = new WorkerStorage([$job]);
        $queue = new WorkerQueue($storage);

        $worker = new Worker(
            queue: $queue,
            queueName: 'ocr',
            concurrency: 1,
            leaseSeconds: 60,
            handler: static fn (Job $job): Future => async(static fn () => throw new \RuntimeException('boom')),
            backoffStrategy: new ExponentialBackoffStrategy(baseDelaySeconds: 3, maxDelaySeconds: 30, multiplier: 2.0),
            stopWhenIdle: true,
        );

        $worker->run();

        self::assertSame([[2, 6]], $storage->failed);
    }

    private static function job(int $id, int $attempts): Job
    {
        return new Job(
            id: $id,
            queueName: 'ocr',
            type: 'document.ocr',
            payload: ['project_file_id' => $id],
            status: 'processing',
            priority: 0,
            attempts: $attempts,
            maxAttempts: 5,
            availableAt: new \DateTimeImmutable(),
            lockedUntil: new \DateTimeImmutable('+60 seconds'),
            lockedBy: 'worker-1',
            lastError: null,
            idempotencyKey: null,
            createdAt: new \DateTimeImmutable(),
            updatedAt: new \DateTimeImmutable(),
            completedAt: null,
        );
    }
}

final class WorkerQueue implements QueueInterface
{
    use FutureFactory;

    public function __construct(private readonly QueueStorageInterface $storage)
    {
    }

    public function dispatch(string $queue, string $type, array $payload, \AmpSqlQueue\DispatchOptions $options = new \AmpSqlQueue\DispatchOptions()): Future
    {
        return $this->future(static fn (): DispatchResult => new DispatchResult(
            jobId: 1,
            queueName: $queue,
            type: $type,
            inserted: true,
            idempotencyKey: $options->idempotencyKey,
        ));
    }

    public function storage(): QueueStorageInterface
    {
        return $this->storage;
    }
}

final class WorkerStorage extends RecordingStorage
{
    /** @param list<Job> $jobs */
    public function __construct(private array $jobs)
    {
    }

    /** @var list<int> */
    public array $ackedIds = [];

    /** @var list<array{0:int,1:int}> */
    public array $failed = [];

    public function reserve(string $queueName, string $workerId, int $leaseSeconds): Future
    {
        return $this->future(function (): ?Job {
            return array_shift($this->jobs);
        });
    }

    public function ack(Job $job): Future
    {
        return $this->future(function () use ($job): null {
            $this->ackedIds[] = (int) $job->id;

            return null;
        });
    }

    public function fail(Job $job, \Throwable $error, int $delaySeconds): Future
    {
        return $this->future(function () use ($job, $delaySeconds): null {
            $this->failed[] = [(int) $job->id, $delaySeconds];

            return null;
        });
    }
}
