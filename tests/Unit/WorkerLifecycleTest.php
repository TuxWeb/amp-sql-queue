<?php

declare(strict_types=1);

namespace AmpSqlQueue\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use AmpSqlQueue\Job;
use AmpSqlQueue\Worker;
use function Amp\async;

final class WorkerLifecycleTest extends \PHPUnit\Framework\TestCase
{
    public function testWorkerRefillsFreedConcurrencySlotBeforeSlowJobCompletes(): void
    {
        $slow = new DeferredFuture();
        $storage = new WorkerStorage([
            self::job(1),
            self::job(2),
            self::job(3),
        ]);
        $queue = new WorkerQueue($storage);

        $started = [];
        $worker = new Worker(
            queue: $queue,
            queueName: 'workflow',
            concurrency: 2,
            leaseSeconds: 60,
            handler: static function (Job $job) use (&$started, $slow): Future {
                $started[] = (int) $job->id;

                if ($job->id === 1) {
                    return $slow->getFuture();
                }

                return async(static fn (): null => null);
            },
            stopWhenIdle: true,
        );

        $future = async($worker->run(...));
        \Amp\delay(0.05);

        self::assertContains(3, $started);

        $slow->complete(null);
        $future->await();
    }

    public function testWorkerEmitsLifecycleEvents(): void
    {
        $storage = new WorkerStorage([self::job(10)]);
        $queue = new WorkerQueue($storage);
        $events = [];

        $worker = new Worker(
            queue: $queue,
            queueName: 'workflow',
            concurrency: 1,
            leaseSeconds: 60,
            handler: static fn (Job $job): Future => async(static fn (): null => null),
            stopWhenIdle: true,
        );
        $worker->addHook(static function (string $event, array $context) use (&$events): void {
            $events[] = $event;
        });

        $worker->run();

        self::assertSame([
            'worker.started',
            'job.reserved',
            'job.started',
            'job.succeeded',
            'job.acked',
            'worker.idle',
            'worker.stopped',
        ], $events);
    }

    private static function job(int $id): Job
    {
        return new Job(
            id: $id,
            queueName: 'workflow',
            type: 'workflow.start',
            payload: ['id' => $id],
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
    }
}
