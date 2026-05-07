<?php

declare(strict_types=1);

namespace AmpSqlQueue\Tests\Integration;

use Amp\Mysql\MysqlConfig;
use Amp\Mysql\MysqlConnectionPool;
use Amp\Future;
use AmpSqlQueue\DispatchOptions;
use AmpSqlQueue\Job;
use AmpSqlQueue\Queue;
use AmpSqlQueue\Worker;
use AmpSqlQueue\Storage\MysqlQueueStorage;
use PHPUnit\Framework\TestCase;
use function Amp\async;
use function Amp\delay;

final class MysqlQueueStorageTest extends TestCase
{
    private MysqlConnectionPool $pool;

    protected function setUp(): void
    {
        $dsn = getenv('AMP_SQL_QUEUE_MYSQL_DSN');
        if ($dsn === false || $dsn === '') {
            self::markTestSkipped('AMP_SQL_QUEUE_MYSQL_DSN is not set.');
        }

        $this->pool = new MysqlConnectionPool(MysqlConfig::fromString($dsn));
        $this->pool->query('DROP TABLE IF EXISTS amp_queue_failed_jobs');
        $this->pool->query('DROP TABLE IF EXISTS amp_queue_jobs');
        $this->pool->query((string) file_get_contents(__DIR__ . '/../../migrations/mysql/001_create_queue_tables.sql'));
    }

    public function testDispatchReserveAndAck(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));

        $queue->dispatch('ocr', 'document.ocr', ['project_file_id' => 123], new DispatchOptions(priority: 10))->await();

        $job = $queue->storage()->reserve('ocr', 'worker-1', 60)->await();

        self::assertNotNull($job);
        self::assertSame(['project_file_id' => 123], $job->payload);
        self::assertSame(1, $job->attempts);

        $queue->storage()->ack($job)->await();

        $row = $this->pool->execute('SELECT status FROM amp_queue_jobs WHERE id = ?', [$job->id])->fetchRow();
        self::assertSame('completed', $row['status'] ?? null);
    }

    public function testIdempotencyKeyPreventsDuplicateDispatch(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $options = new DispatchOptions(idempotencyKey: 'ocr:123');

        $queue->dispatch('ocr', 'document.ocr', ['project_file_id' => 123], $options)->await();
        $queue->dispatch('ocr', 'document.ocr', ['project_file_id' => 123], $options)->await();

        $row = $this->pool->query('SELECT COUNT(*) AS total FROM amp_queue_jobs')->fetchRow();
        self::assertSame(1, (int) ($row['total'] ?? 0));
    }

    public function testDispatchResultReportsInsertedAndDeduplicatedJobs(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $options = new DispatchOptions(idempotencyKey: 'workflow:123');

        $first = $queue->dispatch('workflow', 'workflow.start', ['id' => 123], $options)->await();
        $second = $queue->dispatch('workflow', 'workflow.start', ['id' => 123], $options)->await();

        self::assertTrue($first->inserted);
        self::assertFalse($second->inserted);
        self::assertNotNull($first->jobId);
        self::assertSame((string) $first->jobId, (string) $second->jobId);
        self::assertSame('workflow:123', $second->idempotencyKey);
    }

    public function testDelayedJobIsNotReservedBeforeAvailableAt(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $queue->dispatch('workflow', 'workflow.resume', ['id' => 1], new DispatchOptions(delaySeconds: 60))->await();

        self::assertNull($queue->storage()->reserve('workflow', 'worker-1', 60)->await());
    }

    public function testPriorityOrdersAvailableJobs(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $queue->dispatch('workflow', 'low', ['rank' => 'low'], new DispatchOptions(priority: 1))->await();
        $queue->dispatch('workflow', 'high', ['rank' => 'high'], new DispatchOptions(priority: 50))->await();

        $job = $queue->storage()->reserve('workflow', 'worker-1', 60)->await();

        self::assertNotNull($job);
        self::assertSame('high', $job->type);
        self::assertSame(['rank' => 'high'], $job->payload);
    }

    public function testConcurrentReservationsDoNotDuplicateJobsAndSkippedJobRemainsReservable(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $queue->dispatch('workflow', 'one', ['id' => 1])->await();
        $queue->dispatch('workflow', 'two', ['id' => 2])->await();

        $jobs = Future\await([
            async(fn () => $queue->storage()->reserve('workflow', 'worker-1', 60)->await()),
            async(fn () => $queue->storage()->reserve('workflow', 'worker-2', 60)->await()),
        ]);

        $reserved = array_values(array_filter($jobs, static fn (mixed $job): bool => $job instanceof Job));

        self::assertNotSame([], $reserved);
        self::assertCount(count(array_unique(array_map(static fn (Job $job): string => (string) $job->id, $reserved))), $reserved);

        $next = $queue->storage()->reserve('workflow', 'worker-3', 60)->await();
        self::assertNotNull($next);
        self::assertNotContains((string) $next->id, array_map(static fn (Job $job): string => (string) $job->id, $reserved));
    }

    public function testFailRequeuesJobWithDelayUntilMaxAttempts(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $queue->dispatch('workflow', 'workflow.start', ['id' => 1], new DispatchOptions(maxAttempts: 3))->await();

        $job = $queue->storage()->reserve('workflow', 'worker-1', 60)->await();
        self::assertNotNull($job);

        $queue->storage()->fail($job, new \RuntimeException('temporary'), 30)->await();

        $row = $this->pool->execute(
            "SELECT status, attempts, JSON_UNQUOTE(JSON_EXTRACT(last_error, '$.message')) AS message, available_at > CURRENT_TIMESTAMP(6) AS is_delayed FROM amp_queue_jobs WHERE id = ?",
            [$job->id],
        )->fetchRow();

        self::assertSame('pending', $row['status'] ?? null);
        self::assertSame(1, (int) ($row['attempts'] ?? 0));
        self::assertSame('temporary', $row['message'] ?? null);
        self::assertSame(1, (int) ($row['is_delayed'] ?? 0));
    }

    public function testFailMovesJobToDeadLetterWhenAttemptsAreExhausted(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $queue->dispatch('workflow', 'workflow.start', ['id' => 1], new DispatchOptions(maxAttempts: 1))->await();

        $job = $queue->storage()->reserve('workflow', 'worker-1', 60)->await();
        self::assertNotNull($job);

        $queue->storage()->fail($job, new \RuntimeException('permanent'), 0)->await();

        $jobRow = $this->pool->execute('SELECT status FROM amp_queue_jobs WHERE id = ?', [$job->id])->fetchRow();
        $failedRow = $this->pool->execute(
            "SELECT original_job_id, attempts, JSON_UNQUOTE(JSON_EXTRACT(last_error, '$.message')) AS message FROM amp_queue_failed_jobs WHERE original_job_id = ?",
            [$job->id],
        )->fetchRow();

        self::assertSame('failed', $jobRow['status'] ?? null);
        self::assertIsArray($failedRow);
        self::assertSame((string) $job->id, (string) self::scalar($failedRow['original_job_id'] ?? null));
        self::assertSame(1, (int) ($failedRow['attempts'] ?? 0));
        self::assertSame('permanent', $failedRow['message'] ?? null);
    }

    public function testExpiredLeaseIsRecoveredForReservation(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $queue->dispatch('workflow', 'workflow.start', ['id' => 1])->await();

        $job = $queue->storage()->reserve('workflow', 'worker-1', 1)->await();
        self::assertNotNull($job);

        $this->pool->execute(
            "UPDATE amp_queue_jobs SET locked_until = DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL 1 SECOND) WHERE id = ?",
            [$job->id],
        );

        $recovered = $queue->storage()->reserve('workflow', 'worker-2', 60)->await();

        self::assertNotNull($recovered);
        self::assertSame((string) $job->id, (string) $recovered->id);
        self::assertSame('worker-2', $recovered->lockedBy);
        self::assertSame(2, $recovered->attempts);
    }

    public function testExtendLeaseKeepsProcessingJobLockedLonger(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $queue->dispatch('workflow', 'workflow.start', ['id' => 1])->await();

        $job = $queue->storage()->reserve('workflow', 'worker-1', 1)->await();
        self::assertNotNull($job);

        delay(1.1);
        $queue->storage()->extendLease($job, 60)->await();

        self::assertNull($queue->storage()->reserve('workflow', 'worker-2', 60)->await());

        $row = $this->pool->execute(
            "SELECT status, locked_by, locked_until > CURRENT_TIMESTAMP(6) AS locked FROM amp_queue_jobs WHERE id = ?",
            [$job->id],
        )->fetchRow();

        self::assertSame('processing', $row['status'] ?? null);
        self::assertSame('worker-1', $row['locked_by'] ?? null);
        self::assertSame(1, (int) ($row['locked'] ?? 0));
    }

    public function testWorkerRetriesWithBackoffAndEventuallyAcknowledges(): void
    {
        $queue = new Queue(new MysqlQueueStorage($this->pool));
        $queue->dispatch('workflow', 'workflow.start', ['id' => 1], new DispatchOptions(maxAttempts: 3))->await();

        $calls = 0;
        $worker = new Worker(
            queue: $queue,
            queueName: 'workflow',
            concurrency: 1,
            leaseSeconds: 60,
            handler: static function (Job $job) use (&$calls): Future {
                return async(static function () use (&$calls): null {
                    $calls++;
                    if ($calls === 1) {
                        throw new \RuntimeException('retry me');
                    }

                    return null;
                });
            },
            backoffStrategy: new \AmpSqlQueue\Backoff\ExponentialBackoffStrategy(baseDelaySeconds: 0),
            stopWhenIdle: true,
        );

        $worker->run();

        $row = $this->pool->query('SELECT status, attempts FROM amp_queue_jobs')->fetchRow();

        self::assertSame(2, $calls);
        self::assertSame('completed', $row['status'] ?? null);
        self::assertSame(2, (int) ($row['attempts'] ?? 0));
    }

    private static function scalar(mixed $value): int|string|float|bool|null
    {
        if (\is_int($value) || \is_string($value) || \is_float($value) || \is_bool($value) || $value === null) {
            return $value;
        }

        throw new \UnexpectedValueException('Expected scalar value.');
    }
}
