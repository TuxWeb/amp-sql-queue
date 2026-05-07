<?php

declare(strict_types=1);

namespace AmpSqlQueue\Storage;

use Amp\Future;
use Amp\Postgres\PostgresConnectionPool;
use AmpSqlQueue\DispatchResult;
use AmpSqlQueue\Internal\FutureFactory;
use AmpSqlQueue\Job;
use function Amp\delay;
use function Amp\Future\awaitFirst;

final class PostgresQueueStorage implements QueueStorageInterface
{
    use FutureFactory;

    private const string Channel = 'amp_queue_jobs';

    private JobMapper $mapper;

    public function __construct(private readonly PostgresConnectionPool $pool, ?JobMapper $mapper = null)
    {
        $this->mapper = $mapper ?? new JobMapper();
    }

    public function dispatch(StoredJobEnvelope $job): Future
    {
        return $this->future(function () use ($job): DispatchResult {
            $result = $this->pool->execute(
                <<<'SQL'
                INSERT INTO amp_queue_jobs (
                    queue_name, type, payload, status, priority, attempts, max_attempts,
                    available_at, idempotency_key, created_at, updated_at
                )
                VALUES (?, ?, ?::jsonb, 'pending', ?, 0, ?, NOW() + (?::text || ' seconds')::interval, ?, NOW(), NOW())
                ON CONFLICT (queue_name, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                RETURNING id
                SQL,
                [
                    $job->queueName,
                    $job->type,
                    $job->payload,
                    $job->priority,
                    $job->maxAttempts,
                    $job->delaySeconds,
                    $job->idempotencyKey,
                ],
            );

            $row = $result->fetchRow();
            $inserted = $row !== null;
            $jobId = self::normalizeJobId($row['id'] ?? null);

            if (!$inserted && $job->idempotencyKey !== null) {
                $existing = $this->pool->execute(
                    "SELECT id FROM amp_queue_jobs WHERE queue_name = ? AND idempotency_key = ?",
                    [$job->queueName, $job->idempotencyKey],
                )->fetchRow();
                $jobId = self::normalizeJobId($existing['id'] ?? null);
            }

            $this->pool->notify(self::Channel, $job->queueName);

            return new DispatchResult(
                jobId: $jobId,
                queueName: $job->queueName,
                type: $job->type,
                inserted: $inserted,
                idempotencyKey: $job->idempotencyKey,
            );
        });
    }

    public function reserve(string $queueName, string $workerId, int $leaseSeconds): Future
    {
        return $this->future(function () use ($queueName, $workerId, $leaseSeconds): ?Job {
            $this->recoverAbandoned($queueName);

            $result = $this->pool->execute(
                <<<'SQL'
                WITH candidate AS (
                    SELECT id
                    FROM amp_queue_jobs
                    WHERE queue_name = ?
                      AND status = 'pending'
                      AND available_at <= NOW()
                    ORDER BY priority DESC, available_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE amp_queue_jobs AS j
                SET status = 'processing',
                    locked_by = ?,
                    locked_until = NOW() + (?::text || ' seconds')::interval,
                    attempts = attempts + 1,
                    updated_at = NOW()
                FROM candidate
                WHERE j.id = candidate.id
                RETURNING j.*
                SQL,
                [$queueName, $workerId, $leaseSeconds],
            );

            $row = $result->fetchRow();

            return $row === null ? null : $this->mapper->fromRow($row);
        });
    }

    public function ack(Job $job): Future
    {
        return $this->future(function () use ($job): null {
            $this->pool->execute(
                "UPDATE amp_queue_jobs SET status = 'completed', locked_by = NULL, locked_until = NULL, completed_at = NOW(), updated_at = NOW() WHERE id = ?",
                [$job->id],
            );

            return null;
        });
    }

    public function fail(Job $job, \Throwable $error, int $delaySeconds): Future
    {
        return $this->future(function () use ($job, $error, $delaySeconds): null {
            $lastError = ErrorPayload::fromThrowable($error);
            $transaction = $this->pool->beginTransaction();

            try {
                if ($job->attempts >= $job->maxAttempts) {
                    $transaction->execute(
                        "UPDATE amp_queue_jobs SET status = 'failed', locked_by = NULL, locked_until = NULL, last_error = ?::jsonb, updated_at = NOW() WHERE id = ?",
                        [$lastError, $job->id],
                    );
                    $transaction->execute(
                        <<<'SQL'
                        INSERT INTO amp_queue_failed_jobs (
                            original_job_id, queue_name, type, payload, attempts, max_attempts, last_error, failed_at, created_at
                        )
                        VALUES (?, ?, ?, ?::jsonb, ?, ?, ?::jsonb, NOW(), ?)
                        SQL,
                        [
                            $job->id,
                            $job->queueName,
                            $job->type,
                            \json_encode($job->payload, JSON_THROW_ON_ERROR),
                            $job->attempts,
                            $job->maxAttempts,
                            $lastError,
                            $job->createdAt->format('Y-m-d H:i:s.uP'),
                        ],
                    );
                } else {
                    $transaction->execute(
                        "UPDATE amp_queue_jobs SET status = 'pending', locked_by = NULL, locked_until = NULL, last_error = ?::jsonb, available_at = NOW() + (?::text || ' seconds')::interval, updated_at = NOW() WHERE id = ?",
                        [$lastError, $delaySeconds, $job->id],
                    );
                }

                $transaction->commit();
            } catch (\Throwable $throwable) {
                if ($transaction->isActive()) {
                    $transaction->rollback();
                }

                throw $throwable;
            }

            if ($job->attempts < $job->maxAttempts) {
                $this->pool->notify(self::Channel, $job->queueName);
            }

            return null;
        });
    }

    public function release(Job $job, int $delaySeconds): Future
    {
        return $this->future(function () use ($job, $delaySeconds): null {
            $this->pool->execute(
                "UPDATE amp_queue_jobs SET status = 'pending', locked_by = NULL, locked_until = NULL, available_at = NOW() + (?::text || ' seconds')::interval, updated_at = NOW() WHERE id = ?",
                [$delaySeconds, $job->id],
            );
            $this->pool->notify(self::Channel, $job->queueName);

            return null;
        });
    }

    public function cancel(Job $job): Future
    {
        return $this->future(function () use ($job): null {
            $this->pool->execute(
                "UPDATE amp_queue_jobs SET status = 'cancelled', locked_by = NULL, locked_until = NULL, updated_at = NOW() WHERE id = ?",
                [$job->id],
            );

            return null;
        });
    }

    public function extendLease(Job $job, int $leaseSeconds): Future
    {
        return $this->future(function () use ($job, $leaseSeconds): null {
            $this->pool->execute(
                "UPDATE amp_queue_jobs SET locked_until = NOW() + (?::text || ' seconds')::interval, updated_at = NOW() WHERE id = ? AND status = 'processing'",
                [$leaseSeconds, $job->id],
            );

            return null;
        });
    }

    public function waitForJob(string $queueName, float $timeoutSeconds): Future
    {
        return $this->future(function () use ($queueName, $timeoutSeconds): null {
            $listener = $this->pool->listen(self::Channel);

            try {
                awaitFirst([
                    \Amp\async(function () use ($listener, $queueName): null {
                        foreach ($listener as $notification) {
                            if ($notification->payload === $queueName || $notification->payload === '') {
                                return null;
                            }
                        }

                        return null;
                    }),
                    \Amp\async(static function () use ($timeoutSeconds): null {
                        delay($timeoutSeconds);
                        return null;
                    }),
                ]);
            } finally {
                $listener->unlisten();
            }

            return null;
        });
    }

    private function recoverAbandoned(string $queueName): void
    {
        $this->pool->execute(
            "UPDATE amp_queue_jobs SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW() WHERE queue_name = ? AND status = 'processing' AND locked_until <= NOW()",
            [$queueName],
        );
    }

    private static function normalizeJobId(mixed $value): int|string|null
    {
        if (\is_int($value) || \is_string($value)) {
            return $value;
        }

        return null;
    }
}
