<?php

declare(strict_types=1);

namespace AmpSqlQueue\Storage;

use Amp\Future;
use Amp\Mysql\MysqlConnectionPool;
use AmpSqlQueue\DispatchResult;
use AmpSqlQueue\Internal\FutureFactory;
use AmpSqlQueue\Job;
use function Amp\delay;

final class MysqlQueueStorage implements QueueStorageInterface
{
    use FutureFactory;

    private JobMapper $mapper;
    private float $pollDelaySeconds;

    public function __construct(
        private readonly MysqlConnectionPool $pool,
        ?JobMapper $mapper = null,
        private readonly float $minPollDelaySeconds = 0.05,
        private readonly float $maxPollDelaySeconds = 2.0,
    ) {
        $this->mapper = $mapper ?? new JobMapper();
        $this->pollDelaySeconds = $this->minPollDelaySeconds;
    }

    public function dispatch(StoredJobEnvelope $job): Future
    {
        return $this->future(function () use ($job): DispatchResult {
            $result = $this->pool->execute(
                <<<'SQL'
                INSERT IGNORE INTO amp_queue_jobs (
                    queue_name, type, payload, status, priority, attempts, max_attempts,
                    available_at, idempotency_key, created_at, updated_at
                )
                VALUES (?, ?, ?, 'pending', ?, 0, ?, DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND), ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
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

            $inserted = $result->getRowCount() > 0;
            $jobId = self::normalizeJobId($result->getLastInsertId());

            if (!$inserted && $job->idempotencyKey !== null) {
                $existing = $this->pool->execute(
                    "SELECT id FROM amp_queue_jobs WHERE queue_name = ? AND idempotency_key = ?",
                    [$job->queueName, $job->idempotencyKey],
                )->fetchRow();
                $jobId = self::normalizeJobId($existing['id'] ?? null);
            }

            $this->resetPollDelay();

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
            $transaction = $this->pool->beginTransaction();

            try {
                $result = $transaction->execute(
                    <<<'SQL'
                    SELECT id
                    FROM amp_queue_jobs
                    WHERE queue_name = ?
                      AND status = 'pending'
                      AND available_at <= CURRENT_TIMESTAMP(6)
                    ORDER BY priority DESC, available_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    SQL,
                    [$queueName],
                );
                $candidate = $result->fetchRow();

                if ($candidate === null) {
                    $transaction->commit();
                    return null;
                }

                $transaction->execute(
                    <<<'SQL'
                    UPDATE amp_queue_jobs
                    SET status = 'processing',
                        locked_by = ?,
                        locked_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND),
                        attempts = attempts + 1,
                        updated_at = CURRENT_TIMESTAMP(6)
                    WHERE id = ?
                    SQL,
                    [$workerId, $leaseSeconds, $candidate['id']],
                );

                $jobResult = $transaction->execute('SELECT * FROM amp_queue_jobs WHERE id = ?', [$candidate['id']]);
                $row = $jobResult->fetchRow();
                $transaction->commit();
            } catch (\Throwable $throwable) {
                if ($transaction->isActive()) {
                    $transaction->rollback();
                }

                throw $throwable;
            }

            $this->resetPollDelay();

            return $row === null ? null : $this->mapper->fromRow($row);
        });
    }

    public function ack(Job $job): Future
    {
        return $this->future(function () use ($job): null {
            $this->pool->execute(
                "UPDATE amp_queue_jobs SET status = 'completed', locked_by = NULL, locked_until = NULL, completed_at = CURRENT_TIMESTAMP(6), updated_at = CURRENT_TIMESTAMP(6) WHERE id = ?",
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
                        "UPDATE amp_queue_jobs SET status = 'failed', locked_by = NULL, locked_until = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP(6) WHERE id = ?",
                        [$lastError, $job->id],
                    );
                    $transaction->execute(
                        <<<'SQL'
                        INSERT INTO amp_queue_failed_jobs (
                            original_job_id, queue_name, type, payload, attempts, max_attempts, last_error, failed_at, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(6), ?)
                        SQL,
                        [
                            $job->id,
                            $job->queueName,
                            $job->type,
                            \json_encode($job->payload, JSON_THROW_ON_ERROR),
                            $job->attempts,
                            $job->maxAttempts,
                            $lastError,
                            $job->createdAt->format('Y-m-d H:i:s.u'),
                        ],
                    );
                } else {
                    $transaction->execute(
                        "UPDATE amp_queue_jobs SET status = 'pending', locked_by = NULL, locked_until = NULL, last_error = ?, available_at = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND), updated_at = CURRENT_TIMESTAMP(6) WHERE id = ?",
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

            $this->resetPollDelay();

            return null;
        });
    }

    public function release(Job $job, int $delaySeconds): Future
    {
        return $this->future(function () use ($job, $delaySeconds): null {
            $this->pool->execute(
                "UPDATE amp_queue_jobs SET status = 'pending', locked_by = NULL, locked_until = NULL, available_at = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND), updated_at = CURRENT_TIMESTAMP(6) WHERE id = ?",
                [$delaySeconds, $job->id],
            );
            $this->resetPollDelay();

            return null;
        });
    }

    public function cancel(Job $job): Future
    {
        return $this->future(function () use ($job): null {
            $this->pool->execute(
                "UPDATE amp_queue_jobs SET status = 'cancelled', locked_by = NULL, locked_until = NULL, updated_at = CURRENT_TIMESTAMP(6) WHERE id = ?",
                [$job->id],
            );

            return null;
        });
    }

    public function extendLease(Job $job, int $leaseSeconds): Future
    {
        return $this->future(function () use ($job, $leaseSeconds): null {
            $this->pool->execute(
                "UPDATE amp_queue_jobs SET locked_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND), updated_at = CURRENT_TIMESTAMP(6) WHERE id = ? AND status = 'processing'",
                [$leaseSeconds, $job->id],
            );

            return null;
        });
    }

    public function waitForJob(string $queueName, float $timeoutSeconds): Future
    {
        return $this->future(function () use ($timeoutSeconds): null {
            $delay = \min($timeoutSeconds, $this->pollDelaySeconds);
            delay($delay);
            $this->pollDelaySeconds = \min($this->maxPollDelaySeconds, $this->pollDelaySeconds * 2.0);

            return null;
        });
    }

    private function recoverAbandoned(string $queueName): void
    {
        $this->pool->execute(
            "UPDATE amp_queue_jobs SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = CURRENT_TIMESTAMP(6) WHERE queue_name = ? AND status = 'processing' AND locked_until <= CURRENT_TIMESTAMP(6)",
            [$queueName],
        );
    }

    private function resetPollDelay(): void
    {
        $this->pollDelaySeconds = $this->minPollDelaySeconds;
    }

    private static function normalizeJobId(mixed $value): int|string|null
    {
        if (\is_int($value) || \is_string($value)) {
            return $value;
        }

        return null;
    }
}
