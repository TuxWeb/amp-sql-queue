<?php

declare(strict_types=1);

namespace AmpSqlQueue\Storage;

use Amp\Future;
use AmpSqlQueue\DispatchResult;
use AmpSqlQueue\Job;

interface QueueStorageInterface
{
    /** @return Future<DispatchResult> */
    public function dispatch(StoredJobEnvelope $job): Future;

    /** @return Future<Job|null> */
    public function reserve(string $queueName, string $workerId, int $leaseSeconds): Future;

    /** @return Future<null> */
    public function ack(Job $job): Future;

    /** @return Future<null> */
    public function fail(Job $job, \Throwable $error, int $delaySeconds): Future;

    /** @return Future<null> */
    public function release(Job $job, int $delaySeconds): Future;

    /** @return Future<null> */
    public function cancel(Job $job): Future;

    /** @return Future<null> */
    public function extendLease(Job $job, int $leaseSeconds): Future;

    /** @return Future<null> */
    public function waitForJob(string $queueName, float $timeoutSeconds): Future;
}
