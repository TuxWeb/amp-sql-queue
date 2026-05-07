<?php

declare(strict_types=1);

namespace AmpSqlQueue;

final class WorkerHookEvents
{
    public const string WorkerStarted = 'worker.started';
    public const string WorkerIdle = 'worker.idle';
    public const string WorkerStopped = 'worker.stopped';
    public const string JobReserved = 'job.reserved';
    public const string JobStarted = 'job.started';
    public const string JobSucceeded = 'job.succeeded';
    public const string JobFailed = 'job.failed';
    public const string JobReleased = 'job.released';
    public const string JobDeadLettered = 'job.dead_lettered';
    public const string JobAcked = 'job.acked';
}
