<?php

declare(strict_types=1);

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use AmpSqlQueue\Job;
use AmpSqlQueue\Queue;
use AmpSqlQueue\Storage\PostgresQueueStorage;
use AmpSqlQueue\Worker;
use AmpSqlQueue\WorkerHookEvents;
use function Amp\async;

require __DIR__ . '/../vendor/autoload.php';

$pool = new PostgresConnectionPool(PostgresConfig::fromString(
    getenv('AMP_SQL_QUEUE_POSTGRES_DSN') ?: 'host=127.0.0.1 port=5432 user=amp_queue password=amp_queue dbname=amp_queue',
));

$queue = new Queue(new PostgresQueueStorage($pool));

$worker = new Worker(
    queue: $queue,
    queueName: 'ocr',
    concurrency: 4,
    leaseSeconds: 300,
    handler: static function (Job $job) {
        return async(static function () use ($job): void {
            fwrite(STDOUT, 'Processing job ' . $job->id . ' type ' . $job->type . PHP_EOL);
        });
    },
    stopWhenIdle: (bool) getenv('AMP_SQL_QUEUE_WORKER_STOP_WHEN_IDLE'),
);

$worker->addHook(static function (string $event, array $context): void {
    if ($event === WorkerHookEvents::JobFailed) {
        fwrite(STDERR, 'Job failed: ' . $context['job']->id . PHP_EOL);
    }
});

$worker->run();
