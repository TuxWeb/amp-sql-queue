<?php

declare(strict_types=1);

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use AmpSqlQueue\DispatchOptions;
use AmpSqlQueue\Queue;
use AmpSqlQueue\Storage\PostgresQueueStorage;

require __DIR__ . '/../vendor/autoload.php';

$pool = new PostgresConnectionPool(PostgresConfig::fromString(
    getenv('AMP_SQL_QUEUE_POSTGRES_DSN') ?: 'host=127.0.0.1 port=5432 user=amp_queue password=amp_queue dbname=amp_queue',
));

$queue = new Queue(new PostgresQueueStorage($pool));

$result = $queue->dispatch(
    queue: 'ocr',
    type: 'document.ocr',
    payload: ['project_file_id' => 123],
    options: new DispatchOptions(
        delaySeconds: 0,
        priority: 10,
        maxAttempts: 5,
        idempotencyKey: 'ocr:123',
    ),
)->await();

fwrite(
    STDOUT,
    sprintf(
        "Dispatched job %s (%s)\n",
        (string) $result->jobId,
        $result->inserted ? 'inserted' : 'already existed',
    ),
);
