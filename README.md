# amp-sql-queue

Persistent, async, non-blocking SQL job queue for PHP 8.3+, AMPHP, Revolt, PostgreSQL, MySQL, and MariaDB.

`amp-sql-queue` provides one queue API over two SQL storage backends:

- `PostgresQueueStorage` using `amphp/postgres`
- `MysqlQueueStorage` using `amphp/mysql`

It is designed as a small library, not a framework integration. There is no PDO, Doctrine DBAL, Symfony, Laravel, or blocking database client in the runtime path.

## Status

This project is an early open source base. The core API, worker, SQL storage classes, migrations, examples, PHPUnit tests, and PHPStan configuration are present. Database-backed integration tests are available and run when PostgreSQL/MySQL DSNs are configured.

## Features

- One public API for PostgreSQL, MySQL, and MariaDB
- Real async/non-blocking database access through AMPHP
- Persistent SQL-backed jobs
- Atomic reservation with `SELECT ... FOR UPDATE SKIP LOCKED`
- Delayed jobs via `available_at`
- Priority ordering
- Lease / visibility timeout via `locked_until`
- Manual lease extension for long-running jobs
- Recovery of abandoned jobs
- Retry with exponential backoff
- Dead letter table
- Optional idempotency key
- Dispatch receipts with job id and insert/deduplication status
- Worker lifecycle hooks for observability
- Concurrent workers in the same PHP process
- Multiple worker processes through SQL row locking
- PostgreSQL `LISTEN/NOTIFY` with polling fallback
- MySQL adaptive polling
- JSON serializer by default
- Extensible contracts
- Typed exceptions
- Separate PostgreSQL and MySQL migrations

## Requirements

- PHP 8.3+
- Composer
- PostgreSQL 12+ recommended, with `FOR UPDATE SKIP LOCKED`
- MySQL 8.0+ or MariaDB 10.6+ required for `FOR UPDATE SKIP LOCKED`

Core runtime dependencies:

- `amphp/amp`
- `revolt/event-loop`

Install the database driver you need:

- `amphp/postgres` for `PostgresQueueStorage`
- `amphp/mysql` for `MysqlQueueStorage` with MySQL or MariaDB

Development dependencies:

- PHPUnit
- PHPStan

## Installation

```bash
composer require tuxweb/amp-sql-queue amphp/postgres
```

For MySQL or MariaDB:

```bash
composer require tuxweb/amp-sql-queue amphp/mysql
```

For local development in this repository:

```bash
composer install
```

## Database Schema

Apply the migration for your database.

PostgreSQL:

```bash
psql "$AMP_SQL_QUEUE_POSTGRES_DSN" -f migrations/postgres/001_create_queue_tables.sql
```

MySQL / MariaDB:

```bash
mysql "$AMP_SQL_QUEUE_MYSQL_DSN" < migrations/mysql/001_create_queue_tables.sql
```

The logical schema uses two tables:

- `amp_queue_jobs`
- `amp_queue_failed_jobs`

Important fields include:

- `queue_name`
- `type`
- `payload`
- `status`
- `priority`
- `attempts`
- `max_attempts`
- `available_at`
- `locked_until`
- `locked_by`
- `last_error`
- `idempotency_key`
- `created_at`
- `updated_at`
- `completed_at`

Supported statuses:

- `pending`
- `processing`
- `completed`
- `failed`
- `cancelled`

## Dispatching Jobs

PostgreSQL example:

```php
<?php

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnectionPool;
use AmpSqlQueue\DispatchOptions;
use AmpSqlQueue\Queue;
use AmpSqlQueue\Storage\PostgresQueueStorage;
use function Amp\Future\await;

$pool = new PostgresConnectionPool(PostgresConfig::fromString(
    'host=127.0.0.1 port=5432 user=amp_queue password=amp_queue dbname=amp_queue',
));

$queue = new Queue(new PostgresQueueStorage($pool));

$result = $queue->dispatch(
    queue: 'ocr',
    type: 'document.ocr',
    payload: [
        'project_file_id' => 123,
    ],
    options: new DispatchOptions(
        delaySeconds: 0,
        priority: 10,
        maxAttempts: 5,
        idempotencyKey: 'ocr:123',
    ),
)->await();

var_dump($result->jobId, $result->inserted);
```

MySQL / MariaDB use the same queue API:

```php
<?php

use Amp\Mysql\MysqlConfig;
use Amp\Mysql\MysqlConnectionPool;
use AmpSqlQueue\Queue;
use AmpSqlQueue\Storage\MysqlQueueStorage;

$pool = new MysqlConnectionPool(MysqlConfig::fromString(
    'host=127.0.0.1 user=amp_queue password=amp_queue db=amp_queue',
));

$queue = new Queue(new MysqlQueueStorage($pool));
```

## Running Workers

```php
<?php

use Amp\Future;
use AmpSqlQueue\Job;
use AmpSqlQueue\Worker;
use function Amp\async;

$worker = new Worker(
    queue: $queue,
    queueName: 'ocr',
    concurrency: 4,
    leaseSeconds: 300,
    handler: static function (Job $job): Future {
        return async(static function () use ($job): void {
            // Do real async work here.
            // Use AMPHP-compatible clients for I/O.
        });
    },
);

$worker->run();
```

The worker:

- reserves jobs from the configured queue
- runs up to `concurrency` jobs at once
- acknowledges successful jobs
- fails unsuccessful jobs
- computes retry delay through `BackoffStrategyInterface`
- emits lifecycle hooks for observability
- stops accepting new jobs after `SIGINT` or `SIGTERM`
- waits for active jobs before exiting

## Worker Hooks

Worker hooks are optional and framework-agnostic. Use them to bridge queue activity into logs,
metrics, tracing, or a workflow runtime event stream.

```php
use AmpSqlQueue\WorkerHookEvents;

$worker->addHook(static function (string $event, array $context): void {
    if ($event === WorkerHookEvents::JobFailed) {
        error_log('Job failed: ' . $context['job']->id);
    }
});
```

Emitted events include:

- `worker.started`
- `worker.idle`
- `worker.stopped`
- `job.reserved`
- `job.started`
- `job.succeeded`
- `job.failed`
- `job.released`
- `job.dead_lettered`
- `job.acked`

## Long-Running Jobs

Workers reserve jobs with a lease. If a job may run longer than the lease, extend it from the
handler before it expires:

```php
$queue->storage()->extendLease($job, 300)->await();
```

For workflows that pause for external input, do not hold the lease while waiting. Persist the
workflow interrupt, acknowledge the current job, and dispatch a later resume job when input arrives.

## Retry and Backoff

`maxAttempts` is stored on the job at dispatch time. This makes retry behavior durable across worker restarts and multiple processes.

```php
new DispatchOptions(
    maxAttempts: 5,
);
```

The worker owns the delay policy. By default it uses `ExponentialBackoffStrategy`.

```php
use AmpSqlQueue\Backoff\ExponentialBackoffStrategy;

$worker = new Worker(
    queue: $queue,
    queueName: 'ocr',
    concurrency: 4,
    leaseSeconds: 300,
    handler: $handler,
    backoffStrategy: new ExponentialBackoffStrategy(
        baseDelaySeconds: 2,
        maxDelaySeconds: 300,
        multiplier: 2.0,
        jitterRatio: 0.1,
    ),
);
```

When attempts are exhausted, the job is marked `failed` and copied into `amp_queue_failed_jobs`.

## Idempotency

Set an idempotency key to avoid duplicate dispatches for the same queue:

```php
new DispatchOptions(
    idempotencyKey: 'ocr:123',
);
```

The uniqueness scope is:

```text
queue_name + idempotency_key
```

`NULL` idempotency keys are allowed, so jobs without a key are never deduplicated.

## Storage Behavior

### PostgreSQL

PostgreSQL uses:

- `JSONB` payloads
- partial unique index for optional idempotency
- `FOR UPDATE SKIP LOCKED`
- `UPDATE ... RETURNING`
- `LISTEN/NOTIFY` to wake waiting workers
- polling fallback when notifications are missed or unavailable

### MySQL / MariaDB

MySQL and MariaDB use:

- `JSON` payloads
- InnoDB row locks
- explicit transactions for reserve
- `FOR UPDATE SKIP LOCKED`
- adaptive polling because MySQL/MariaDB have no direct equivalent to PostgreSQL `LISTEN/NOTIFY`

MariaDB 11.8 is covered by the integration suite in this workspace. Under concurrent reservation,
MariaDB may return one job and one empty reservation while another pending job remains available;
the next reservation picks it up. This preserves safety while PostgreSQL shows stronger immediate
parallel reservation behavior.

Both storage classes expose the same public API.

## Public Contracts

The main extension points are:

- `AmpSqlQueue\QueueInterface`
- `AmpSqlQueue\Storage\QueueStorageInterface`
- `AmpSqlQueue\Handler\JobHandlerInterface`
- `AmpSqlQueue\Backoff\BackoffStrategyInterface`
- `AmpSqlQueue\Serialization\SerializerInterface`

Default implementations:

- `AmpSqlQueue\Queue`
- `AmpSqlQueue\Worker`
- `AmpSqlQueue\Backoff\ExponentialBackoffStrategy`
- `AmpSqlQueue\Serialization\JsonSerializer`
- `AmpSqlQueue\Storage\PostgresQueueStorage`
- `AmpSqlQueue\Storage\MysqlQueueStorage`

## Examples

Example scripts are available in `examples/`:

- `examples/dispatch.php`
- `examples/worker.php`

Run them after applying migrations and setting the database DSN:

```bash
export AMP_SQL_QUEUE_POSTGRES_DSN='host=127.0.0.1 port=5432 user=amp_queue password=amp_queue dbname=amp_queue'
php examples/dispatch.php
php examples/worker.php
```

## Development

Install dependencies:

```bash
composer install
```

Run checks:

```bash
composer validate --strict
vendor/bin/phpunit
vendor/bin/phpstan analyse
```

Run only unit tests:

```bash
vendor/bin/phpunit --testsuite Unit
```

## Integration Tests

Integration tests require real PostgreSQL and MySQL/MariaDB databases. The included Docker Compose file is only for development and CI convenience; it is not used by the library at runtime.

Start databases:

```bash
docker compose up -d postgres mysql
```

Set DSNs:

```bash
export AMP_SQL_QUEUE_POSTGRES_DSN='host=127.0.0.1 port=5432 user=amp_queue password=amp_queue dbname=amp_queue'
export AMP_SQL_QUEUE_MYSQL_DSN='host=127.0.0.1 port=3306 user=amp_queue password=amp_queue db=amp_queue'
```

Run integration tests:

```bash
vendor/bin/phpunit --testsuite Integration
```

If the DSNs are not set, integration tests are skipped.

## Design Notes

This package deliberately keeps classes small and avoids framework-specific behavior. The queue facade handles API-level concerns. Storage classes handle database-specific SQL. The worker handles concurrency, retry, and graceful shutdown.

Use AMPHP-compatible libraries inside handlers. Blocking calls in a handler will block the event loop and reduce concurrency.

## License

MIT. See `LICENSE`.
