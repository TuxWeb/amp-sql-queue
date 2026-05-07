# amp-sql-queue Project Memory

## Project Goal

Build `tuxweb/amp-sql-queue`, an open source PHP 8.3+ library for a persistent, async, non-blocking SQL job queue compatible with PostgreSQL, MySQL, and MariaDB through one public API.

The library must use AMPHP and Revolt, avoid blocking database clients, and remain framework-agnostic.

## Runtime Constraints

- PHP 8.3+
- `amphp/amp`
- `amphp/postgres` is optional at install time and required only for `PostgresQueueStorage`.
- `amphp/mysql` is optional at install time and required only for `MysqlQueueStorage` with MySQL or MariaDB.
- `revolt/event-loop`
- Composer
- PSR-4
- No PDO
- No Doctrine DBAL
- No Symfony or Laravel dependency
- No blocking database access in runtime code

## Core Architecture

Main public classes and contracts:

- `AmpSqlQueue\QueueInterface`
- `AmpSqlQueue\Queue`
- `AmpSqlQueue\DispatchResult`
- `AmpSqlQueue\Worker`
- `AmpSqlQueue\WorkerHookInterface`
- `AmpSqlQueue\WorkerHookEvents`
- `AmpSqlQueue\Storage\QueueStorageInterface`
- `AmpSqlQueue\Storage\PostgresQueueStorage`
- `AmpSqlQueue\Storage\MysqlQueueStorage`
- `AmpSqlQueue\Handler\JobHandlerInterface`
- `AmpSqlQueue\Backoff\BackoffStrategyInterface`
- `AmpSqlQueue\Backoff\ExponentialBackoffStrategy`
- `AmpSqlQueue\Serialization\SerializerInterface`
- `AmpSqlQueue\Serialization\JsonSerializer`

`Queue` is a small facade. It validates dispatch input, serializes payloads, and delegates to storage. `dispatch()` returns `Future<DispatchResult>` so callers can see the job id, whether a new row was inserted, and whether an idempotency key caused deduplication.

`Worker` owns processing concurrency, retry/backoff, graceful shutdown, and calls storage for reserve/ack/fail/release. It refills freed concurrency slots as individual jobs finish instead of waiting for the full active batch. Optional hooks can be registered with `addHook()` for observability.

Storage implementations own database-specific SQL.

## Design Decisions

- Payloads are JSON by default.
- PostgreSQL uses `JSONB`.
- MySQL/MariaDB use `JSON`.
- `max_attempts` belongs to the job and is stored in `amp_queue_jobs`.
- Retry delay policy belongs to the worker via `BackoffStrategyInterface`.
- `fail(Job $job, Throwable $error, int $delaySeconds)` receives the delay already computed by the worker.
- `dispatch()` returns a `DispatchResult` with `jobId`, `queueName`, `type`, `inserted`, and `idempotencyKey`.
- Idempotency is optional and scoped by `queue_name + idempotency_key`.
- Jobs without an idempotency key are not deduplicated.
- Dead letter jobs are copied into `amp_queue_failed_jobs` when attempts are exhausted.
- Atomic reservation must use `SELECT ... FOR UPDATE SKIP LOCKED`.
- Long-running handlers can call `extendLease(Job $job, int $leaseSeconds)` to move `locked_until` forward while a job is still processing.
- Jobs waiting on external/human input should not hold a lease; persist external state, acknowledge/release the current job, and dispatch a later resume job.
- Worker hooks are framework-agnostic and intended for logs, metrics, tracing, and runtime event bridges.

Worker hook event names:

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

## Database Behavior

Common logical tables:

- `amp_queue_jobs`
- `amp_queue_failed_jobs`

Required job statuses:

- `pending`
- `processing`
- `completed`
- `failed`
- `cancelled`

PostgreSQL behavior:

- Uses `amphp/postgres`.
- Uses `FOR UPDATE SKIP LOCKED`.
- Uses `UPDATE ... RETURNING`.
- Uses `LISTEN/NOTIFY` for low-latency worker wakeups.
- Keeps polling fallback.
- Uses partial unique index for optional idempotency.

MySQL / MariaDB behavior:

- Uses `amphp/mysql`.
- Requires MySQL 8.0+ or MariaDB 10.6+ for `FOR UPDATE SKIP LOCKED`.
- Uses explicit transactions for reserve.
- Uses adaptive polling because MySQL/MariaDB have no direct equivalent to PostgreSQL `LISTEN/NOTIFY`.
- Relies on InnoDB row locks.
- Do not use `CAST(? AS JSON)` in runtime SQL; MariaDB 11.8 rejects that prepared statement syntax. Pass JSON as string and let JSON columns validate it.
- MariaDB concurrent `SKIP LOCKED` reservation may return one reserved job and one empty reservation while another row remains pending. The next reservation should pick up the pending row. Tests assert safety/no duplicates, not identical immediate parallelism to PostgreSQL.

## Files Of Interest

- `README.md`: public project documentation.
- `composer.json`: package config and dependencies.
- `src/`: production code.
- `src/Storage/PostgresQueueStorage.php`: PostgreSQL storage.
- `src/Storage/MysqlQueueStorage.php`: MySQL storage.
- `migrations/postgres/001_create_queue_tables.sql`: PostgreSQL schema.
- `migrations/mysql/001_create_queue_tables.sql`: MySQL schema.
- `tests/Unit/`: fast unit tests.
- `tests/Integration/`: DSN-gated database integration tests.
- `examples/dispatch.php`: dispatch example.
- `examples/worker.php`: worker example.
- `docs/superpowers/specs/2026-05-07-amp-sql-queue-design.md`: approved design.
- `docs/superpowers/plans/2026-05-07-amp-sql-queue.md`: implementation plan.

## Verification Commands

Run fast checks:

```bash
composer validate --strict
vendor/bin/phpunit --testsuite Unit
vendor/bin/phpstan analyse
```

Run all tests. Integration tests skip unless DSNs are set:

```bash
vendor/bin/phpunit
```

Run integration tests with local databases:

```bash
docker compose up -d postgres mysql
export AMP_SQL_QUEUE_POSTGRES_DSN='host=127.0.0.1 port=5432 user=amp_queue password=amp_queue dbname=amp_queue'
export AMP_SQL_QUEUE_MYSQL_DSN='host=127.0.0.1 port=3306 user=amp_queue password=amp_queue db=amp_queue'
vendor/bin/phpunit --testsuite Integration
```

## Current Verification Status

Last verified in this workspace:

- `composer validate --strict`: passes.
- `vendor/bin/phpunit --testsuite Unit`: passes, 12 tests and 25 assertions.
- `AMP_SQL_QUEUE_POSTGRES_DSN=... vendor/bin/phpunit tests/Integration/PostgresQueueStorageTest.php`: passes, 11 tests and 39 assertions against real PostgreSQL.
- `AMP_SQL_QUEUE_MYSQL_DSN=... vendor/bin/phpunit tests/Integration/MysqlQueueStorageTest.php`: passes, 11 tests and 42 assertions against MariaDB 11.8.
- `vendor/bin/phpstan analyse`: passes.
- Full `AMP_SQL_QUEUE_POSTGRES_DSN=... AMP_SQL_QUEUE_MYSQL_DSN=... vendor/bin/phpunit`: passes, 34 tests and 107 assertions.

Note: PHPUnit may warn that `.phpunit.cache/test-results` cannot be written if the workspace is read-only. This warning does not indicate a test failure.

PostgreSQL integration coverage currently includes:

- dispatch/reserve/ack
- idempotency
- `DispatchResult` inserted/deduplicated behavior
- delayed jobs not reservable before `available_at`
- priority ordering
- concurrent reservation with `SKIP LOCKED`
- failure requeue with delay and `last_error`
- dead letter copy when attempts are exhausted
- expired lease recovery
- lease extension
- worker retry/backoff followed by ack

MariaDB integration coverage mirrors the PostgreSQL cases, with a concurrency assertion adjusted for MariaDB's `SKIP LOCKED` behavior: concurrent reservations must not duplicate jobs, and any skipped pending job must remain reservable immediately afterward.

## Repository Note

The workspace currently contains a `.git` directory that is empty or invalid. Git commands such as `git status` fail with "not a git repository". Do not assume commits are possible until the repository is initialized or repaired.

## Development Guidance

- Keep classes small and explicit.
- Avoid framework-specific abstractions.
- Prefer storage-specific SQL over a premature generic SQL dialect abstraction.
- Use AMPHP-compatible clients in handlers; blocking I/O inside handlers blocks the event loop.
- Preserve a single public API for PostgreSQL, MySQL, and MariaDB.
- Add tests before changing behavior.
- Keep integration tests gated by environment variables so the unit suite remains fast.
