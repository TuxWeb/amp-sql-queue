<?php

declare(strict_types=1);

namespace AmpSqlQueue;

use Amp\Future;
use AmpSqlQueue\Backoff\BackoffStrategyInterface;
use AmpSqlQueue\Backoff\ExponentialBackoffStrategy;
use AmpSqlQueue\Exception\InvalidArgumentException;
use AmpSqlQueue\Handler\JobHandlerInterface;
use AmpSqlQueue\Internal\FutureFactory;
use Revolt\EventLoop;
use function Amp\delay;

final class Worker
{
    use FutureFactory;

    private bool $stopping = false;
    private readonly string $workerId;
    /** @var array<int, WorkerHookInterface|callable> */
    private array $hooks = [];

    /** @var callable(Job):Future<mixed> */
    private readonly mixed $handler;

    /**
     * @param callable(Job):Future<mixed>|JobHandlerInterface $handler
     */
    public function __construct(
        private readonly QueueInterface $queue,
        private readonly string $queueName,
        private readonly int $concurrency,
        private readonly int $leaseSeconds,
        callable|JobHandlerInterface $handler,
        private readonly BackoffStrategyInterface $backoffStrategy = new ExponentialBackoffStrategy(),
        private readonly float $idlePollSeconds = 0.25,
        private readonly bool $stopWhenIdle = false,
        ?string $workerId = null,
    ) {
        if ($this->queueName === '') {
            throw new InvalidArgumentException('Queue name cannot be empty.');
        }

        if ($this->concurrency < 1) {
            throw new InvalidArgumentException('Worker concurrency must be greater than or equal to one.');
        }

        if ($this->leaseSeconds < 1) {
            throw new InvalidArgumentException('Worker lease must be greater than or equal to one second.');
        }

        if ($handler instanceof JobHandlerInterface) {
            $this->handler = $handler->handle(...);
        } else {
            $this->handler = $handler;
        }

        $this->workerId = $workerId ?? \gethostname() . '-' . \getmypid() . '-' . \bin2hex(\random_bytes(4));
    }

    public function stop(): void
    {
        $this->stopping = true;
    }

    public function addHook(WorkerHookInterface|callable $hook): void
    {
        $this->hooks[] = $hook;
    }

    public function run(): void
    {
        $this->registerSignalHandlers();
        $this->emitHook(WorkerHookEvents::WorkerStarted);

        /** @var array<int, Future<null>> $active */
        $active = [];

        try {
            while (!$this->stopping || $active !== []) {
                while (!$this->stopping && \count($active) < $this->concurrency) {
                    $job = $this->queue->storage()->reserve($this->queueName, $this->workerId, $this->leaseSeconds)->await();

                    if (!$job instanceof Job) {
                        if ($this->stopWhenIdle) {
                            $this->stopping = true;
                            $this->emitHook(WorkerHookEvents::WorkerIdle);
                            break;
                        }

                        $this->emitHook(WorkerHookEvents::WorkerIdle);
                        $this->queue->storage()->waitForJob($this->queueName, $this->idlePollSeconds)->await();
                        break;
                    }

                    $this->emitHook(WorkerHookEvents::JobReserved, ['job' => $job]);
                    $active[] = $this->process($job);
                }

                if ($active === []) {
                    continue;
                }

                foreach (Future::iterate($active) as $index => $future) {
                    unset($active[$index]);
                    $future->await();
                    break;
                }
            }
        } finally {
            $this->emitHook(WorkerHookEvents::WorkerStopped);
        }
    }

    /** @return Future<null> */
    private function process(Job $job): Future
    {
        return $this->future(function () use ($job): null {
            try {
                $this->emitHook(WorkerHookEvents::JobStarted, ['job' => $job]);
                ($this->handler)($job)->await();
                $this->emitHook(WorkerHookEvents::JobSucceeded, ['job' => $job]);
                $this->queue->storage()->ack($job)->await();
                $this->emitHook(WorkerHookEvents::JobAcked, ['job' => $job]);
            } catch (\Throwable $error) {
                $delaySeconds = $this->backoffStrategy->delaySeconds($job->attempts, $job->maxAttempts, $error);
                $this->emitHook(WorkerHookEvents::JobFailed, [
                    'job' => $job,
                    'error' => $error,
                    'delaySeconds' => $delaySeconds,
                ]);
                $this->queue->storage()->fail($job, $error, $delaySeconds)->await();
                $this->emitHook(
                    $job->attempts >= $job->maxAttempts ? WorkerHookEvents::JobDeadLettered : WorkerHookEvents::JobReleased,
                    [
                        'job' => $job,
                        'error' => $error,
                        'delaySeconds' => $delaySeconds,
                    ],
                );
            }

            return null;
        });
    }

    /**
     * @param array<string, mixed> $context
     */
    private function emitHook(string $event, array $context = []): void
    {
        $context += [
            'queueName' => $this->queueName,
            'workerId' => $this->workerId,
        ];

        foreach ($this->hooks as $hook) {
            if ($hook instanceof WorkerHookInterface) {
                $hook->handle($event, $context);
                continue;
            }

            $hook($event, $context);
        }
    }

    private function registerSignalHandlers(): void
    {
        foreach ([\SIGINT, \SIGTERM] as $signal) {
            try {
                EventLoop::onSignal($signal, function (): void {
                    $this->stop();
                });
            } catch (\Throwable) {
                return;
            }
        }
    }
}
