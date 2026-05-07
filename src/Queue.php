<?php

declare(strict_types=1);

namespace AmpSqlQueue;

use Amp\Future;
use AmpSqlQueue\Exception\InvalidArgumentException;
use AmpSqlQueue\Serialization\JsonSerializer;
use AmpSqlQueue\Serialization\SerializerInterface;
use AmpSqlQueue\Storage\QueueStorageInterface;
use AmpSqlQueue\Storage\StoredJobEnvelope;

final readonly class Queue implements QueueInterface
{
    public function __construct(
        private QueueStorageInterface $storage,
        private SerializerInterface $serializer = new JsonSerializer(),
    ) {
    }

    public function dispatch(
        string $queue,
        string $type,
        array $payload,
        DispatchOptions $options = new DispatchOptions(),
    ): Future {
        if ($queue === '') {
            throw new InvalidArgumentException('Queue name cannot be empty.');
        }

        if ($type === '') {
            throw new InvalidArgumentException('Job type cannot be empty.');
        }

        return $this->storage->dispatch(new StoredJobEnvelope(
            queueName: $queue,
            type: $type,
            payload: $this->serializer->serialize($payload),
            delaySeconds: $options->delaySeconds,
            priority: $options->priority,
            maxAttempts: $options->maxAttempts,
            idempotencyKey: $options->idempotencyKey,
        ));
    }

    public function storage(): QueueStorageInterface
    {
        return $this->storage;
    }
}
