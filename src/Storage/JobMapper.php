<?php

declare(strict_types=1);

namespace AmpSqlQueue\Storage;

use AmpSqlQueue\Job;
use AmpSqlQueue\Serialization\JsonSerializer;
use AmpSqlQueue\Serialization\SerializerInterface;

final readonly class JobMapper
{
    public function __construct(private SerializerInterface $serializer = new JsonSerializer())
    {
    }

    /**
     * @param array<string, mixed> $row
     */
    public function fromRow(array $row): Job
    {
        return new Job(
            id: $this->id($row['id'] ?? null),
            queueName: $this->string($row['queue_name'] ?? null),
            type: $this->string($row['type'] ?? null),
            payload: $this->decodeJson($row['payload']),
            status: $this->string($row['status'] ?? null),
            priority: $this->int($row['priority'] ?? null),
            attempts: $this->int($row['attempts'] ?? null),
            maxAttempts: $this->int($row['max_attempts'] ?? null),
            availableAt: $this->dateTime($row['available_at']),
            lockedUntil: $this->nullableDateTime($row['locked_until'] ?? null),
            lockedBy: $row['locked_by'] === null ? null : $this->string($row['locked_by']),
            lastError: $row['last_error'] === null ? null : $this->decodeJson($row['last_error']),
            idempotencyKey: $row['idempotency_key'] === null ? null : $this->string($row['idempotency_key']),
            createdAt: $this->dateTime($row['created_at']),
            updatedAt: $this->dateTime($row['updated_at']),
            completedAt: $this->nullableDateTime($row['completed_at'] ?? null),
        );
    }

    /** @return array<string, mixed> */
    private function decodeJson(mixed $value): array
    {
        if (\is_array($value)) {
            /** @var array<string, mixed> $value */
            return $value;
        }

        return $this->serializer->deserialize($this->string($value));
    }

    private function dateTime(mixed $value): \DateTimeImmutable
    {
        if ($value instanceof \DateTimeImmutable) {
            return $value;
        }

        if ($value instanceof \DateTimeInterface) {
            return \DateTimeImmutable::createFromInterface($value);
        }

        return new \DateTimeImmutable($this->string($value));
    }

    private function nullableDateTime(mixed $value): ?\DateTimeImmutable
    {
        if ($value === null || $value === '') {
            return null;
        }

        return $this->dateTime($value);
    }

    private function string(mixed $value): string
    {
        if (\is_string($value) || \is_int($value) || \is_float($value)) {
            return (string) $value;
        }

        throw new \UnexpectedValueException('Expected scalar SQL value.');
    }

    private function int(mixed $value): int
    {
        if (\is_int($value)) {
            return $value;
        }

        if (\is_string($value) && \is_numeric($value)) {
            return (int) $value;
        }

        throw new \UnexpectedValueException('Expected integer SQL value.');
    }

    private function id(mixed $value): int|string
    {
        if (\is_int($value) || \is_string($value)) {
            return $value;
        }

        throw new \UnexpectedValueException('Expected SQL job id.');
    }
}
