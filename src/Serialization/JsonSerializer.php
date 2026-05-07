<?php

declare(strict_types=1);

namespace AmpSqlQueue\Serialization;

use AmpSqlQueue\Exception\SerializationException;

final class JsonSerializer implements SerializerInterface
{
    public function serialize(array $payload): string
    {
        try {
            return \json_encode($payload, JSON_THROW_ON_ERROR);
        } catch (\JsonException $exception) {
            throw new SerializationException('Unable to serialize queue payload as JSON.', previous: $exception);
        }
    }

    public function deserialize(string $payload): array
    {
        try {
            $decoded = \json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $exception) {
            throw new SerializationException('Unable to deserialize queue payload JSON.', previous: $exception);
        }

        if (!\is_array($decoded)) {
            throw new SerializationException('Queue payload JSON must decode to an array.');
        }

        /** @var array<string, mixed> $decoded */
        return $decoded;
    }
}
