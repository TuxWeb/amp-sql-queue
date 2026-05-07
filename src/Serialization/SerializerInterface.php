<?php

declare(strict_types=1);

namespace AmpSqlQueue\Serialization;

interface SerializerInterface
{
    /** @param array<string, mixed> $payload */
    public function serialize(array $payload): string;

    /** @return array<string, mixed> */
    public function deserialize(string $payload): array;
}
