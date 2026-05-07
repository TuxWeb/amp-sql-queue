<?php

declare(strict_types=1);

namespace AmpSqlQueue;

interface WorkerHookInterface
{
    /**
     * @param array<string, mixed> $context
     */
    public function handle(string $event, array $context): void;
}
