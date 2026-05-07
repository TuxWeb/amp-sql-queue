<?php

declare(strict_types=1);

namespace AmpSqlQueue;

use Amp\Future;
use AmpSqlQueue\Storage\QueueStorageInterface;

interface QueueInterface
{
    /**
     * @param array<string, mixed> $payload
     * @return Future<DispatchResult>
     */
    public function dispatch(
        string $queue,
        string $type,
        array $payload,
        DispatchOptions $options = new DispatchOptions(),
    ): Future;

    public function storage(): QueueStorageInterface;
}
