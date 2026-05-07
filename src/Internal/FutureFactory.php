<?php

declare(strict_types=1);

namespace AmpSqlQueue\Internal;

use Amp\Future;
use function Amp\async;

trait FutureFactory
{
    /**
     * @template T
     * @param \Closure():T $closure
     * @return Future<T>
     */
    protected function future(\Closure $closure): Future
    {
        /** @var Future<T> $future */
        $future = async($closure);

        return $future;
    }
}
