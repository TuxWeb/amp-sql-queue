<?php

declare(strict_types=1);

namespace AmpSqlQueue\Handler;

use Amp\Future;
use AmpSqlQueue\Job;

interface JobHandlerInterface
{
    /** @return Future<mixed> */
    public function handle(Job $job): Future;
}
