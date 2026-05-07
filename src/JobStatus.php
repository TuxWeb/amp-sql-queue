<?php

declare(strict_types=1);

namespace AmpSqlQueue;

final class JobStatus
{
    public const string Pending = 'pending';
    public const string Processing = 'processing';
    public const string Completed = 'completed';
    public const string Failed = 'failed';
    public const string Cancelled = 'cancelled';

    /** @return list<string> */
    public static function all(): array
    {
        return [
            self::Pending,
            self::Processing,
            self::Completed,
            self::Failed,
            self::Cancelled,
        ];
    }
}
