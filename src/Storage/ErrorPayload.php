<?php

declare(strict_types=1);

namespace AmpSqlQueue\Storage;

final class ErrorPayload
{
    public static function fromThrowable(\Throwable $error): string
    {
        return \json_encode([
            'class' => $error::class,
            'message' => $error->getMessage(),
            'code' => $error->getCode(),
            'file' => $error->getFile(),
            'line' => $error->getLine(),
        ], JSON_THROW_ON_ERROR);
    }
}
