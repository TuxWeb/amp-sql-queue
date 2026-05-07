<?php

declare(strict_types=1);

namespace AmpSqlQueue\Tests\Unit;

use AmpSqlQueue\Exception\SerializationException;
use AmpSqlQueue\Serialization\JsonSerializer;
use PHPUnit\Framework\TestCase;

final class JsonSerializerTest extends TestCase
{
    public function testRoundTripsJsonPayload(): void
    {
        $serializer = new JsonSerializer();

        $encoded = $serializer->serialize(['project_file_id' => 123, 'tags' => ['ocr']]);

        self::assertSame(['project_file_id' => 123, 'tags' => ['ocr']], $serializer->deserialize($encoded));
    }

    public function testRejectsInvalidJson(): void
    {
        $serializer = new JsonSerializer();

        $this->expectException(SerializationException::class);

        $serializer->deserialize('{invalid');
    }
}
