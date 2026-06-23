<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result;

use Keboola\JobQueueInternalClient\Result\GenericJobResult;
use PHPUnit\Framework\TestCase;

class GenericJobResultTest extends TestCase
{
    public function testJsonSerializeReturnsWrappedArrayVerbatim(): void
    {
        $data = [
            'message' => 'done',
            'nested' => [
                'foo' => 'bar',
                'list' => [1, 2, 3],
            ],
        ];

        $result = new GenericJobResult($data);

        self::assertSame($data, $result->jsonSerialize());
    }

    public function testJsonSerializeOfEmptyArray(): void
    {
        $result = new GenericJobResult([]);

        self::assertSame([], $result->jsonSerialize());
    }
}
