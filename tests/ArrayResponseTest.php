<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\ApiClientBase\ResponseModelInterface;
use Keboola\JobQueueInternalClient\ArrayResponse;
use PHPUnit\Framework\TestCase;

class ArrayResponseTest extends TestCase
{
    public function testWrapsMapBody(): void
    {
        $response = ArrayResponse::fromResponseData(['id' => '123', 'status' => 'created']);

        self::assertInstanceOf(ResponseModelInterface::class, $response);
        self::assertSame(['id' => '123', 'status' => 'created'], $response->data);
    }

    public function testWrapsListBody(): void
    {
        $response = ArrayResponse::fromResponseData([['id' => '1'], ['id' => '2']]);

        self::assertSame([['id' => '1'], ['id' => '2']], $response->data);
    }
}
