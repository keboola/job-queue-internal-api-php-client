<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\TransportErrorMessageResolver;
use PHPUnit\Framework\TestCase;

class TransportErrorMessageResolverTest extends TestCase
{
    public function testAlwaysReturnsNullSoTransportMessageIsUsed(): void
    {
        $resolver = new TransportErrorMessageResolver();

        self::assertNull($resolver('{"error":"Job \"123\" not found"}', 404));
        self::assertNull($resolver('', 500));
    }
}
