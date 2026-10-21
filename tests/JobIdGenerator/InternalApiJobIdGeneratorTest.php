<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobIdGenerator;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\JobIdGenerator\InternalApiJobIdGenerator;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;

class InternalApiJobIdGeneratorTest extends TestCase
{
    public function testGenerateJobId(): void
    {
        $internalClient = $this->createMock(Client::class);
        $internalClient->expects(self::once())->method('generateJobIds')->with()->willReturn(['12345']);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects(self::never())->method('getBasicClient');

        self::assertSame('12345', (new InternalApiJobIdGenerator($internalClient))->generateJobId($clientWrapper));
    }
}
