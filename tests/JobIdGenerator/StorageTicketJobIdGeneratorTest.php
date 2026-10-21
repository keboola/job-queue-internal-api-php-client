<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobIdGenerator;

use Keboola\JobQueueInternalClient\JobIdGenerator\StorageTicketJobIdGenerator;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;

class StorageTicketJobIdGeneratorTest extends TestCase
{
    public function testGenerateJobId(): void
    {
        $storageClient = $this->createMock(StorageClient::class);
        $storageClient->expects(self::once())->method('generateId')->willReturn('12345');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($storageClient);

        self::assertSame('12345', (new StorageTicketJobIdGenerator())->generateJobId($clientWrapper));
    }
}
