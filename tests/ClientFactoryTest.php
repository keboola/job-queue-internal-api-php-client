<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\ClientFactory;
use Keboola\JobQueueInternalClient\ExistingJobFactory;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class ClientFactoryTest extends TestCase
{
    public function testGetClient(): void
    {
        $testLogger = new TestLogger();
        $factory = new ClientFactory(
            'https://connection.keboola.com',
            'internalApiToken',
            $this->createMock(ExistingJobFactory::class),
            $testLogger
        );
        $client = $factory->getClient();
        self::assertInstanceOf(Client::class, $client);
    }
}
