<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\ClientFactory;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class ClientFactoryTest extends TestCase
{
    public function testGetClient(): void
    {
        $testLogger = new TestLogger();
        $factory = new ClientFactory(
            (string) getenv('TEST_QUEUE_API_URL'),
            (string) getenv('TEST_QUEUE_API_TOKEN'),
            new JobFactory(
                new StorageClientPlainFactory(new ClientOptions((string) getenv('TEST_STORAGE_API_URL'))),
                ObjectEncryptorFactory::getAwsEncryptor('no-used', 'alias/some-key', 'us-east-1')
            ),
            $testLogger
        );
        $client = $factory->getClient();
        self::assertInstanceOf(Client::class, $client);
    }
}
