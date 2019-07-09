<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Exception;
use Keboola\JobQueueInternalClient\JobFactory\StorageClientFactory;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class StorageClientFactoryTest extends TestCase
{
    /** @var string */
    private $storageApiUrl;

    /** @var string */
    private $storageApiToken;

    public function setUp(): void
    {
        parent::setUp();
        if (empty(getenv('TEST_STORAGE_API_URL')) || empty(getenv('TEST_STORAGE_API_TOKEN'))) {
            throw new Exception(
                'The environment variable "TEST_STORAGE_API_URL" or "TEST_STORAGE_API_TOKEN" is empty.'
            );
        }

        $this->storageApiUrl = getenv('TEST_STORAGE_API_URL');
        $this->storageApiToken = getenv('TEST_STORAGE_API_TOKEN');
    }

    public function testGetClient(): void
    {
        $storageClientFactory = $this->getStorageClientFactory();
        $client = $storageClientFactory->getClient(getenv('TEST_STORAGE_API_TOKEN'));

        $this->assertInstanceOf(Client::class, $client);
        $this->assertEquals($this->storageApiUrl, $client->getApiUrl());
        $this->assertEquals($this->storageApiToken, $client->getTokenString());
    }

    private function getStorageClientFactory(): StorageClientFactory
    {
        return new StorageClientFactory($this->storageApiUrl);
    }
}
