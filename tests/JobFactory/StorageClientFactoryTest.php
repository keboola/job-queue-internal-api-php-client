<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\StorageClientFactory;
use Keboola\JobQueueInternalClient\Tests\BaseTest;
use Keboola\StorageApi\Client;

class StorageClientFactoryTest extends BaseTest
{
    /** @var string */
    private $storageApiUrl;

    /** @var string */
    private $storageApiToken;

    public function setUp(): void
    {
        parent::setUp();
        $this->storageApiUrl = (string) getenv('TEST_STORAGE_API_URL');
        $this->storageApiToken = (string) getenv('TEST_STORAGE_API_TOKEN');
    }

    public function testGetClient(): void
    {
        $storageClientFactory = new StorageClientFactory($this->storageApiUrl);
        $client = $storageClientFactory->getClient((string) getenv('TEST_STORAGE_API_TOKEN'));

        $this->assertInstanceOf(Client::class, $client);
        $this->assertEquals($this->storageApiUrl, $client->getApiUrl());
        $this->assertEquals($this->storageApiToken, $client->getTokenString());
    }

    public function testCreateFactoryInvalidUrl(): void
    {
        self::expectExceptionMessage('Value "foo bar" is invalid: Storage API URL is not valid.');
        self::expectException(ClientException::class);
        new StorageClientFactory('foo bar');
    }
}
