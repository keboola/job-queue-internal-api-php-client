<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\StorageClientFactory;
use Keboola\JobQueueInternalClient\Tests\BaseTest;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Psr\Log\Test\TestLogger;

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
        $storageClientFactory = new StorageClientFactory($this->storageApiUrl, new TestLogger());
        $client = $storageClientFactory->getClient((string) getenv('TEST_STORAGE_API_TOKEN'));

        $this->assertInstanceOf(Client::class, $client);
        $this->assertEquals($this->storageApiUrl, $client->getApiUrl());
        $this->assertEquals($this->storageApiToken, $client->getTokenString());
    }

    public function testGetClientWithBranch(): void
    {
        $storageClientFactory = new StorageClientFactory($this->storageApiUrl, new TestLogger());
        /** @var BranchAwareClient $client */
        $client = $storageClientFactory->getClient(
            (string) getenv('TEST_STORAGE_API_TOKEN'),
            'dev-branch'
        );

        $this->assertInstanceOf(BranchAwareClient::class, $client);
        $this->assertEquals($this->storageApiUrl, $client->getApiUrl());
        $this->assertEquals($this->storageApiToken, $client->getTokenString());
    }

    public function testCreateFactoryInvalidUrl(): void
    {
        self::expectExceptionMessage('Value "foo bar" is invalid: Storage API URL is not valid.');
        self::expectException(ClientException::class);
        new StorageClientFactory('foo bar', new TestLogger());
    }
}
