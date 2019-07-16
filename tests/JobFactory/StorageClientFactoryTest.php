<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Exception;
use Keboola\JobQueueInternalClient\ClientException;
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
        $storageClientFactory = new StorageClientFactory($this->storageApiUrl);
        $client = $storageClientFactory->getClient(getenv('TEST_STORAGE_API_TOKEN'));

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
