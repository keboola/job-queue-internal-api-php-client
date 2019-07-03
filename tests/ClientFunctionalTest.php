<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Exception;
use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\Job;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use PHPUnit\Framework\TestCase;

class ClientFunctionalTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();
        if (empty(getenv('TEST_QUEUE_API_URL')) || empty(getenv('TEST_STORAGE_API_URL'))
            || empty(getenv('TEST_STORAGE_API_TOKEN'))
            || empty(getenv('TEST_KMS_KEY_ALIAS')) || empty(getenv('TEST_KMS_REGION'))
            || empty(getenv('TEST_AWS_ACCESS_KEY_ID')) || empty(getenv('TEST_AWS_SECRET_ACCESS_KEY'))
        ) {
            throw new Exception('The environment variable "TEST_QUEUE_API_URL" or "TEST_STORAGE_API_URL" ' .
                'or "TEST_STORAGE_API_TOKEN" or "TEST_KMS_KEY_ALIAS" or "TEST_KMS_REGION" or ' .
                '"TEST_AWS_ACCESS_KEY_ID" or "TEST_AWS_SECRET_ACCESS_KEY" is empty.');
        }
        putenv('AWS_ACCESS_KEY_ID=' . getenv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . getenv('TEST_AWS_SECRET_ACCESS_KEY'));
    }

    private function getJobFactory(): JobFactory
    {
        $storageClientFactory = new JobFactory\StorageClientFactory((string) getenv('TEST_STORAGE_API_URL'));
        $objectEncryptorFactory = new ObjectEncryptorFactory(
            (string) getenv('TEST_KMS_KEY_ALIAS'),
            (string) getenv('TEST_KMS_REGION'),
            '',
            ''
        );
        return new JobFactory($storageClientFactory, $objectEncryptorFactory);
    }

    private function getClient(): Client
    {
        return new Client($this->getJobFactory(), (string) getenv('TEST_QUEUE_API_URL'), 'dummy', []);
    }

    public function testCreateJob(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'params' => [
                'config' => '454124290',
                'component' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
            ],
        ]);
        $response = $client->createJob($job);
        self::assertNotEmpty($response['id']);
        self::assertNotEmpty($response['createdTime']);
        unset($response['id']);
        unset($response['createdTime']);
        $expected = [
            'params' => [
                'config' => '454124290',
                'component' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
            ],
            'status' => 'created',
        ];
        self::assertEquals($expected, $response);
    }
}
