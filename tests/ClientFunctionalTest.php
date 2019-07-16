<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Exception;
use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

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
        $this->cleanJobs();
    }

    private function cleanJobs(): void
    {
        // cancel all created jobs
        $client = $this->getClient();
        $response = $client->getJobsWithStatus([JobFactory::STATUS_CREATED]);
        /** @var Job $job */
        foreach ($response as $job) {
            $newJob = $client->getJobFactory()->modifyJob($job, ['status' => JobFactory::STATUS_CANCELLED]);
            $client->updateJob($newJob);
        }
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
        return new Client(new NullLogger(), $this->getJobFactory(), (string) getenv('TEST_QUEUE_API_URL'), 'dummy', []);
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
        $response = $client->createJob($job)->jsonSerialize();
        self::assertNotEmpty($response['id']);
        unset($response['id']);
        self::assertNotEmpty($response['createdTime']);
        unset($response['createdTime']);
        $storageClient = new \Keboola\StorageApi\Client(
            [
                'url' => getenv('TEST_STORAGE_API_URL'),
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ]
        );
        $tokenInfo = $storageClient->verifyToken();
        self::assertStringStartsWith('KBC::ProjectSecure::', $response['token']['token']);
        unset($response['token']['token']);
        $expected = [
            'params' => [
                'config' => '454124290',
                'component' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'row' => null,
                'tag' => null,
            ],
            'status' => 'created',
            'project' => [
                'id' => $tokenInfo['owner']['id'],
            ],
            'token' => [
                'id' => $tokenInfo['id'],
            ],
        ];
        self::assertEquals($expected, $response);
    }

    public function testGetJob(): void
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
        $createdJob = $client->createJob($job);
        $client = $this->getClient();
        $job = $client->getJob($createdJob->getId());
        self::assertEquals($createdJob->getToken(), $job->getToken());
        self::assertEquals($createdJob->getConfigData(), $job->getConfigData());
        self::assertEquals($createdJob->getComponentId(), $job->getComponentId());
        self::assertEquals($createdJob->getConfigId(), $job->getConfigId());
        self::assertEquals($createdJob->getMode(), $job->getMode());
    }

    public function testGetInvalidJob(): void
    {
        $client = $this->getClient();
        self::expectException(ClientException::class);
        self::expectExceptionMessage('404 Not Found');
        $client->getJob('123456');
    }

    public function testGetJobsWithStatuses(): void
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
        $createdJob = $client->createJob($job);
        $client = $this->getClient();
        $response = $client->getJobsWithStatus([JobFactory::STATUS_CREATED]);
        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());
    }

    public function testGetJobsWithIds(): void
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
        $createdJob = $client->createJob($job);
        $client = $this->getClient();
        $response = $client->getJobsWithIds([$createdJob->getId()]);
        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());
    }

    public function testPostJobResult(): void
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
        $createdJob = $client->createJob($job);
        $client = $this->getClient();
        $job = $client->getJob($createdJob->getId());
        self::assertEquals(JobFactory::STATUS_CREATED, $job->getStatus());
        self::assertEquals(null, $job->getResult());
        $client->postJobResult($createdJob->getId(), JobFactory::STATUS_SUCCESS, ['foo' => 'bar']);
        $job = $client->getJob($createdJob->getId());
        self::assertEquals(JobFactory::STATUS_SUCCESS, $job->getStatus());
        self::assertEquals(['foo' => 'bar'], $job->getResult());
    }
}
