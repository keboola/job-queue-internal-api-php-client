<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\StateTargetEqualsCurrentException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobResult;
use Keboola\JobQueueInternalClient\JobListOptions;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Psr\Log\NullLogger;

class ClientFunctionalTest extends BaseTest
{
    public function setUp(): void
    {
        parent::setUp();
        putenv('AWS_ACCESS_KEY_ID=' . getenv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . getenv('TEST_AWS_SECRET_ACCESS_KEY'));
        putenv('AZURE_TENANT_ID=' . getenv('TEST_AZURE_TENANT_ID'));
        putenv('AZURE_CLIENT_ID=' . getenv('TEST_AZURE_CLIENT_ID'));
        putenv('AZURE_CLIENT_SECRET=' . getenv('TEST_AZURE_CLIENT_SECRET'));
        $this->cleanJobs();
    }

    public function cipherProvider(): array
    {
        return [
            'azure' => [
                'kmsKeyId' => '',
                'keyVaultUrl' => getenv('TEST_AZURE_KEY_VAULT_URL'),
                'KBC::ProjectSecureKV::',
            ],
            'aws' => [
                'kmsKeyId' => getenv('TEST_KMS_KEY_ID'),
                'keyVaultUrl' => '',
                'KBC::ProjectSecure::',
            ],
        ];
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

    private function getJobFactory(?string $kmsKeyId = null, ?string $keyVaultUrl = null): JobFactory
    {
        $storageClientFactory = new JobFactory\StorageClientFactory((string) getenv('TEST_STORAGE_API_URL'));
        $objectEncryptorFactory = new ObjectEncryptorFactory(
            $kmsKeyId ?? (string) getenv('TEST_KMS_KEY_ID'),
            (string) getenv('TEST_KMS_REGION'),
            '',
            '',
            $keyVaultUrl ?? (string) getenv('TEST_AZURE_KEY_VAULT_URL')
        );
        return new JobFactory($storageClientFactory, $objectEncryptorFactory);
    }

    private function getClient(?string $kmsKeyId = null, ?string $keyVaultUrl = null): Client
    {
        return new Client(
            new NullLogger(),
            $this->getJobFactory($kmsKeyId, $keyVaultUrl),
            (string) getenv('TEST_QUEUE_API_URL'),
            'dummy'
        );
    }

    /**
     * @param string $kmsKeyId
     * @param string $keyVaultUrl
     * @param string $cipherPrefix
     * @dataProvider cipherProvider
     */
    public function testCreateJob(string $kmsKeyId, string $keyVaultUrl, string $cipherPrefix): void
    {
        $client = $this->getClient($kmsKeyId, $keyVaultUrl);
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
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
        self::assertStringStartsWith($cipherPrefix, $response['#tokenString']);
        unset($response['#tokenString']);
        self::assertNotEmpty($response['runId']);
        unset($response['runId']);
        $expected = [
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
            'configRowId' => null,
            'tag' => null,
            'configData' => [],
            'status' => 'created',
            'desiredStatus' => 'processing',
            'projectId' => (string) $tokenInfo['owner']['id'],
            'projectName' => (string) $tokenInfo['owner']['name'],
            'tokenId' => $tokenInfo['id'],
            'tokenDescription' => $tokenInfo['description'],
            'result' => [],
            'usageData' => [],
            'isFinished' => false,
            'startTime' => null,
            'endTime' => null,
            'durationSeconds' => 0,
        ];
        self::assertEquals($expected, $response);
    }

    public function testGetJob(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $client = $this->getClient();
        $job = $client->getJob($createdJob->getId());
        self::assertEquals($createdJob->getTokenString(), $job->getTokenString());
        self::assertEquals($createdJob->getConfigData(), $job->getConfigData());
        self::assertEquals($createdJob->getComponentId(), $job->getComponentId());
        self::assertEquals($createdJob->getConfigId(), $job->getConfigId());
        self::assertEquals($createdJob->getMode(), $job->getMode());
        self::assertEquals([], $job->getResult());
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
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
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
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $response = $client->getJobsWithIds([$createdJob->getId()]);
        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());
    }

    public function testListJobsByComponent(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '12345',
            'componentId' => 'keboola.ex-google-drive',
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '56789',
            'componentId' => 'keboola.ex-google-analytics',
            'mode' => 'run',
        ]);
        $client->createJob($job2);

        // list one component
        $response = $client->listJobs(
            (new JobListOptions())
                ->setComponents(['keboola.ex-google-drive'])
                ->setStatuses([JobFactory::STATUS_CREATED]),
            true
        );
        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());

        // list more components
        $response = $client->listJobs(
            (new JobListOptions())
                ->setComponents(['keboola.ex-google-drive', 'keboola.ex-google-analytics'])
                ->setStatuses([JobFactory::STATUS_CREATED]),
            true
        );
        self::assertCount(2, $response);
    }

    public function testListJobsEscaping(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '(*^&^$%£  $"£)?! \'',
            'componentId' => '{}=žýřčšěš',
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);

        //@todo: components filter doesn't work
        $response = $client->listJobs(
            (new JobListOptions())
                ->setConfigs(['(*^&^$%£  $"£)?! \''])
                ->setComponents(['{}=žýřčšěš'])
                ->setStatuses([JobFactory::STATUS_CREATED]),
            true
        );
        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());
    }

    public function testGetJobsWithNoIds(): void
    {
        $client = $this->getClient();
        $response = $client->getJobsWithIds([]);
        self::assertCount(0, $response);
    }

    public function testGetJobsWithNoStatuses(): void
    {
        $client = $this->getClient();
        $response = $client->getJobsWithStatus([]);
        self::assertCount(0, $response);
    }

    public function testPostJobResult(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $client = $this->getClient();
        $job = $client->getJob($createdJob->getId());
        self::assertEquals(JobFactory::STATUS_CREATED, $job->getStatus());
        self::assertEquals([], $job->getResult());
        $client->postJobResult(
            $createdJob->getId(),
            JobFactory::STATUS_PROCESSING,
            (new JobResult())->setMessage('bar')
        );
        $job = $client->getJob($createdJob->getId());
        self::assertEquals(JobFactory::STATUS_PROCESSING, $job->getStatus());
        self::assertEquals('bar', $job->getResult()['message']);
    }

    public function testGetJobsWithProjectId(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $response = $client->listJobs(
            (new JobListOptions())->setProjects([$job->getProjectId()])->setIds([$job->getId()]),
            false
        );

        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());
    }

    public function testGetJobsWithProjectIdNonExisting(): void
    {
        $client = $this->getClient();

        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
        ]);

        $client->createJob($job);
        $client = $this->getClient();
        $response = $client->listJobs(
            (new JobListOptions())
                ->setProjects([$job->getProjectId()])
                ->setComponents(['keboola.non-existent']),
            false
        );

        self::assertCount(0, $response);
    }

    public function testUpdateJobStatusRejected(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        self::expectException(StateTargetEqualsCurrentException::class);
        self::expectExceptionMessage('Invalid status transition of job');
        $client->updateJob($createdJob);
    }

    public function testUpdateJobDesiredStatus(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $terminatingJob = $client->getJobFactory()->modifyJob(
            $createdJob,
            ['desiredStatus' => JobFactory::DESIRED_STATUS_TERMINATING]
        );
        $client->updateJob($terminatingJob);

        $updatedJob = $client->getJob($createdJob->getId());

        self::assertEquals(JobFactory::STATUS_CREATED, $updatedJob->getStatus());
        self::assertEquals(JobFactory::DESIRED_STATUS_TERMINATING, $updatedJob->getDesiredStatus());
    }
}
