<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Psr\Log\NullLogger;

abstract class BaseClientFunctionalTest extends BaseTest
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

    protected function getClient(?string $kmsKeyId = null, ?string $keyVaultUrl = null): Client
    {
        return new Client(
            new NullLogger(),
            $this->getJobFactory($kmsKeyId, $keyVaultUrl),
            (string) getenv('TEST_QUEUE_API_URL'),
            (string) getenv('TEST_QUEUE_API_TOKEN'),
        );
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
}
