<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\ExistingJobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobPatchData;
use Keboola\JobQueueInternalClient\NewJobFactory;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Psr\Log\NullLogger;

abstract class BaseClientFunctionalTest extends BaseTest
{
    use TestEnvVarsTrait;
    use EncryptorOptionsTest;

    public function setUp(): void
    {
        parent::setUp();
        putenv('AWS_ACCESS_KEY_ID=' . self::getRequiredEnv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . self::getRequiredEnv('TEST_AWS_SECRET_ACCESS_KEY'));
        putenv('AZURE_TENANT_ID=' . self::getRequiredEnv('TEST_AZURE_TENANT_ID'));
        putenv('AZURE_CLIENT_ID=' . self::getRequiredEnv('TEST_AZURE_CLIENT_ID'));
        putenv('AZURE_CLIENT_SECRET=' . self::getRequiredEnv('TEST_AZURE_CLIENT_SECRET'));
        putenv('GCP_KMS_KEY_ID=' . self::getRequiredEnv('TEST_GCP_KMS_KEY_ID'));
        putenv('GOOGLE_APPLICATION_CREDENTIALS=' . self::getRequiredEnv('TEST_GOOGLE_APPLICATION_CREDENTIALS'));
        $this->cleanJobs();
    }

    /**
     * @param non-empty-string|null $kmsKeyId
     * @param non-empty-string|null $keyVaultUrl
     * @param non-empty-string|null $gkmsKeyId
     */
    protected function getNewJobFactory(
        ?string $kmsKeyId = null,
        ?string $keyVaultUrl = null,
        ?string $gkmsKeyId = null,
    ): NewJobFactory {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            self::getRequiredEnv('TEST_STORAGE_API_URL'),
        ));

        return new NewJobFactory(
            $storageClientFactory,
            new JobRuntimeResolver($storageClientFactory),
            $this->getJobObjectEncryptor($kmsKeyId, $keyVaultUrl, $gkmsKeyId),
        );
    }

    /**
     * @param non-empty-string|null $kmsKeyId
     * @param non-empty-string|null $keyVaultUrl
     * @param non-empty-string|null $gkmsKeyId
     */
    protected function getClient(
        ?string $kmsKeyId = null,
        ?string $keyVaultUrl = null,
        ?string $gkmsKeyId = null,
    ): Client {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            self::getRequiredEnv('TEST_STORAGE_API_URL'),
        ));

        $existingJobFactory = new ExistingJobFactory(
            $storageClientFactory,
            $this->getJobObjectEncryptor($kmsKeyId, $keyVaultUrl, $gkmsKeyId),
        );

        return new Client(
            new NullLogger(),
            $existingJobFactory,
            self::getRequiredEnv('TEST_QUEUE_API_URL'),
            self::getRequiredEnv('TEST_QUEUE_API_TOKEN'),
            null,
            null,
        );
    }

    private function cleanJobs(): void
    {
        // cancel all created jobs
        $client = $this->getClient();
        $response = $client->getJobsWithStatus([JobInterface::STATUS_CREATED]);
        /** @var Job $job */
        foreach ($response as $job) {
            $client->patchJob($job->getId(), (new JobPatchData())->setStatus(JobInterface::STATUS_CANCELLED));
        }
    }

    /**
     * @param non-empty-string|null $kmsKeyId
     * @param non-empty-string|null $keyVaultUrl
     * @param non-empty-string|null $gkmsKeyId
     */
    private function getJobObjectEncryptor(
        ?string $kmsKeyId,
        ?string $keyVaultUrl,
        ?string $gkmsKeyId,
    ): JobObjectEncryptor {
        $stackId = self::getRequiredEnv('TEST_STORAGE_API_URL');
        self::assertNotEmpty($stackId);

        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(
            new EncryptorOptions(
                $stackId,
                $kmsKeyId ?? self::getRequiredEnv('TEST_KMS_KEY_ID'),
                self::getRequiredEnv('TEST_KMS_REGION'),
                null,
                $keyVaultUrl ?? self::getRequiredEnv('TEST_AZURE_KEY_VAULT_URL'),
                $gkmsKeyId ?? self::getRequiredEnv('TEST_GCP_KMS_KEY_ID'),
            ),
        );

        return new JobObjectEncryptor($objectEncryptor);
    }
}
