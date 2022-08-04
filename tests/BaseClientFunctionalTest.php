<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigValidator;
use Keboola\JobQueueInternalClient\ExistingJobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider\DataPlaneObjectEncryptorProvider;
use Keboola\JobQueueInternalClient\JobPatchData;
use Keboola\JobQueueInternalClient\NewJobFactory;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Psr\Log\NullLogger;
use Symfony\Component\Validator\Validation;

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

    protected function getNewJobFactory(?string $kmsKeyId = null, ?string $keyVaultUrl = null): NewJobFactory
    {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            (string) getenv('TEST_STORAGE_API_URL')
        ));

        $objectEncryptorProvider = $this->getObjectEncryptorProvider($kmsKeyId, $keyVaultUrl);

        return new NewJobFactory(
            $storageClientFactory,
            new JobRuntimeResolver($storageClientFactory),
            $objectEncryptorProvider
        );
    }

    protected function getClient(?string $kmsKeyId = null, ?string $keyVaultUrl = null): Client
    {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            (string) getenv('TEST_STORAGE_API_URL')
        ));

        $objectEncryptorProvider = $this->getObjectEncryptorProvider($kmsKeyId, $keyVaultUrl);

        $existingJobFactory = new ExistingJobFactory(
            $storageClientFactory,
            $objectEncryptorProvider
        );

        return new Client(
            new NullLogger(),
            $existingJobFactory,
            (string) getenv('TEST_QUEUE_API_URL'),
            (string) getenv('TEST_QUEUE_API_TOKEN'),
        );
    }

    private function cleanJobs(): void
    {
        // cancel all created jobs
        $client = $this->getClient();
        $response = $client->getJobsWithStatus([Job::STATUS_CREATED]);
        /** @var Job $job */
        foreach ($response as $job) {
            $client->patchJob($job->getId(), (new JobPatchData())->setStatus(Job::STATUS_CANCELLED));
        }
    }

    private function getObjectEncryptorProvider(
        ?string $kmsKeyId,
        ?string $keyVaultUrl
    ): DataPlaneObjectEncryptorProvider {
        $controlPlaneObjectEncryptor = ObjectEncryptorFactory::getEncryptor(
            new EncryptorOptions(
                (string) parse_url((string) getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST),
                $kmsKeyId ?? (string) getenv('TEST_KMS_KEY_ID'),
                (string) getenv('TEST_KMS_REGION'),
                null,
                $keyVaultUrl ?? (string) getenv('TEST_AZURE_KEY_VAULT_URL'),
            )
        );

        $dataPlaneConfigRepository = new DataPlaneConfigRepository(
            new ManageApiClient([
                'url' => (string) getenv('TEST_STORAGE_API_URL'),
                'token' => (string) getenv('TEST_MANAGE_API_TOKEN'),
            ]),
            new DataPlaneConfigValidator(Validation::createValidator()),
            (string) parse_url((string) getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST),
            (string) getenv('TEST_KMS_REGION'),
        );

        return new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            getenv('SUPPORTS_DATA_PLANE') === 'true',
        );
    }
}
