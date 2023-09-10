<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigValidator;
use Keboola\JobQueueInternalClient\ExistingJobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
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

        $objectEncryptorProvider = $this->getObjectEncryptorProvider($kmsKeyId, $keyVaultUrl, $gkmsKeyId);

        return new NewJobFactory(
            $storageClientFactory,
            new JobRuntimeResolver($storageClientFactory),
            $objectEncryptorProvider,
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

        $objectEncryptorProvider = $this->getObjectEncryptorProvider($kmsKeyId, $keyVaultUrl, $gkmsKeyId);

        $existingJobFactory = new ExistingJobFactory(
            $storageClientFactory,
            $objectEncryptorProvider,
        );

        return new Client(
            new NullLogger(),
            $existingJobFactory,
            self::getRequiredEnv('TEST_QUEUE_API_URL'),
            self::getRequiredEnv('TEST_QUEUE_API_TOKEN'),
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
    private function getObjectEncryptorProvider(
        ?string $kmsKeyId,
        ?string $keyVaultUrl,
        ?string $gkmsKeyId,
    ): DataPlaneObjectEncryptorProvider {
        $stackId = self::getRequiredEnv('TEST_STORAGE_API_URL');
        self::assertNotEmpty($stackId);

        $controlPlaneObjectEncryptor = ObjectEncryptorFactory::getEncryptor(
            new EncryptorOptions(
                $stackId,
                $kmsKeyId ?? self::getRequiredEnv('TEST_KMS_KEY_ID'),
                self::getRequiredEnv('TEST_KMS_REGION'),
                null,
                $keyVaultUrl ?? self::getRequiredEnv('TEST_AZURE_KEY_VAULT_URL'),
                $gkmsKeyId ?? self::getRequiredEnv('TEST_GCP_KMS_KEY_ID'),
            ),
        );

        $dataPlaneConfigRepository = new DataPlaneConfigRepository(
            new ManageApiClient([
                'url' => self::getRequiredEnv('TEST_STORAGE_API_URL'),
                'token' => '',
            ]),
            new DataPlaneConfigValidator(Validation::createValidator()),
            $stackId,
            self::getRequiredEnv('TEST_KMS_REGION'),
        );

        return new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            self::getOptionalEnv('SUPPORTS_DATA_PLANE') === 'true',
        );
    }
}
