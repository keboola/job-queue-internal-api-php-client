<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\ExistingJobFactory;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\PermissionChecker\BranchType;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

class ExistingJobFactoryTest extends BaseTest
{
    use TestEnvVarsTrait;
    use EncryptorOptionsTest;

    private static string $configId1;
    private const COMPONENT_ID_1 = 'keboola.runner-config-test';
    private static Client $client;
    private static string $projectId;

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        self::$client = new Client(
            [
                'token' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
                'url' => self::getRequiredEnv('TEST_STORAGE_API_URL'),
            ],
        );

        self::$projectId = (string) self::$client->verifyToken()['owner']['id'];

        $componentsApi = new Components(self::$client);
        $configuration = new Configuration();
        $configuration->setConfiguration([]);
        $configuration->setComponentId(self::COMPONENT_ID_1);
        $configuration->setName('ClientListConfigurationsJobsFunctionalTest');
        self::$configId1 = $componentsApi->addConfiguration($configuration)['id'];
    }

    public static function tearDownAfterClass(): void
    {
        parent::tearDownAfterClass();
        if (self::$configId1) {
            $componentsApi = new Components(self::$client);
            $componentsApi->deleteConfiguration(self::COMPONENT_ID_1, self::$configId1);
        }
    }

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
    }

    private function getJobFactory(): array
    {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            self::getRequiredEnv('TEST_STORAGE_API_URL'),
        ));

        $objectEncryptor = ObjectEncryptorFactory::getEncryptor($this->getEncryptorOptions());

        $factory = new ExistingJobFactory(
            $storageClientFactory,
            new JobObjectEncryptor($objectEncryptor),
        );

        return [$factory, $objectEncryptor];
    }

    public function testLoadInvalidJob(): void
    {
        $jobData = [
            'id' => '664651692',
            'runId' => '664651692',
            'status' => 'waiting',
            'desiredStatus' => 'processing',
            'mode' => 'run',
            'projectId' => '219',
            'tokenId' => '12345',
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
        ];

        [$factory] = $this->getJobFactory();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches(
            '#The child (node|config) "componentId" (at path|under) "job" must be configured.#',
        );

        $factory->loadFromExistingJobData($jobData);
    }

    public function testEncryptionExistingJob(): void
    {
        [$factory, $objectEncryptor] = $this->getJobFactory();
        $data = [
            'id' => '123',
            'runId' => '123',
            'projectId' => self::$projectId,
            'branchType' => BranchType::DEFAULT->value,
            'tokenId' => '1234',
            'status' => JobInterface::STATUS_CREATED,
            'desiredStatus' => JobInterface::DESIRED_STATUS_PROCESSING,
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'configData' => [
                '#foo1' => $objectEncryptor->encryptForProject(
                    'bar1',
                    self::COMPONENT_ID_1,
                    self::$projectId,
                ),
                '#foo2' => $objectEncryptor->encryptForComponent(
                    'bar2',
                    self::COMPONENT_ID_1,
                ),
                '#foo3' => $objectEncryptor->encryptForConfiguration(
                    'bar3',
                    self::COMPONENT_ID_1,
                    self::$projectId,
                    (string) self::$configId1,
                ),
            ],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ];

        $job = $factory->loadFromExistingJobData($data);

        self::assertStringStartsWith('KBC::ProjectSecure', $job->getConfigData()['#foo1']);
        self::assertStringStartsWith('KBC::ComponentSecure', $job->getConfigData()['#foo2']);
        self::assertStringStartsWith('KBC::ConfigSecure', $job->getConfigData()['#foo3']);
        self::assertEquals(
            [
                '#foo1' => 'bar1',
                '#foo2' => 'bar2',
                '#foo3' => 'bar3',
            ],
            $job->getConfigDataDecrypted(),
        );
    }
}
