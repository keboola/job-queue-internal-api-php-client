<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\TestingEncryptorConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\ExistingJobFactory;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider\DataPlaneObjectEncryptorProvider;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use RuntimeException;

class ExistingJobFactoryTest extends BaseTest
{
    private static string $configId1;
    private const COMPONENT_ID_1 = 'keboola.runner-config-test';
    private static Client $client;
    private static string $projectId;

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        self::$client = new Client(
            [
                'token' => (string) getenv('TEST_STORAGE_API_TOKEN'),
                'url' => (string) getenv('TEST_STORAGE_API_URL'),
            ]
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
        putenv('AWS_ACCESS_KEY_ID=' . getenv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . getenv('TEST_AWS_SECRET_ACCESS_KEY'));
        putenv('AZURE_TENANT_ID=' . getenv('TEST_AZURE_TENANT_ID'));
        putenv('AZURE_CLIENT_ID=' . getenv('TEST_AZURE_CLIENT_ID'));
        putenv('AZURE_CLIENT_SECRET=' . getenv('TEST_AZURE_CLIENT_SECRET'));
    }

    private function getJobFactoryWithoutDataPlaneSupport(): array
    {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            (string) getenv('TEST_STORAGE_API_URL')
        ));

        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(new EncryptorOptions(
            (string) parse_url((string) getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST),
            (string) getenv('TEST_KMS_KEY_ID'),
            (string) getenv('TEST_KMS_REGION'),
            null,
            (string) getenv('TEST_AZURE_KEY_VAULT_URL'),
        ));

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $objectEncryptorProvider = new DataPlaneObjectEncryptorProvider(
            $objectEncryptor,
            $dataPlaneConfigRepository,
            false
        );

        $factory = new ExistingJobFactory(
            $storageClientFactory,
            $objectEncryptorProvider,
        );

        return [$factory, $objectEncryptor];
    }

    private function getJobFactoryWithDataPlaneSupport(bool $projectHasDataPlane): array
    {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            (string) getenv('TEST_STORAGE_API_URL')
        ));

        $controlPlaneObjectEncryptor = ObjectEncryptorFactory::getEncryptor(new EncryptorOptions(
            (string) parse_url((string) getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST),
            (string) getenv('TEST_KMS_KEY_ID'),
            (string) getenv('TEST_KMS_REGION'),
            null,
            (string) getenv('TEST_AZURE_KEY_VAULT_URL'),
        ));

        $dataPlaneObjectEncryptor = ObjectEncryptorFactory::getEncryptor(new EncryptorOptions(
            'custom-value',
            (string) getenv('TEST_KMS_KEY_ID'),
            (string) getenv('TEST_KMS_REGION'),
            null,
            (string) getenv('TEST_AZURE_KEY_VAULT_URL'),
        ));

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);

        if ($projectHasDataPlane) {
            $dataPlaneConfig = new DataPlaneConfig(
                'dataPlaneId',
                new KubernetesConfig('', '', '', ''),
                new TestingEncryptorConfig($dataPlaneObjectEncryptor),
            );

            $dataPlaneConfigRepository
                ->method('fetchProjectDataPlane')
                ->with(self::$projectId)
                ->willReturn($dataPlaneConfig)
            ;

            $dataPlaneConfigRepository
                ->method('fetchDataPlaneConfig')
                ->with('dataPlaneId')
                ->willReturn($dataPlaneConfig)
            ;
        } else {
            $dataPlaneConfigRepository
                ->method('fetchProjectDataPlane')
                ->willReturn(null)
            ;
        }

        $objectEncryptorProvider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            true
        );

        $factory = new ExistingJobFactory(
            $storageClientFactory,
            $objectEncryptorProvider,
        );

        return [$factory, $controlPlaneObjectEncryptor, $dataPlaneObjectEncryptor];
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
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
        ];

        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches(
            '#The child (node|config) "componentId" (at path|under) "job" must be configured.#'
        );

        $factory->loadFromExistingJobData($jobData);
    }

    public function testEncryptionExistingControlPlaneJob(): void
    {
        [$factory, $objectEncryptor] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            'id' => '123',
            'runId' => '123',
            'projectId' => self::$projectId,
            'tokenId' => '1234',
            'status' => JobFactory::STATUS_CREATED,
            'desiredStatus' => JobFactory::DESIRED_STATUS_PROCESSING,
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
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
                    (string) self::$configId1
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
            $job->getConfigDataDecrypted()
        );
    }

    public function testLoadExistingDataPlaneJob(): void
    {
        [$factory, , $dataPlaneObjectEncryptor] = $this->getJobFactoryWithDataPlaneSupport(true);

        $data = [
            'id' => '123',
            'runId' => '123',
            'projectId' => self::$projectId,
            'dataPlaneId' => 'dataPlaneId',
            'tokenId' => '1234',
            'status' => JobFactory::STATUS_CREATED,
            'desiredStatus' => JobFactory::DESIRED_STATUS_PROCESSING,
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'configData' => [
                '#foo1' => $dataPlaneObjectEncryptor->encryptForProject(
                    'bar1',
                    self::COMPONENT_ID_1,
                    self::$projectId,
                ),
                '#foo2' => $dataPlaneObjectEncryptor->encryptForComponent(
                    'bar2',
                    self::COMPONENT_ID_1,
                ),
                '#foo3' => $dataPlaneObjectEncryptor->encryptForConfiguration(
                    'bar3',
                    self::COMPONENT_ID_1,
                    self::$projectId,
                    (string) self::$configId1
                ),
            ],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ];
        $job = $factory->loadFromExistingJobData($data);
        self::assertNotEmpty($job->getId());
        self::assertSame(self::$configId1, $job->getConfigId());
        self::assertSame(getenv('TEST_STORAGE_API_TOKEN'), $job->getTokenString());

        self::assertStringStartsWith('KBC::ProjectSecure', $job->getConfigData()['#foo1']);
        self::assertStringStartsWith('KBC::ComponentSecure', $job->getConfigData()['#foo2']);
        self::assertStringStartsWith('KBC::ConfigSecure', $job->getConfigData()['#foo3']);
        self::assertEquals(
            [
                '#foo1' => 'bar1',
                '#foo2' => 'bar2',
                '#foo3' => 'bar3',
            ],
            $job->getConfigDataDecrypted()
        );
    }

    public function testLoadExistingDataPlaneJobWithoutDataPlaneSupport(): void
    {
        [$factory, $controlPlaneObjectEncryptor] = $this->getJobFactoryWithoutDataPlaneSupport();

        $data = [
            'id' => '123',
            'runId' => '123',
            'projectId' => self::$projectId,
            'dataPlaneId' => 'dataPlaneId',
            'tokenId' => '1234',
            'status' => JobFactory::STATUS_CREATED,
            'desiredStatus' => JobFactory::DESIRED_STATUS_PROCESSING,
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'configData' => [
                '#foo1' => $controlPlaneObjectEncryptor->encryptForProject(
                    'bar1',
                    self::COMPONENT_ID_1,
                    self::$projectId,
                ),
                '#foo2' => $controlPlaneObjectEncryptor->encryptForComponent(
                    'bar2',
                    self::COMPONENT_ID_1,
                ),
                '#foo3' => $controlPlaneObjectEncryptor->encryptForConfiguration(
                    'bar3',
                    self::COMPONENT_ID_1,
                    self::$projectId,
                    (string) self::$configId1
                ),
            ],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ];

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Can\'t provide dataPlane encryptor on stack without dataPlane support');

        $factory->loadFromExistingJobData($data);
    }
}
