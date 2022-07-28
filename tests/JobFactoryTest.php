<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\TestingEncryptorConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

class JobFactoryTest extends BaseTest
{
    private static string $configId1;
    private const COMPONENT_ID_1 = 'keboola.runner-config-test';
    private static Client $client;
    private static string $projectId;
    private static string $componentId1Tag;

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

        $component = $componentsApi->getComponent(self::COMPONENT_ID_1);
        self::$componentId1Tag = $component['data']['definition']['tag'];
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

        $factory = new JobFactory(
            $storageClientFactory,
            new JobFactory\JobRuntimeResolver($storageClientFactory),
            $objectEncryptor,
            $dataPlaneConfigRepository,
            false,
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

        $factory = new JobFactory(
            $storageClientFactory,
            new JobFactory\JobRuntimeResolver($storageClientFactory),
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            true,
        );

        return [$factory, $controlPlaneObjectEncryptor, $dataPlaneObjectEncryptor];
    }

    public function testCreateNewJob(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => 'keboola.runner-config-test',
            'mode' => 'run',
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals([], $job->getConfigData());
        self::assertEquals(getenv('TEST_STORAGE_API_TOKEN'), $job->getTokenDecrypted());
        self::assertEquals([], $job->getConfigDataDecrypted());
        self::assertIsArray($job->getConfigRowIds());
        self::assertEmpty($job->getConfigRowIds());
        self::assertSame(self::$componentId1Tag, $job->getTag());
        self::assertEquals($job->getId(), $job->getRunId());
        self::assertNull($job->getBranchId());
        self::assertNull($job->getOrchestrationJobId());
    }

    public function testCreateNewJobNormalize(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => (int) self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
            'tag' => 123,
            'configRowIds' => [123, 456],
            'parentRunId' => 1234.567,
            'orchestrationJobId' => 123456789,
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertSame([], $job->getConfigData());
        self::assertSame(getenv('TEST_STORAGE_API_TOKEN'), $job->getTokenDecrypted());
        self::assertSame([], $job->getConfigDataDecrypted());
        self::assertSame(self::$configId1, $job->getConfigId());
        self::assertSame(['123', '456'], $job->getConfigRowIds());
        self::assertSame('123', $job->getTag());
        self::assertSame('1234.567.' . $job->getId(), $job->getRunId());
        self::assertSame('1234.567', $job->getParentRunId());
        self::assertSame(self::COMPONENT_ID_1, $job->jsonSerialize()['componentId']);
        self::assertSame(['123', '456'], $job->jsonSerialize()['configRowIds']);
        self::assertSame('123', $job->jsonSerialize()['tag']);
        self::assertSame('1234.567.' . $job->getId(), $job->jsonSerialize()['runId']);
        self::assertNull($job->getBranchId());
        self::assertSame('123456789', $job->getOrchestrationJobId());
        self::assertEquals(['values' => []], $job->getVariableValuesData());
    }

    public function testCreateNewJobNormalizeBranch(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'componentId' => 123,
            'mode' => 'run',
            'tag' => 123,
            'configRowIds' => [123, 456],
            'parentRunId' => 1234.567,
            'branchId' => 1234,
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertSame([], $job->getConfigData());
        self::assertSame(getenv('TEST_STORAGE_API_TOKEN'), $job->getTokenDecrypted());
        self::assertSame([], $job->getConfigDataDecrypted());
        self::assertSame(null, $job->getConfigId());
        self::assertSame(['123', '456'], $job->getConfigRowIds());
        self::assertSame('123', $job->getTag());
        self::assertSame('1234.567.' . $job->getId(), $job->getRunId());
        self::assertSame('1234.567', $job->getParentRunId());
        self::assertSame('123', $job->jsonSerialize()['componentId']);
        self::assertSame(['123', '456'], $job->jsonSerialize()['configRowIds']);
        self::assertSame('123', $job->jsonSerialize()['tag']);
        self::assertSame('1234.567.' . $job->getId(), $job->jsonSerialize()['runId']);
        self::assertSame('1234', $job->getBranchId());
        self::assertEquals(['values' => []], $job->getVariableValuesData());
    }

    public function testCreateNewJobFull(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '2345',
            'componentId' => 'keboola.runner-config-test',
            'mode' => 'run',
            'configRowIds' => ['234'],
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'tag' => 'latest',
            'variableValuesData' => [
                'values' => [
                    [
                        'name' => 'bar',
                        'value' => 'Kochba',
                    ],
                ],
            ],
            'backend' => [
                'type' => 'custom',
            ],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals(['234'], $job->getConfigRowIds());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
        self::assertEquals('2345.' . $job->getId(), $job->getRunId());
        self::assertEquals(['values' => [['name' => 'bar', 'value' => 'Kochba']]], $job->getVariableValuesData());
        self::assertSame(null, $job->getBackend()->getType());
        self::assertSame('custom', $job->getBackend()->getContainerType());
        self::assertSame('standard', $job->getType());
    }

    public function testCreateJobForceRun(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'componentId' => 'keboola.runner-config-test',
            'tag' => 'latest',
            'mode' => 'forceRun',
            'configData' => [],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertSame('forceRun', $job->getMode());
        self::assertSame('standard', $job->getType());
    }

    public function testCreateNewJobParallelismNumeric(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '2345',
            'componentId' => 'keboola.runner-config-test',
            'tag' => 'latest',
            'mode' => 'run',
            'configData' => [],
            'parallelism' => '5',
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals([], $job->getConfigRowIds());
        self::assertEquals([], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
        self::assertEquals('2345.' . $job->getId(), $job->getRunId());
        self::assertEquals(['values' => []], $job->getVariableValuesData());
        self::assertSame(null, $job->getBackend()->getType());
        self::assertSame('container', $job->getType());
    }

    public function testCreateNewJobParallelismInfinity(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '2345',
            'componentId' => 'keboola.runner-config-test',
            'tag' => 'latest',
            'mode' => 'run',
            'configData' => [],
            'parallelism' => 'infinity',
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals([], $job->getConfigRowIds());
        self::assertEquals([], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
        self::assertEquals('2345.' . $job->getId(), $job->getRunId());
        self::assertEquals(['values' => []], $job->getVariableValuesData());
        self::assertSame(null, $job->getBackend()->getType());
        self::assertSame('container', $job->getType());
    }

    public function testCreateNewJobParallelismZero(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '2345',
            'componentId' => 'keboola.runner-config-test',
            'tag' => 'latest',
            'mode' => 'run',
            'configData' => [],
            'parallelism' => '0',
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals([], $job->getConfigRowIds());
        self::assertEquals([], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
        self::assertEquals('2345.' . $job->getId(), $job->getRunId());
        self::assertEquals(['values' => []], $job->getVariableValuesData());
        self::assertSame(null, $job->getBackend()->getType());
        self::assertSame('standard', $job->getType());
    }

    public function testCreateNewJobParallelismForcedType(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '2345',
            'componentId' => 'keboola.runner-config-test',
            'tag' => 'latest',
            'mode' => 'run',
            'configData' => [],
            'parallelism' => 'infinity',
            'type' => 'standard',
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals([], $job->getConfigRowIds());
        self::assertEquals([], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
        self::assertEquals('2345.' . $job->getId(), $job->getRunId());
        self::assertEquals(['values' => []], $job->getVariableValuesData());
        self::assertSame(null, $job->getBackend()->getType());
        self::assertSame('standard', $job->getType());
    }

    public function testCreateNewOrchestratorJob(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'componentId' => 'keboola.orchestrator',
            'tag' => 'latest',
            'mode' => 'run',
            'configData' => [],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals([], $job->getConfigData());
        self::assertEquals('keboola.orchestrator', $job->getComponentId());
        self::assertSame('orchestrationContainer', $job->getType());
    }

    public function testCreateNewOrchestratorPhaseJob(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'componentId' => 'keboola.orchestrator',
            'tag' => 'latest',
            'mode' => 'run',
            'configData' => ['phaseId' => '123'],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertEquals(['phaseId' => '123'], $job->getConfigData());
        self::assertEquals('keboola.orchestrator', $job->getComponentId());
        self::assertSame('phaseContainer', $job->getType());
    }

    public function testCreateNewOrchestratorPhaseJobZeroPhase(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'componentId' => 'keboola.orchestrator',
            'tag' => 'latest',
            'mode' => 'run',
            'configData' => ['phaseId' => 0],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertEquals(['phaseId' => 0], $job->getConfigData());
        self::assertEquals('keboola.orchestrator', $job->getComponentId());
        self::assertSame('phaseContainer', $job->getType());
    }

    public function testCreateNewOrchestratorPhaseJobEmptyPhase(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'componentId' => 'keboola.orchestrator',
            'tag' => 'latest',
            'mode' => 'run',
            'configData' => ['phaseId' => ''],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertEquals(['phaseId' => ''], $job->getConfigData());
        self::assertEquals('keboola.orchestrator', $job->getComponentId());
        self::assertSame('orchestrationContainer', $job->getType());
    }

    public function testCreateNewJobInvalidVariables(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '2345',
            'configData' => [],
            'componentId' => 'keboola.runner-config-test',
            'mode' => 'run',
            'variableValuesId' => '1234',
            'variableValuesData' => [
                'values' => [
                    [
                        'name' => 'bar',
                        'value' => 'Kochba',
                    ],
                ],
            ],
        ];
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Provide either "variableValuesId" or "variableValuesData", but not both.');
        $factory->createNewJob($data);
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

    public function testStaticGetters(): void
    {
        self::assertCount(5, JobFactory::getFinishedStatuses());
        self::assertCount(9, JobFactory::getAllStatuses());
        self::assertCount(3, JobFactory::getLegacyComponents());
        self::assertCount(3, JobFactory::getKillableStatuses());
    }

    public function testModifyJob(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'componentId' => 'keboola.runner-config-test',
            'mode' => 'run',
            'tag' => 'latest',
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
        ];
        $job = $factory->createNewJob($data);
        $newJob = $factory->modifyJob($job, ['configId' => '345', 'status' => 'waiting']);
        self::assertNotEmpty($job->getId());
        self::assertEquals('345', $newJob->getConfigId());
        self::assertNull($job->getConfigId());
        self::assertEquals('waiting', $newJob->getStatus());
        self::assertEquals('created', $job->getStatus());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertIsArray($job->getConfigRowIds());
        self::assertEmpty($job->getConfigRowIds());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
    }

    public function testCreateInvalidJob(): void
    {
        $jobData = [
            'configId' => '123',
            'componentId' => 'keboola.test',
            'mode' => 'run',
        ];
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches(
            '#The child (node|config) "\#tokenString" (at path|under) "job" must be configured.#'
        );

        $factory->createNewJob($jobData);
    }

    public function testCreateInvalidToken(): void
    {
        $data = [
            '#tokenString' => 'invalid',
            'configId' => '123',
            'componentId' => 'keboola.test',
            'mode' => 'run',
        ];
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Cannot create job: "Invalid access token".');

        $factory->createNewJob($data);
    }

    public function testEncryption(): void
    {
        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(new EncryptorOptions(
            (string) parse_url((string) getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST),
            (string) getenv('TEST_KMS_KEY_ID'),
            (string) getenv('TEST_KMS_REGION'),
            null,
            (string) getenv('TEST_AZURE_KEY_VAULT_URL')
        ));

        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
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
                '#foo4' => 'bar4',
            ],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ];
        $job = $factory->createNewJob($data);
        // these are encrypted manually and left as is
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getConfigData()['#foo1']);
        self::assertStringStartsWith('KBC::ComponentSecure', $job->getConfigData()['#foo2']);
        self::assertStringStartsWith('KBC::ConfigSecure', $job->getConfigData()['#foo3']);
        // this is encrypted automatically
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getConfigData()['#foo4']);
        // all are decrypted successfully
        self::assertEquals(
            [
                '#foo1' => 'bar1',
                '#foo2' => 'bar2',
                '#foo3' => 'bar3',
                '#foo4' => 'bar4',
            ],
            $job->getConfigDataDecrypted()
        );
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

    public function testCreateNewControlPlaneJob(): void
    {
        [$factory, $controlPlaneObjectEncryptor] = $this->getJobFactoryWithDataPlaneSupport(false);

        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '2345',
            'componentId' => 'keboola.runner-config-test',
            'mode' => 'run',
            'configRowIds' => ['234'],
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'tag' => 'latest',
            'variableValuesData' => [
                'values' => [
                    [
                        'name' => 'bar',
                        'value' => 'Kochba',
                    ],
                ],
            ],
            'backend' => [
                'type' => 'custom',
            ],
        ];

        $job = $factory->createNewJob($data);

        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals(['234'], $job->getConfigRowIds());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
        self::assertEquals('2345.' . $job->getId(), $job->getRunId());
        self::assertEquals(['values' => [['name' => 'bar', 'value' => 'Kochba']]], $job->getVariableValuesData());
        self::assertNull($job->getBackend()->getType());
        self::assertSame('custom', $job->getBackend()->getContainerType());
        self::assertSame('standard', $job->getType());

        $decodedToken = $controlPlaneObjectEncryptor->decryptForProject(
            $job->getTokenString(),
            'keboola.runner-config-test',
            self::$projectId
        );
        self::assertSame($decodedToken, $job->getTokenDecrypted());
    }

    public function testCreateNewDataPlaneJob(): void
    {
        [$factory, , $dataPlaneObjectEncryptor] = $this->getJobFactoryWithDataPlaneSupport(true);

        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '2345',
            'componentId' => 'keboola.runner-config-test',
            'mode' => 'run',
            'configRowIds' => ['234'],
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'tag' => 'latest',
            'variableValuesData' => [
                'values' => [
                    [
                        'name' => 'bar',
                        'value' => 'Kochba',
                    ],
                ],
            ],
            'backend' => [
                'type' => 'custom',
            ],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals(['234'], $job->getConfigRowIds());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
        self::assertEquals('2345.' . $job->getId(), $job->getRunId());
        self::assertEquals(['values' => [['name' => 'bar', 'value' => 'Kochba']]], $job->getVariableValuesData());
        self::assertNull($job->getBackend()->getType());
        self::assertSame('custom', $job->getBackend()->getContainerType());
        self::assertSame('standard', $job->getType());

        $decodedToken = $dataPlaneObjectEncryptor->decryptForProject(
            $job->getTokenString(),
            'keboola.runner-config-test',
            self::$projectId
        );
        self::assertSame($decodedToken, $job->getTokenDecrypted());
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
}
