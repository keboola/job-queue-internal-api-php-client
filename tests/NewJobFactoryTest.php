<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Generator;
use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\TestingEncryptorConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider\DataPlaneObjectEncryptorProvider;
use Keboola\JobQueueInternalClient\NewJobFactory;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

class NewJobFactoryTest extends BaseTest
{
    use TestEnvVarsTrait;
    use EncryptorOptionsTest;

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
                'token' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
                'url' => self::getRequiredEnv('TEST_STORAGE_API_URL'),
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
        putenv('AWS_ACCESS_KEY_ID=' . self::getRequiredEnv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . self::getRequiredEnv('TEST_AWS_SECRET_ACCESS_KEY'));
        putenv('AZURE_TENANT_ID=' . self::getRequiredEnv('TEST_AZURE_TENANT_ID'));
        putenv('AZURE_CLIENT_ID=' . self::getRequiredEnv('TEST_AZURE_CLIENT_ID'));
        putenv('AZURE_CLIENT_SECRET=' . self::getRequiredEnv('TEST_AZURE_CLIENT_SECRET'));
    }

    private function getJobFactoryWithoutDataPlaneSupport(): array
    {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            self::getRequiredEnv('TEST_STORAGE_API_URL')
        ));

        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(self::getEncryptorOptions());

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $objectEncryptorProvider = new DataPlaneObjectEncryptorProvider(
            $objectEncryptor,
            $dataPlaneConfigRepository,
            false
        );

        $factory = new NewJobFactory(
            $storageClientFactory,
            new JobRuntimeResolver($storageClientFactory),
            $objectEncryptorProvider,
        );

        return [$factory, $objectEncryptor];
    }

    private function getJobFactoryWithDataPlaneSupport(bool $projectHasDataPlane): array
    {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            self::getRequiredEnv('TEST_STORAGE_API_URL')
        ));

        $controlPlaneObjectEncryptor = ObjectEncryptorFactory::getEncryptor(self::getEncryptorOptions());

        $dataPlaneObjectEncryptor = ObjectEncryptorFactory::getEncryptor(new EncryptorOptions(
            'custom-value',
            self::getRequiredEnv('TEST_KMS_KEY_ID'),
            self::getRequiredEnv('TEST_KMS_REGION'),
            null,
            self::getRequiredEnv('TEST_AZURE_KEY_VAULT_URL'),
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

        $factory = new NewJobFactory(
            $storageClientFactory,
            new JobRuntimeResolver($storageClientFactory),
            $objectEncryptorProvider,
        );

        return [$factory, $controlPlaneObjectEncryptor, $dataPlaneObjectEncryptor];
    }

    public function testCreateNewJob(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => 'keboola.runner-config-test',
            'mode' => 'run',
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertNull($job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertEquals([], $job->getConfigData());
        self::assertEquals(self::getRequiredEnv('TEST_STORAGE_API_TOKEN'), $job->getTokenDecrypted());
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
        self::assertSame(self::getRequiredEnv('TEST_STORAGE_API_TOKEN'), $job->getTokenDecrypted());
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
            'tag' => 123,
            'configRowIds' => [123, 456],
            'parentRunId' => 1234.567,
            'branchId' => 'default',
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getTokenString());
        self::assertSame([], $job->getConfigData());
        self::assertSame(self::getRequiredEnv('TEST_STORAGE_API_TOKEN'), $job->getTokenDecrypted());
        self::assertSame([], $job->getConfigDataDecrypted());
        self::assertSame(null, $job->getConfigId());
        self::assertSame(['123', '456'], $job->getConfigRowIds());
        self::assertSame('123', $job->getTag());
        self::assertSame('1234.567.' . $job->getId(), $job->getRunId());
        self::assertSame('1234.567', $job->getParentRunId());
        self::assertSame(self::COMPONENT_ID_1, $job->jsonSerialize()['componentId']);
        self::assertSame(['123', '456'], $job->jsonSerialize()['configRowIds']);
        self::assertSame('123', $job->jsonSerialize()['tag']);
        self::assertSame('1234.567.' . $job->getId(), $job->jsonSerialize()['runId']);
        self::assertSame('default', $job->getBranchId());
        self::assertEquals(['values' => []], $job->getVariableValuesData());
    }

    public function testCreateNewJobFull(): void
    {
        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(self::getEncryptorOptions());

        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();
        $data = [
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

    public function testCreateNewControlPlaneJob(): void
    {
        [$factory, $controlPlaneObjectEncryptor] = $this->getJobFactoryWithDataPlaneSupport(false);

        $data = [
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
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


    /** @dataProvider provideBranchIds */
    public function testCreateJobBranchType(
        ?string $branchId,
        bool $isDefault,
        array $features,
        int $invocationCount,
        string $expectedBranchType,
        string $expectedPrefix,
    ): void {
        $trackingInvocationCount = 0;
        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(self::getEncryptorOptions());
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->method('verifyToken')
            ->willReturnCallback(function () use ($features) {
                $tokenInfo = self::$client->verifyToken();
                $tokenInfo['owner']['features'] = $features;
                return $tokenInfo;
            });
        $clientMock
            ->method('apiGet')
            ->willReturnCallback(function (...$args) use ($isDefault, &$trackingInvocationCount) {
                if ($args[0] === 'dev-branches/987') {
                    $trackingInvocationCount++;
                    return ['id' => '987', 'isDefault' => $isDefault];
                }
                return self::$client->apiGet(...$args);
            });
        $clientMock
            ->method('generateId')
            ->willReturnCallback(fn(...$args) => self::$client->generateId());
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock
            ->method('getBasicClient')
            ->willReturn($clientMock);
        $clientWrapperMock
            ->method('getBranchClient')
            ->willReturn($clientMock);
        $storageClientFactoryMock = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $objectEncryptorProvider = new DataPlaneObjectEncryptorProvider(
            $objectEncryptor,
            $dataPlaneConfigRepository,
            false
        );

        $factory = new NewJobFactory(
            $storageClientFactoryMock,
            new JobRuntimeResolver($storageClientFactoryMock),
            $objectEncryptorProvider,
        );

        $data = [
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => 'keboola.runner-config-test',
            'branchId' => $branchId,
            'mode' => 'run',
        ];
        $job = $factory->createNewJob($data);
        self::assertSame($expectedBranchType, $job->getBranchType()->value);
        self::assertSame($invocationCount, $trackingInvocationCount);
        self::assertStringStartsWith($expectedPrefix, $job->getTokenString());
    }

    public function provideBranchIds(): Generator
    {
        yield 'branch id null' => [
            'branchId' => null,
            'isDefault' => true,
            'features' => [],
            'invocationCount' => 0,
            'expectedBranchType' => 'default',
            'expectedPrefix' => 'KBC::ProjectSecureKV::',
        ];
        yield 'branch id default' => [
            'branchId' => 'default',
            'isDefault' => true,
            'features' => [],
            'invocationCount' => 0,
            'expectedBranchType' => 'default',
            'expectedPrefix' => 'KBC::ProjectSecureKV::',
        ];
        yield 'branch id numeric dev' => [
            'branchId' => '987',
            'isDefault' => false,
            'features' => [],
            'invocationCount' => 1,
            'expectedBranchType' => 'dev',
            'expectedPrefix' => 'KBC::ProjectSecureKV::',
        ];
        yield 'branch id numeric default' => [
            'branchId' => '987',
            'isDefault' => true,
            'features' => [],
            'invocationCount' => 1,
            'expectedBranchType' => 'default',
            'expectedPrefix' => 'KBC::ProjectSecureKV::',
        ];

        yield 'branch id null with feature' => [
            'branchId' => null,
            'isDefault' => true,
            'features' => ['protected-default-branch'],
            'invocationCount' => 0,
            'expectedBranchType' => 'default',
            'expectedPrefix' => 'KBC::BranchTypeSecureKV::',
        ];
        yield 'branch id default with feature' => [
            'branchId' => 'default',
            'isDefault' => true,
            'features' => ['protected-default-branch'],
            'invocationCount' => 0,
            'expectedBranchType' => 'default',
            'expectedPrefix' => 'KBC::BranchTypeSecureKV::',
        ];
        yield 'branch id numeric dev with feature' => [
            'branchId' => '987',
            'isDefault' => false,
            'features' => ['protected-default-branch'],
            'invocationCount' => 1,
            'expectedBranchType' => 'dev',
            'expectedPrefix' => 'KBC::BranchTypeSecureKV::',
        ];
        yield 'branch id numeric default with feature' => [
            'branchId' => '987',
            'isDefault' => true,
            'features' => ['protected-default-branch'],
            'invocationCount' => 1,
            'expectedBranchType' => 'default',
            'expectedPrefix' => 'KBC::BranchTypeSecureKV::',
        ];
    }
}
