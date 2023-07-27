<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\TestingEncryptorConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider\DataPlaneObjectEncryptorProvider;
use Keboola\JobQueueInternalClient\NewJobFactory;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\PermissionChecker\BranchType;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class NewJobFactoryTest extends BaseTest
{
    use TestEnvVarsTrait;
    use EncryptorOptionsTest;

    private const COMPONENT_ID_1 = 'keboola.runner-config-test';
    private const TEST_BRANCH_NAME = 'job-queue-new-job-factory-test';

    private static string $configId1;
    private static Client $client;
    private static string $projectId;
    private static string $defaultBranchId;
    private static string $devBranchId;
    private static string $componentId1Tag;

    private LoggerInterface $logger;
    private TestHandler $logsHandler;

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        $clientWrapper = new ClientWrapper(new ClientOptions(
            url: self::getRequiredEnv('TEST_STORAGE_API_URL'),
            token: self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
        ));

        self::$client = $clientWrapper->getBasicClient();
        self::$projectId = (string) self::$client->verifyToken()['owner']['id'];
        self::$defaultBranchId = $clientWrapper->getDefaultBranch()['branchId'];

        $devBranchesClient = new DevBranches(self::$client);
        foreach ($devBranchesClient->listBranches() as $branch) {
            if ($branch['name'] === self::TEST_BRANCH_NAME) {
                self::$devBranchId = (string) $branch['id'];
                break;
            }
        }
        if (!isset(self::$devBranchId)) {
            self::$devBranchId = (string) $devBranchesClient->createBranch(self::TEST_BRANCH_NAME)['id'];
        }

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

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('test', [$this->logsHandler]);
    }

    /**
     * @return array{NewJobFactory, ObjectEncryptor}
     */
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
            $this->logger,
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
            new NullLogger(),
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
        self::assertSame(self::$defaultBranchId, $job->getBranchId());
        self::assertSame(BranchType::DEFAULT, $job->getBranchType());
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
        self::assertSame(self::$defaultBranchId, $job->getBranchId());
        self::assertSame(BranchType::DEFAULT, $job->getBranchType());
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
        self::assertSame(self::$defaultBranchId, $job->getBranchId());
        self::assertSame(BranchType::DEFAULT, $job->getBranchType());
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

    public function testCreateJobBranchType(): void
    {
        // don't want to mock stuff here, functional test is more reliable in this case
        // can't use data provider because test depends on real resolved branch IDs

        [$factory] = $this->getJobFactoryWithoutDataPlaneSupport();

        // branchId null
        $this->logsHandler->reset();
        $job = $factory->createNewJob([
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => 'keboola.runner-config-test',
            'branchId' => null,
            'mode' => 'run',
        ]);
        self::assertSame(self::$defaultBranchId, $job->getBranchId());
        self::assertSame(BranchType::DEFAULT, $job->getBranchType());
        self::assertTrue($this->logsHandler->hasWarning(
            'Not setting branchId is deprecated, set actual branch ID',
        ));
        self::assertFalse($this->logsHandler->hasWarning(
            'Using branchId alias "default" is deprecated, set actual branch ID',
        ));

        // branchId default
        $this->logsHandler->reset();
        $job = $factory->createNewJob([
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => 'keboola.runner-config-test',
            'branchId' => 'default',
            'mode' => 'run',
        ]);
        self::assertSame(self::$defaultBranchId, $job->getBranchId());
        self::assertSame(BranchType::DEFAULT, $job->getBranchType());
        self::assertFalse($this->logsHandler->hasWarning(
            'Not setting branchId is deprecated, set actual branch ID',
        ));
        self::assertTrue($this->logsHandler->hasWarning(
            'Using branchId alias "default" is deprecated, set actual branch ID',
        ));

        // branchId numeric default
        $this->logsHandler->reset();
        $job = $factory->createNewJob([
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => 'keboola.runner-config-test',
            'branchId' => self::$defaultBranchId,
            'mode' => 'run',
        ]);
        self::assertSame(self::$defaultBranchId, $job->getBranchId());
        self::assertSame(BranchType::DEFAULT, $job->getBranchType());
        self::assertFalse($this->logsHandler->hasWarning(
            'Not setting branchId is deprecated, set actual branch ID',
        ));
        self::assertFalse($this->logsHandler->hasWarning(
            'Using branchId alias "default" is deprecated, set actual branch ID',
        ));

        // branchId numeric dev
        $this->logsHandler->reset();
        $job = $factory->createNewJob([
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => 'keboola.runner-config-test',
            'branchId' => self::$devBranchId,
            'mode' => 'run',
        ]);
        self::assertSame(self::$devBranchId, $job->getBranchId());
        self::assertSame(BranchType::DEV, $job->getBranchType());
        self::assertFalse($this->logsHandler->hasWarning(
            'Not setting branchId is deprecated, set actual branch ID',
        ));
        self::assertFalse($this->logsHandler->hasWarning(
            'Using branchId alias "default" is deprecated, set actual branch ID',
        ));
    }

    /** @dataProvider provideEncryptionPrefixes */
    public function testCreateJobEncryptionPrefix(
        array $features,
        string $expectedPrefix,
    ): void {
        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(self::getEncryptorOptions());

        // mock forwards all calls directly to a regular client, except token features which are faked
        $clientMock = $this->createPartialMock(Client::class, ['verifyToken', 'apiGet', 'apiPostJson']);
        $clientMock->method('verifyToken')->willReturnCallback(function () use ($features) {
            $tokenInfo = self::$client->verifyToken();
            $tokenInfo['owner']['features'] = $features;
            return $tokenInfo;
        });
        $clientMock->method('apiGet')->willReturnCallback(fn(...$args) => self::$client->apiGet(...$args));
        $clientMock->method('apiPostJson')->willReturnCallback(fn(...$args) => self::$client->apiPostJson(...$args));

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBasicClient')->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);

        $storageClientFactoryMock = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock->method('createClientWrapper')->willReturn($clientWrapperMock);

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
            new NullLogger(),
        );

        $data = [
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => 'keboola.runner-config-test',
            'mode' => 'run',
        ];
        $job = $factory->createNewJob($data);
        self::assertStringStartsWith($expectedPrefix, $job->getTokenString());
    }

    public function provideEncryptionPrefixes(): iterable
    {
        yield 'standard project' => [
            'features' => [],
            'expectedPrefix' => 'KBC::ProjectSecureKV::',
        ];

        yield 'SOX project' => [
            'features' => ['protected-default-branch'],
            'expectedPrefix' => 'KBC::BranchTypeSecureKV::',
        ];
    }
}
