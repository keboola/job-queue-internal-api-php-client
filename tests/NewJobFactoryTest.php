<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Generator;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\JobType;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\NewJobFactory;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApi\BranchAwareClient;
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
            ],
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
        putenv('GCP_KMS_KEY_ID=' . self::getRequiredEnv('TEST_GCP_KMS_KEY_ID'));
        putenv('GOOGLE_APPLICATION_CREDENTIALS=' . self::getRequiredEnv('TEST_GOOGLE_APPLICATION_CREDENTIALS'));
    }

    private function getJobFactory(): NewJobFactory
    {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            self::getRequiredEnv('TEST_STORAGE_API_URL'),
        ));

        $objectEncryptor = ObjectEncryptorFactory::getEncryptor($this->getEncryptorOptions());

        return new NewJobFactory(
            $storageClientFactory,
            new JobRuntimeResolver($storageClientFactory),
            new JobObjectEncryptor($objectEncryptor),
        );
    }

    public function testCreateNewJob(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertNull($job->getOrchestrationTaskId());
        self::assertNull($job->getOnlyOrchestrationTaskIds());
        self::assertNull($job->getPreviousJobId());
    }

    public function testCreateNewJobNormalize(): void
    {
        $factory = $this->getJobFactory();
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
        $factory = $this->getJobFactory();
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
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::STANDARD, $job->getType());
    }

    public function testCreateJobForceRun(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::STANDARD, $job->getType());
    }

    public function testCreateNewJobParallelismNumeric(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::ROW_CONTAINER, $job->getType());
    }

    public function testCreateNewJobParallelismInfinity(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::ROW_CONTAINER, $job->getType());
    }

    public function testCreateNewJobParallelismZero(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::STANDARD, $job->getType());
    }

    public function testCreateNewJobParallelismForcedType(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::STANDARD, $job->getType());
    }

    public function testCreateNewOrchestratorJob(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::ORCHESTRATION_CONTAINER, $job->getType());
    }

    public function testCreateNewOrchestratorPhaseJob(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::PHASE_CONTAINER, $job->getType());
    }

    public function testCreateNewOrchestratorPhaseJobZeroPhase(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::PHASE_CONTAINER, $job->getType());
    }

    public function testCreateNewOrchestratorPhaseJobEmptyPhase(): void
    {
        $factory = $this->getJobFactory();
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
        self::assertSame(JobType::ORCHESTRATION_CONTAINER, $job->getType());
    }

    public function testCreateNewJobInvalidVariables(): void
    {
        $factory = $this->getJobFactory();
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

    public function testCreateNewOrchestratorChildJob(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
            'configId' => self::$configId1,
            'orchestrationJobId' => '333',
            'orchestrationTaskId' => '444',
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertSame('keboola.runner-config-test', $job->getComponentId());
        self::assertSame('333', $job->getOrchestrationJobId());
        self::assertSame('444', $job->getOrchestrationTaskId());
    }

    public function testCreateRerunOrchestratorJob(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'componentId' => 'keboola.orchestrator',
            'mode' => 'run',
            'configData' => [],
            'previousJobId' => '1234',
            'onlyOrchestrationTaskIds' => ['444'],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertSame('keboola.orchestrator', $job->getComponentId());
        self::assertSame('1234', $job->getPreviousJobId());
        self::assertSame(['444'], $job->getOnlyOrchestrationTaskIds());
    }

    public function testCreateInvalidJob(): void
    {
        $jobData = [
            'configId' => '123',
            'componentId' => 'keboola.test',
            'mode' => 'run',
        ];
        $factory = $this->getJobFactory();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches(
            '#The child (node|config) "\#tokenString" (at path|under) "job" must be configured.#',
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
        $factory = $this->getJobFactory();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Cannot create job: "Invalid access token".');

        $factory->createNewJob($data);
    }

    public function testEncryption(): void
    {
        $objectEncryptor = ObjectEncryptorFactory::getEncryptor($this->getEncryptorOptions());

        $factory = $this->getJobFactory();
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
                    (string) self::$configId1,
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
            $job->getConfigDataDecrypted(),
        );
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
        $objectEncryptor = ObjectEncryptorFactory::getEncryptor($this->getEncryptorOptions());
        $basicClientMock = $this->createMock(Client::class);
        $basicClientMock->method('apiGet')
            ->willReturnCallback(function (...$args) use ($isDefault, &$trackingInvocationCount) {
                if ($args[0] === 'dev-branches/987') {
                    $trackingInvocationCount++;
                    return ['id' => '987', 'isDefault' => $isDefault];
                }
                return self::$client->apiGet(...$args);
            });
        $basicClientMock
            ->method('generateId')
            ->willReturnCallback(fn(...$args) => self::$client->generateId());
        $basicClientMock
            ->method('verifyToken')
            ->willReturnCallback(function () use ($features) {
                $tokenInfo = self::$client->verifyToken();
                $tokenInfo['owner']['features'] = $features;
                return $tokenInfo;
            });

        $branchClientMock = $this->createMock(BranchAwareClient::class);
        $branchClientMock
            ->method('apiGet')
            ->willReturnCallback(function (...$args) {
                return self::$client->apiGet(...$args);
            });

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock
            ->method('getBasicClient')
            ->willReturn($basicClientMock);
        $clientWrapperMock
            ->method('getBranchClient')
            ->willReturn($branchClientMock);
        $storageClientFactoryMock = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $factory = new NewJobFactory(
            $storageClientFactoryMock,
            new JobRuntimeResolver($storageClientFactoryMock),
            new JobObjectEncryptor($objectEncryptor),
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
