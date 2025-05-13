<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Generator;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\ConfigurationDisabledException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\JobType;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;

class JobRuntimeResolverTest extends TestCase
{
    private const DEFAULT_BRANCH_ID = '9999';

    /** @var array */
    private const JOB_DATA = [
        'id' => '123456456',
        'runId' => '123456456',
        'configId' => '454124290',
        'componentId' => 'keboola.ex-db-snowflake',
        'mode' => 'run',
        'status' => 'created',
        'desiredStatus' => 'processing',
        'projectId' => '123',
        'tokenId' => '456',
        '#tokenString' => 'KBC::ProjectSecure::token',
        'backend' => null,
        'branchId' => 'default',
    ];

    public function resolveDefaultBackendContextData(): Generator
    {
        yield 'standard job' => [
            'keboola.ex-db-snowflake',
            [],
            JobType::STANDARD,
            '123-dummy-component-type',
        ];
        yield 'row container job' => [
            'keboola.ex-db-snowflake',
            [
                'parallelism' => '5',
            ],
            JobType::ROW_CONTAINER,
            null,
        ];
        yield 'phase container job' => [
            JobFactory::ORCHESTRATOR_COMPONENT,
            [
                'configData' => [
                    'phaseId' => '123',
                ],
            ],
            JobType::PHASE_CONTAINER,
            null,
        ];
        yield 'orchestration job' => [
            JobFactory::ORCHESTRATOR_COMPONENT,
            [],
            JobType::ORCHESTRATION_CONTAINER,
            null,
        ];
        yield 'flow job' => [
            JobFactory::FLOW_COMPONENT,
            [],
            JobType::ORCHESTRATION_CONTAINER,
            null,
        ];
    }

    /**
     * @dataProvider resolveDefaultBackendContextData
     */
    public function testResolveDefaultBackendContext(
        string $componentId,
        array $customJobData,
        JobType $expectedJobType,
        ?string $expectedContext,
    ): void {
        $jobData = $this::JOB_DATA;
        unset($jobData['configId']);
        $jobData['componentId'] = $componentId;
        $jobData['configData'] = [
            'parameters' => ['foo' => 'bar'],
        ];
        $jobData = array_merge($jobData, $customJobData);
        $componentData = [
            'type' => 'dummy-component-type',
            'id' => $componentId,
            'data' => [
                'definition' => [
                    'tag' => '9.9.9',
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())->method('apiGet')
            ->withConsecutive(
                [sprintf('components/%s', $componentId)],
            )->willReturnOnConsecutiveCalls(
                $componentData,
            );
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClient')->willReturn($storageClient);
        $storageClientFactoryMock = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

        $jobData = $jobRuntimeResolver->resolveJobData($jobData, $this->createToken());
        self::assertSame($expectedJobType->value, $jobData['type']);
        self::assertSame($expectedContext, $jobData['backend']['context']);
    }

    public function testResolveRuntimeSettingsInJob(): void
    {
        $jobData = $this::JOB_DATA;
        $jobData['tag'] = '1.2.3';
        $jobData['variableValuesData'] = [
            'values' => [
                [
                    'name' => 'foo',
                    'value' => 'bar',
                ],
            ],
        ];
        $jobData['backend'] = [
            'type' => 'custom',
            'context' => 'wml',
        ];
        $jobData['parallelism'] = '5';
        $componentData = [
            'data' => [
                'staging_storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ];

        $configuration = [
            'id' => '454124290',
            'isDisabled' => false,
            'configuration' => [
                'runtime' => [
                    'tag' => '4.5.6',
                    'parallelism' => '5',
                ],
                'parameters' => ['foo' => 'bar'],
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'bar',
                            'value' => 'Kochba',
                        ],
                    ],
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnOnConsecutiveCalls(
                $componentData,
                $configuration,
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );

        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => '454124290',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => 'custom',
                    'context' => 'wml',
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'tag' => '1.2.3',
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'foo',
                            'value' => 'bar',
                        ],
                    ],
                ],
                'parallelism' => '5',
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'container',
                'variableValuesId' => null,
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function testResolveRuntimeSettingsInConfigData(): void
    {
        $jobData = self::JOB_DATA;
        unset($jobData['configId']);
        $jobData['configData'] = [
            'variableValuesId' => '123',
            'runtime' => [
                'tag' => '3.2.1',
                'backend' => [
                    'type' => 'mass-produced',
                    'context' => 'wml',
                ],
                'parallelism' => '5',
            ],
            'parameters' => ['foo' => 'bar'],
        ];
        $componentData = [
            'data' => [
                'staging_storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
            )->willReturnOnConsecutiveCalls(
                $componentData,
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );

        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => 'mass-produced',
                    'context' => 'wml',
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'configData' => [
                    'variableValuesId' => '123',
                    'runtime' => [
                        'tag' => '3.2.1',
                        'backend' => [
                            'type' => 'mass-produced',
                            'context' => 'wml',
                        ],
                        'parallelism' => '5',
                    ],
                    'parameters' => [
                        'foo' => 'bar',
                    ],
                ],
                'tag' => '3.2.1',
                'parallelism' => '5',
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'container',
                'variableValuesId' => '123',
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function testResolveRuntimeSettingsInConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        $configuration = [
            'id' => '454124290',
            'isDisabled' => false,
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'stereotyped',
                        'context' => 'wml',
                    ],
                    'tag' => '4.5.6',
                    'parallelism' => '5',
                ],
                'parameters' => ['foo' => 'bar'],
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'bar',
                            'value' => 'Kochba',
                        ],
                    ],
                ],
            ],
        ];

        $componentData = [
            'data' => [
                'staging_storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnOnConsecutiveCalls(
                $componentData,
                $configuration,
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );

        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => '454124290',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => 'stereotyped',
                    'context' => 'wml',
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'tag' => '4.5.6',
                'parallelism' => '5',
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'container',
                'variableValuesId' => null,
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'bar',
                            'value' => 'Kochba',
                        ],
                    ],
                ],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function testResolveRuntimeSettingsInConfigurationOfTransformations(): void
    {
        $jobData = self::JOB_DATA;
        $configuration = [
            'id' => '454124290',
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'stereotyped',
                        'context' => 'wml',
                    ],
                    'image_tag' => '4.5.6',
                    'parallelism' => '5',
                ],
            ],
        ];
        $componentData = [
            'data' => [
                'staging_storage' => [
                    'input' => 'workspace-snowflake',
                    'output' => 'workspace-snowflake',
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnOnConsecutiveCalls(
                $componentData,
                $configuration,
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => '454124290',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => 'stereotyped',
                    'containerType' => null,
                    'context' => 'wml',
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'tag' => '4.5.6',
                'parallelism' => '5',
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'container',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function testResolveRuntimeSettingsPriority(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['variableValuesId'] = '123';
        $jobData['configData'] = [
            'variableValuesId' => '456',
            'runtime' => [
                'tag' => '4.5.6',
                'parallelism' => '0',
            ],
            'parameters' => ['foo' => 'bar'],
        ];
        $configuration = [
            'id' => '454124290',
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'stereotyped',
                    ],
                    'tag' => '7.8.9',
                    'parallelism' => '3',
                ],
                'parameters' => ['foo' => 'bar'],
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'bar',
                            'value' => 'Kochba',
                        ],
                    ],
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnOnConsecutiveCalls(
                $this->getTestComponentData(),
                $configuration,
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => '454124290',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => 'stereotyped',
                    'context' => '123-extractor',
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'variableValuesId' => '123',
                'configData' => [
                    'variableValuesId' => '456',
                    'runtime' => [
                        'tag' => '4.5.6',
                        'parallelism' => '0',
                    ],
                    'parameters' => [
                        'foo' => 'bar',
                    ],
                ],
                'tag' => '4.5.6',
                'parallelism' => '0',
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function testResolveRuntimeSettingsNowhere(): void
    {
        $jobData = self::JOB_DATA;
        $configuration = [
            'id' => '454124290',
            'configuration' => [
                'parameters' => ['foo' => 'bar'],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnOnConsecutiveCalls(
                $this->getTestExtractorComponentData(),
                $configuration,
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );

        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => '454124290',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => null,
                    'context' => '123-extractor',
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'tag' => '9.9.9',
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    /** @dataProvider provideExecutorResolutionTestData */
    public function testResolveExecutor(
        array $features,
        array $configuration,
        array $configData,
        array $jobData,
        string $expectedResult,
    ): void {
        $jobData = array_merge(self::JOB_DATA, $jobData);
        $jobData['configData'] = $configData;

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))
            ->method('apiGet')
            ->with(self::callback(function (...$args) {
                static $expectedArgs = [
                    ['components/keboola.ex-db-snowflake'],
                    ['components/keboola.ex-db-snowflake/configs/454124290'],
                ];

                return count($expectedArgs) > 0 && $args === array_shift($expectedArgs);
            }))
            ->willReturnOnConsecutiveCalls(
                $this->getTestExtractorComponentData(),
                $configuration,
            )
        ;

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        $actualResolvedData = $jobRuntimeResolver->resolveJobData($jobData, $this->createToken($features));

        $expectedResolvedData = [
            'id' => '123456456',
            'runId' => '123456456',
            'configId' => '454124290',
            'branchId' => self::DEFAULT_BRANCH_ID,
            'branchType' => 'default',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
            'status' => 'created',
            'desiredStatus' => 'processing',
            'projectId' => '123',
            'tokenId' => '456',
            '#tokenString' => 'KBC::ProjectSecure::token',
            'backend' => [
                'type' => null,
                'containerType' => null,
                'context' => '123-extractor',
            ],
            'configData' => $configData,
            'tag' => '9.9.9',
            'parallelism' => null,
            'executor' => $expectedResult,
            'type' => 'standard',
            'variableValuesId' => null,
            'variableValuesData' => [],
        ];

        ksort($actualResolvedData);
        ksort($expectedResolvedData);

        self::assertSame($expectedResolvedData, $actualResolvedData);
    }

    public function provideExecutorResolutionTestData(): iterable
    {
        yield 'no executor' => [
            'features' => [],
            'config' => [
                'id' => '454124290',
            ],
            'configData' => [],
            'jobData' => [],
            'result' => 'dind',
        ];

        yield 'no-dind feature' => [
            'features' => ['job-queue-no-dind'],
            'config' => [
                'id' => '454124290',
            ],
            'configData' => [],
            'jobData' => [],
            'result' => 'k8sContainers',
        ];

        yield 'executor in config' => [
            'features' => [],
            'config' => [
                'id' => '454124290',
                'configuration' => [
                    'runtime' => [
                        'executor' => 'k8sContainers',
                    ],
                ],
            ],
            'configData' => [],
            'jobData' => [],
            'result' => 'k8sContainers',
        ];

        yield 'config overrules feature' => [
            'features' => ['job-queue-no-dind'],
            'config' => [
                'id' => '454124290',
                'configuration' => [
                    'runtime' => [
                        'executor' => 'dind',
                    ],
                ],
            ],
            'configData' => [],
            'jobData' => [],
            'result' => 'dind',
        ];

        yield 'executor in config data' => [
            'features' => [],
            'config' => [
                'id' => '454124290',
                'configuration' => [
                    'runtime' => [
                        'executor' => 'dind',
                    ],
                ],
            ],
            'configData' => [
                'runtime' => [
                    'executor' => 'k8sContainers',
                ],
            ],
            'jobData' => [],
            'result' => 'k8sContainers',
        ];

        yield 'config data overrules feature' => [
            'features' => ['job-queue-no-dind'],
            'config' => [
                'id' => '454124290',
            ],
            'configData' => [
                'runtime' => [
                    'executor' => 'dind',
                ],
            ],
            'jobData' => [],
            'result' => 'dind',
        ];

        yield 'executor in job data' => [
            'features' => [],
            'config' => [
                'id' => '454124290',
                'configuration' => [
                    'runtime' => [
                        'executor' => 'dind',
                    ],
                ],
            ],
            'configData' => [
                'runtime' => [
                    'executor' => 'dind',
                ],
            ],
            'jobData' => [
                'executor' => 'k8sContainers',
            ],
            'result' => 'k8sContainers',
        ];

        yield 'job data overrules feature' => [
            'features' => ['job-queue-no-dind'],
            'config' => [
                'id' => '454124290',
            ],
            'configData' => [],
            'jobData' => [
                'executor' => 'dind',
            ],
            'result' => 'dind',
        ];
    }

    public function testResolveInvalidConfigurationFailsWithClientException(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configData'] = [
            'variableValuesData' => '123',
            'parameters' => ['foo' => 'bar'],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')
            ->willReturn($this->getTestExtractorComponentData())
        ;

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        $this->expectException(ClientException::class);
        $this->expectExceptionCode(0);
        $this->expectExceptionMessage('Invalid configuration: Invalid type for path "overrides.variableValuesData".');
        $jobRuntimeResolver->resolveJobData($jobData, $this->createToken());
    }

    public function testResolveRuntimeSettingsConfigurationNotFound(): void
    {
        $jobData = self::JOB_DATA;
        $storageClient = $this->createMock(BranchAwareClient::class);
        $countMatcher = self::exactly(2);
        $storageClient->expects($countMatcher)->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnCallback(function () use ($countMatcher) {
                if ($countMatcher->getInvocationCount() === 1) {
                    return $this->getTestComponentData();
                }

                throw new StorageClientException('Configuration "454124290" not found', 404);
            })
        ;

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        $this->expectExceptionMessage('Cannot resolve job parameters: Configuration "454124290" not found');
        $this->expectExceptionCode(0);
        $this->expectException(ClientException::class);
        $jobRuntimeResolver->resolveJobData($jobData, $this->createToken());
    }

    public function testResolveNoConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        unset($jobData['configId']);

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')
            ->willReturn($this->getTestExtractorComponentData())
        ;

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => null,
                    'context' => '123-extractor',
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'tag' => '9.9.9',
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function testResolveEmptyConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configId'] = '';

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')
            ->willReturn($this->getTestExtractorComponentData())
        ;

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => '',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => null,
                    'context' => '123-extractor',
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'tag' => '9.9.9',
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function testResolveEmptyConfigurationData(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configId'] = '123456';

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/123456'],
            )
            ->willReturn(
                $this->getTestExtractorComponentData(),
                ['configuration' => null],
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => '123456',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => null,
                    'context' => '123-extractor',
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'tag' => '9.9.9',
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function testInternalCacheIsClearedForEveryCall(): void
    {
        $jobData = self::JOB_DATA;
        $configuration = [
            'id' => '454124290',
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'stereotyped',
                    ],
                    'tag' => '4.5.6',
                ],
                'parameters' => ['foo' => 'bar'],
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'bar',
                            'value' => 'Kochba',
                        ],
                    ],
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(4))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )
            ->willReturnOnConsecutiveCalls(
                $this->getTestComponentData(),
                $configuration,
                $this->getTestComponentData(),
                $configuration,
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock(
                storageClient: $storageClient,
                expectedInvocationCount: 2,
            ),
        );
        $jobRuntimeResolver->resolveJobData($jobData, $this->createToken());
        $jobRuntimeResolver->resolveJobData($jobData, $this->createToken());
    }

    public function testResolveInvalidComponent(): void
    {
        $jobData = self::JOB_DATA;
        unset($jobData['configId']);
        $component = [
            'id' => 'keboola.ex-db-snowflake',
            'data' => [
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')->willReturn($component);

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        $this->expectExceptionMessage('The component "keboola.ex-db-snowflake" is not runnable.');
        $this->expectException(ClientException::class);
        $jobRuntimeResolver->resolveJobData($jobData, $this->createToken());
    }

    public function testResolveBranchConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['branchId'] = '1234';
        $configuration = [
            'id' => '454124290',
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'stereotyped',
                    ],
                    'tag' => '4.5.6',
                    'parallelism' => '5',
                ],
                'parameters' => ['foo' => 'bar'],
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'bar',
                            'value' => 'Kochba',
                        ],
                    ],
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnOnConsecutiveCalls(
                $this->getTestComponentData(),
                $configuration,
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock(
                storageClient: $storageClient,
                expectedBranchId: '1234',
                actualBranchId: '1234',
            ),
        );
        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => '454124290',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => 'stereotyped',
                    'context' => null,
                ],
                'branchId' => '1234',
                'tag' => '4.5.6',
                'parallelism' => '5',
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'container',
                'variableValuesId' => null,
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'bar',
                            'value' => 'Kochba',
                        ],
                    ],
                ],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function testConfigurationDisabledException(): void
    {
        $jobData = self::JOB_DATA;
        $configuration = [
            'id' => '454124290',
            'isDisabled' => true,
            'configuration' => [
                'parameters' => ['foo' => 'bar'],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnOnConsecutiveCalls(
                $this->getTestComponentData(),
                $configuration,
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );

        $this->expectException(ConfigurationDisabledException::class);
        $this->expectExceptionMessage('Configuration "454124290" of component "keboola.ex-db-snowflake" is disabled.');
        $jobRuntimeResolver->resolveJobData($jobData, $this->createToken());
    }

    /**
     * @dataProvider backendProvider
     */
    public function testResolveBackend(
        ?string $inputBackendType,
        ?string $inputBackendContext,
        ?string $stagingInput,
        int $expectedApiCallCount,
        ?string $expectedType,
        ?string $expectedContainerType,
        ?string $expectedContext,
        array $tokenFeatures,
        string $componentId = 'keboola.ex-db-snowflake',
    ): void {
        $jobData = $this::JOB_DATA;
        $jobData['componentId'] = $componentId;
        $jobData['configId'] = null;
        $jobData['tag'] = '1.2.3';
        $jobData['variableValuesData'] = [
            'values' => [
                [
                    'name' => 'foo',
                    'value' => 'bar',
                ],
            ],
        ];
        $jobData['backend'] = [
            'type' => $inputBackendType,
            'context' => $inputBackendContext,
        ];
        $jobData['parallelism'] = '5';

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly($expectedApiCallCount))->method('apiGet')
            ->with(sprintf('components/%s', $componentId))
            ->willReturn(
                $this->getTestComponentData($stagingInput),
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );

        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => null,
                'componentId' => $componentId,
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => $expectedType,
                    'containerType' => $expectedContainerType,
                    'context' => $expectedContext,
                ],
                'branchId' => self::DEFAULT_BRANCH_ID,
                'tag' => '1.2.3',
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'foo',
                            'value' => 'bar',
                        ],
                    ],
                ],
                'parallelism' => '5',
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'container',
                'variableValuesId' => null,
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken($tokenFeatures)),
        );
    }

    public function backendProvider(): Generator
    {
        yield 'null backend' => [
            null,
            null,
            'local',
            1,
            null,
            null,
            null,
            [],
        ];
        yield 'custom local' => [
            'custom',
            null,
            'local',
            1,
            null,
            'custom',
            null,
            [],
        ];
        yield 'custom context' => [
            null,
            'custom-context',
            null,
            1,
            null,
            null,
            'custom-context',
            [],
        ];
        yield 'custom local without feature' => [
            'custom',
            null,
            'local',
            1,
            null,
            null, // no container backend is set
            null,
            ['pay-as-you-go'],
        ];
        yield 'custom s3' => [
            'custom',
            null,
            's3',
            1,
            null,
            'custom',
            null,
            [],
        ];
        yield 'custom abs' => [
            'custom',
            null,
            'abs',
            1,
            null,
            'custom',
            null,
            [],
        ];
        yield 'custom none' => [
            'custom',
            null,
            'none',
            1,
            null,
            'custom',
            null,
            [],
        ];
        yield 'custom workspace-snowflake' => [
            'custom',
            null,
            'workspace-snowflake',
            1,
            'custom',
            null,
            null,
            [],
        ];
        yield 'custom workspace-snowflake without feature' => [
            'custom',
            null,
            'workspace-snowflake',
            1,
            'custom',
            null,
            null,
            ['pay-as-you-go'],
        ];
        yield 'custom workspace-redshift' => [
            'custom',
            null,
            'workspace-redshift',
            1,
            null,
            null,
            null,
            [],
        ];
        yield 'custom workspace-synapse' => [
            'custom',
            null,
            'workspace-synapse',
            1,
            null,
            null,
            null,
            [],
        ];
        yield 'custom workspace-abs' => [
            'custom',
            null,
            'workspace-abs',
            1,
            null,
            null,
            null,
            [],
        ];
        yield 'custom workspace-exasol' => [
            'custom',
            null,
            'workspace-exasol',
            1,
            null,
            null,
            null,
            [],
        ];
        yield 'custom workspace-teradata' => [
            'custom',
            null,
            'workspace-teradata',
            1,
            null,
            null,
            null,
            [],
        ];
        yield 'custom workspace-bigquery' => [
            'custom',
            null,
            'workspace-bigquery',
            1,
            null,
            null,
            null,
            [],
        ];
        yield 'custom unknown' => [
            'custom',
            null,
            'unknown',
            1,
            null,
            null,
            null,
            [],
        ];
        yield 'custom invalid' => [
            'custom',
            null,
            null,
            1,
            null,
            null,
            null,
            [],
        ];
        yield 'legacy transformation - large' => [
            'large',
            null,
            'none',
            1,
            'large',
            null,
            null,
            [],
            'keboola.legacy-transformation',
        ];
        yield 'legacy transformation - default' => [
            null,
            null,
            'none',
            1,
            null,
            null,
            null,
            [],
            'keboola.legacy-transformation',
        ];
    }

    private function getTestComponentData(?string $stagingInput = 'local'): array
    {
        return [
            'type' => 'extractor',
            'data' => [
                'staging_storage' => [
                    'input' => $stagingInput,
                    'output' => 'local',
                ],
            ],
        ];
    }

    private function getTestExtractorComponentData(): array
    {
        return [
            'type' => 'extractor',
            'id' => 'keboola.ex-db-snowflake',
            'data' => [
                'definition' => [
                    'tag' => '9.9.9',
                ],
            ],
        ];
    }

    public function mergeBackendsProvider(): Generator
    {
        yield 'default context' => [
            'jobData' => [],
            'configData' => [],
            'configuration' => [],
            'expected' => [
                'type' => null,
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-extractor',
            ],
        ];
        yield 'job data + default context' => [
            'jobData' => [
                'type' => 'small',
            ],
            'configData' => [],
            'configuration' => [],
            'expected' => [
                'type' => 'small',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-extractor',
            ],
        ];
        yield 'config data + default context' => [
            'jobData' => [],
            'configData' => [],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-extractor',
            ],
        ];
        yield 'job data' => [
            'jobData' => [
                'type' => 'small',
                'containerType' => 'smallType',
                'context' => '123-wlm',
            ],
            'configData' => [],
            'configuration' => [],
            'expected' => [
                'type' => 'small',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-wlm',
            ],
        ];
        yield 'configuration' => [
            'jobData' => [],
            'configData' => [],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                        'containerType' => 'largeType',
                        'context' => '123-test',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-test',
            ],
        ];
        yield 'job data + configuration' => [
            'jobData' => [
                'type' => 'small',
                'containerType' => 'smallType',
                'context' => '123-wlm',
            ],
            'configData' => [],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                        'containerType' => 'largeType',
                        'context' => '123-test',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'small',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-wlm',
            ],
        ];
        yield 'job data + configuration - do not merge nulls from configuration' => [
            'jobData' => [
                'type' => 'small',
                'containerType' => 'smallType',
                'context' => '123-wlm',
            ],
            'configData' => [],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => null,
                        'containerType' => null,
                        'context' => null,
                    ],
                ],
            ],
            'expected' => [
                'type' => 'small',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-wlm',
            ],
        ];
        yield 'job data + configuration - do not merge nulls from job data' => [
            'jobData' => [
                'type' => null,
                'containerType' => null,
                'context' => null,
            ],
            'configData' => [],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                        'containerType' => 'largeType',
                        'context' => '123-test',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-test',
            ],
        ];
        yield 'job data + configuration - partial merge' => [
            'jobData' => [
                'context' => '123-wlm',
            ],
            'configData' => [],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-wlm',
            ],
        ];
        yield 'job data + configuration + config data - partial merge' => [
            'jobData' => [
                'context' => '123-wlm',
            ],
            'configData' => [
                'runtime' => [
                    'backend' => [
                        'context' => '321-wlm',
                    ],
                ],
            ],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-wlm',
            ],
        ];
        yield 'job data + configuration + config data - partial merge + partial override' => [
            'jobData' => [
                'context' => '123-wlm',
            ],
            'configData' => [
                'runtime' => [
                    'backend' => [
                        'context' => '321-wlm',
                        'type' => 'small',
                    ],
                ],
            ],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'small',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-wlm',
            ],
        ];
        yield 'job data + configuration + config data - partial merge + full override' => [
            'jobData' => [
                'context' => '123-wlm',
                'type' => 'medium',
            ],
            'configData' => [
                'runtime' => [
                    'backend' => [
                        'context' => '321-wlm',
                        'type' => 'small',
                    ],
                ],
            ],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'medium',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-wlm',
            ],
        ];
        yield 'configuration + config data - partial merge' => [
            'jobData' => [],
            'configData' => [
                'runtime' => [
                    'backend' => [
                        'context' => '321-wlm',
                    ],
                ],
            ],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '321-wlm',
            ],
        ];
        yield 'configuration + config data - override' => [
            'jobData' => [],
            'configData' => [
                'runtime' => [
                    'backend' => [
                        'context' => '321-wlm',
                    ],
                ],
            ],
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                        'context' => '123-wlm',
                    ],
                ],
            ],
            'expected' => [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '321-wlm',
            ],
        ];
    }

    /**
     * @dataProvider mergeBackendsProvider
     */
    public function testMergeJobDataBackendWithBackendFromConfig(
        array $jobDataBackend,
        array $jobConfigData,
        array $configuration,
        array $expectedBackend,
    ): void {
        $jobData = $this::JOB_DATA;
        $jobData['tag'] = '1.2.3';
        $jobData['backend'] = $jobDataBackend;
        $jobData['configData'] = $jobConfigData;

        $componentData = $this->getTestComponentData('workspace-snowflake');

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnOnConsecutiveCalls(
                $componentData,
                [
                    'configuration' => $configuration,
                ],
            );

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );

        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => '454124290',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => $expectedBackend,
                'branchId' => self::DEFAULT_BRANCH_ID,
                'tag' => '1.2.3',
                'configData' => $jobData['configData'],
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    /** @dataProvider branchTypeProvider */
    public function testResolveBranch(
        ?string $jobBranchId,
        bool $isDefault,
        string $expectedBranchId,
        string $expectedBranchType,
    ): void {
        $jobData = self::JOB_DATA;
        $jobData['branchId'] = $jobBranchId;
        $jobData['configId'] = null;
        $jobData['tag'] = '1.2.3';

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')
            ->willReturn($this->getTestComponentData());

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock(
                storageClient: $storageClient,
                expectedBranchId: $jobBranchId,
                actualBranchId: $expectedBranchId,
                actualBranchIsDefault: $isDefault,
            ),
        );
        self::assertSame(
            [
                'id' => '123456456',
                'runId' => '123456456',
                'configId' => null,
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'backend' => [
                    'type' => null,
                    'containerType' => null,
                    'context' => '123-extractor',
                ],
                'branchId' => $expectedBranchId,
                'tag' => '1.2.3',
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => $expectedBranchType,
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $this->createToken()),
        );
    }

    public function branchTypeProvider(): Generator
    {
        yield 'branch null' => [
            'jobBranchId' => null,
            'isDefault' => true,
            'expectedBranchId' => 'default',
            'expectedBranchType' => 'default',
        ];
        yield 'branch default' => [
            'jobBranchId' => 'default',
            'isDefault' => true,
            'expectedBranchId' => 'default',
            'expectedBranchType' => 'default',
        ];
        yield 'branch prod' => [
            'jobBranchId' => '1234',
            'isDefault' => true,
            'expectedBranchId' => 'default',
            'expectedBranchType' => 'default',
        ];
        yield 'branch dev' => [
            'jobBranchId' => '456',
            'isDefault' => false,
            'expectedBranchId' => '456',
            'expectedBranchType' => 'dev',
        ];
    }

    private function prepareStorageClientFactoryMock(
        BranchAwareClient $storageClient,
        int $expectedInvocationCount = 1,
        ?string $expectedBranchId = 'default',
        string $actualBranchId = self::DEFAULT_BRANCH_ID,
        bool $actualBranchIsDefault = true,
    ): StorageClientPlainFactory {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects(self::exactly($expectedInvocationCount))
            ->method('getBranchClient')
            ->willReturn($storageClient)
        ;
        $clientWrapper->method('isDefaultBranch')->willReturn($actualBranchIsDefault);
        $clientWrapper->method('getBranchId')->willReturn($actualBranchId);

        $storageClientFactory = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactory->expects(self::exactly($expectedInvocationCount))
            ->method('createClientWrapper')
            ->with(new ClientOptions(
                token: 'KBC::ProjectSecure::token',
                branchId: $expectedBranchId,
            ))
            ->willReturn($clientWrapper)
        ;

        return $storageClientFactory;
    }

    private function createToken(array $features = []): StorageApiToken
    {
        return new StorageApiToken(
            [
                'owner' => [
                    'features' => $features,
                ],
            ],
            'test-token',
        );
    }

    public function resolveJobTypeDataProvider(): Generator
    {
        yield 'row container with explicit standard type' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'parallelism' => '5', // This would normally make it a ROW_CONTAINER
                'type' => 'standard', // But the explicit type should override
            ],
            'expectedType' => JobType::STANDARD,
        ];

        yield 'standard with explicit row container type' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'parallelism' => '0', // This would normally make it a STANDARD
                'type' => 'container', // But the explicit type should override
            ],
            'expectedType' => JobType::ROW_CONTAINER,
        ];

        yield 'orchestrator with explicit row container type' => [
            'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
            'jobData' => [
                'type' => 'container', // Override the orchestration container type
            ],
            'expectedType' => JobType::ROW_CONTAINER,
        ];

        yield 'phase with explicit orchestration container type' => [
            'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
            'jobData' => [
                'configData' => [
                    'phaseId' => '123', // This would normally make it a PHASE_CONTAINER
                ],
                'type' => 'orchestrationContainer', // But the explicit type should override
            ],
            'expectedType' => JobType::ORCHESTRATION_CONTAINER,
        ];

        yield 'flow with explicit phase container type' => [
            'componentId' => JobFactory::FLOW_COMPONENT,
            'jobData' => [
                'type' => 'phaseContainer', // Override the orchestration container type
            ],
            'expectedType' => JobType::PHASE_CONTAINER,
        ];

        // Test all possible enum values with explicit type only
        yield 'explicit type standard' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'type' => 'standard',
            ],
            'expectedType' => JobType::STANDARD,
        ];

        yield 'explicit type container' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'type' => 'container',
            ],
            'expectedType' => JobType::ROW_CONTAINER,
        ];

        yield 'explicit type orchestration' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'type' => 'orchestrationContainer',
            ],
            'expectedType' => JobType::ORCHESTRATION_CONTAINER,
        ];

        yield 'explicit type phase' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'type' => 'phaseContainer',
            ],
            'expectedType' => JobType::PHASE_CONTAINER,
        ];

        yield 'explicit type retry' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'type' => 'retryContainer',
            ],
            'expectedType' => JobType::RETRY_CONTAINER,
        ];

        // Test cases for parallelism condition
        yield 'parallelism numeric positive' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'parallelism' => '10',
            ],
            'expectedType' => JobType::ROW_CONTAINER,
        ];

        yield 'parallelism infinity' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'parallelism' => 'infinity',
            ],
            'expectedType' => JobType::ROW_CONTAINER,
        ];

        yield 'parallelism zero' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'parallelism' => '0',
            ],
            'expectedType' => JobType::STANDARD,
        ];

        yield 'parallelism null' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [
                'parallelism' => null,
            ],
            'expectedType' => JobType::STANDARD,
        ];

        // Test cases for component-specific conditions
        yield 'flow component without explicit type' => [
            'componentId' => JobFactory::FLOW_COMPONENT,
            'jobData' => [],
            'expectedType' => JobType::ORCHESTRATION_CONTAINER,
        ];

        yield 'orchestrator component without explicit type or phaseId' => [
            'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
            'jobData' => [],
            'expectedType' => JobType::ORCHESTRATION_CONTAINER,
        ];

        yield 'orchestrator component with phaseId' => [
            'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
            'jobData' => [
                'configData' => [
                    'phaseId' => '123',
                ],
            ],
            'expectedType' => JobType::PHASE_CONTAINER,
        ];

        yield 'orchestrator component with empty string phaseId' => [
            'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
            'jobData' => [
                'configData' => [
                    'phaseId' => '',
                ],
            ],
            'expectedType' => JobType::ORCHESTRATION_CONTAINER,
        ];

        yield 'standard component with no special configuration' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'jobData' => [],
            'expectedType' => JobType::STANDARD,
        ];
    }

    /**
     * @dataProvider resolveJobTypeDataProvider
     */
    public function testResolveJobType(string $componentId, array $customJobData, JobType $expectedType): void
    {
        $baseJobData = [
            'id' => '123456456',
            'runId' => '123456456',
            'componentId' => $componentId,
            'mode' => 'run',
            'status' => 'created',
            'desiredStatus' => 'processing',
            'projectId' => '123',
            'tokenId' => '456',
            '#tokenString' => 'KBC::ProjectSecure::token',
            'branchId' => 'default',
            'backend' => null,
        ];

        $jobData = array_merge($baseJobData, $customJobData);

        $componentData = [
            'type' => 'dummy-component-type',
            'id' => $componentId,
            'data' => [
                'definition' => [
                    'tag' => '9.9.9',
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())->method('apiGet')
            ->with(sprintf('components/%s', $componentId))
            ->willReturn($componentData);

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClient')->willReturn($storageClient);
        $storageClientFactoryMock = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

        $resolvedJobData = $jobRuntimeResolver->resolveJobData($jobData, $this->createToken());

        self::assertSame(
            $expectedType->value,
            $resolvedJobData['type'],
            sprintf('Failed asserting job type for component "%s"', $componentId),
        );
    }
}
