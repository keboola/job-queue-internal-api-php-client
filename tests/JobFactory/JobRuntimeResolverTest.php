<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Generator;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\ConfigurationDisabledException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\JobType;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use PHPUnit\Framework\TestCase;

class JobRuntimeResolverTest extends TestCase
{
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

        $storageClient = self::createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(1))->method('apiGet')
            ->withConsecutive(
                [sprintf('components/%s', $componentId)],
            )->willReturnOnConsecutiveCalls(
                $componentData,
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClient')->willReturn($storageClient);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(1))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

        $jobData = $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]);
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

        $storageClient = self::createMock(BranchAwareClient::class);
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
                'branchId' => 'default',
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
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]),
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

        $storageClient = self::createMock(BranchAwareClient::class);
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
                'branchId' => 'default',
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
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]),
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

        $storageClient = self::createMock(BranchAwareClient::class);
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
                'branchId' => 'default',
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
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]),
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

        $storageClient = self::createMock(BranchAwareClient::class);
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
                'branchId' => 'default',
                'tag' => '4.5.6',
                'parallelism' => '5',
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'container',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]),
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

        $storageClient = self::createMock(BranchAwareClient::class);
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
                'branchId' => 'default',
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
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]),
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

        $storageClient = self::createMock(BranchAwareClient::class);
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
                'branchId' => 'default',
                'tag' => '9.9.9',
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, []),
        );
    }

    /** @dataProvider provideExecutorResolutionTestData */
    public function testResolveExecutor(
        array $configuration,
        array $configData,
        array $jobData,
        string $expectedResult,
    ): void {
        $jobData = array_merge(self::JOB_DATA, $jobData);
        $jobData['configData'] = $configData;

        $storageClient = self::createMock(BranchAwareClient::class);
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
        $actualResolvedData = $jobRuntimeResolver->resolveJobData($jobData, []);

        $expectedResolvedData = [
            'id' => '123456456',
            'runId' => '123456456',
            'configId' => '454124290',
            'branchId' => 'default',
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
            'config' => [
                'id' => '454124290',
            ],
            'configData' => [],
            'jobData' => [],
            'result' => 'dind',
        ];

        yield 'executor in config' => [
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

        yield 'executor in config data' => [
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

        yield 'executor in job data' => [
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
    }

    public function testResolveInvalidConfigurationFailsWithClientException(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configData'] = [
            'variableValuesData' => '123',
            'parameters' => ['foo' => 'bar'],
        ];

        $storageClient = self::createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(1))->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')
            ->willReturn($this->getTestExtractorComponentData())
        ;

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        self::expectException(ClientException::class);
        self::expectExceptionCode(0);
        self::expectExceptionMessage('Invalid configuration: Invalid type for path "overrides.variableValuesData".');
        $jobRuntimeResolver->resolveJobData($jobData, []);
    }

    public function testResolveRuntimeSettingsConfigurationNotFound(): void
    {
        $jobData = self::JOB_DATA;
        $storageClient = self::createMock(BranchAwareClient::class);
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
        self::expectExceptionMessage('Cannot resolve job parameters: Configuration "454124290" not found');
        self::expectExceptionCode(0);
        self::expectException(ClientException::class);
        $jobRuntimeResolver->resolveJobData($jobData, []);
    }

    public function testResolveNoConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        unset($jobData['configId']);

        $storageClient = self::createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(1))->method('apiGet')
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
                'branchId' => 'default',
                'tag' => '9.9.9',
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, []),
        );
    }

    public function testResolveEmptyConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configId'] = '';

        $storageClient = self::createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(1))->method('apiGet')
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
                'branchId' => 'default',
                'tag' => '9.9.9',
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, []),
        );
    }

    public function testResolveEmptyConfigurationData(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configId'] = '123456';

        $storageClient = self::createMock(BranchAwareClient::class);
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
                'branchId' => 'default',
                'tag' => '9.9.9',
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, []),
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

        $storageClient = self::createMock(BranchAwareClient::class);
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
        $jobRuntimeResolver->resolveJobData($jobData, []);
        $jobRuntimeResolver->resolveJobData($jobData, []);
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

        $storageClient = self::createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')->willReturn($component);

        $jobRuntimeResolver = new JobRuntimeResolver(
            $this->prepareStorageClientFactoryMock($storageClient),
        );
        self::expectExceptionMessage('The component "keboola.ex-db-snowflake" is not runnable.');
        self::expectException(ClientException::class);
        $jobRuntimeResolver->resolveJobData($jobData, []);
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

        $storageClient = self::createMock(BranchAwareClient::class);
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
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]),
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

        $storageClient = self::createMock(BranchAwareClient::class);
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

        self::expectException(ConfigurationDisabledException::class);
        self::expectExceptionMessage('Configuration "454124290" of component "keboola.ex-db-snowflake" is disabled.');
        $jobRuntimeResolver->resolveJobData($jobData, []);
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
        array $tokenInfo,
    ): void {
        $jobData = $this::JOB_DATA;
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

        $storageClient = self::createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly($expectedApiCallCount))->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')
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
                'componentId' => 'keboola.ex-db-snowflake',
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
                'branchId' => 'default',
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
            $jobRuntimeResolver->resolveJobData($jobData, $tokenInfo),
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
            ['owner' => ['features' => ['pay-as-you-go']]],
        ];
        yield 'custom s3' => [
            'custom',
            null,
            's3',
            1,
            null,
            'custom',
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom abs' => [
            'custom',
            null,
            'abs',
            1,
            null,
            'custom',
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom none' => [
            'custom',
            null,
            'none',
            1,
            null,
            'custom',
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-snowflake' => [
            'custom',
            null,
            'workspace-snowflake',
            1,
            'custom',
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-snowflake without feature' => [
            'custom',
            null,
            'workspace-snowflake',
            1,
            'custom',
            null,
            null,
            ['owner' => ['features' => ['pay-as-you-go']]],
        ];
        yield 'custom workspace-redshift' => [
            'custom',
            null,
            'workspace-redshift',
            1,
            null,
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-synapse' => [
            'custom',
            null,
            'workspace-synapse',
            1,
            null,
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-abs' => [
            'custom',
            null,
            'workspace-abs',
            1,
            null,
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-exasol' => [
            'custom',
            null,
            'workspace-exasol',
            1,
            null,
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-teradata' => [
            'custom',
            null,
            'workspace-teradata',
            1,
            null,
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-bigquery' => [
            'custom',
            null,
            'workspace-bigquery',
            1,
            null,
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom unknown' => [
            'custom',
            null,
            'unknown',
            1,
            null,
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom invalid' => [
            'custom',
            null,
            null,
            1,
            null,
            null,
            null,
            ['owner' => ['features' => []]],
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

        $storageClient = self::createMock(BranchAwareClient::class);
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
                'branchId' => 'default',
                'tag' => '1.2.3',
                'configData' => $jobData['configData'],
                'parallelism' => null,
                'executor' => 'dind',
                'branchType' => 'default',
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]),
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

        $storageClient = self::createMock(BranchAwareClient::class);
        $storageClient->expects(self::exactly(1))->method('apiGet')
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
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]),
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
        string $actualBranchId = 'default',
        bool $actualBranchIsDefault = true,
    ): StorageClientPlainFactory {
        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->expects(self::exactly($expectedInvocationCount))
            ->method('getBranchClient')
            ->willReturn($storageClient)
        ;
        $clientWrapper->method('isDefaultBranch')->willReturn($actualBranchIsDefault);
        $clientWrapper->method('getBranchId')->willReturn($actualBranchId);

        $storageClientFactory = self::createMock(StorageClientPlainFactory::class);
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
}
