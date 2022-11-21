<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Generator;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\ConfigurationDisabledException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
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
    ];

    public function resolveDefaultBackendContextData(): Generator
    {
        yield 'standard job' => [
            'keboola.ex-db-snowflake',
            [],
            JobInterface::TYPE_STANDARD,
            '123-dummy-component-type',
        ];
        yield 'row container job' => [
            'keboola.ex-db-snowflake',
            [
                'parallelism' => '5',
            ],
            JobInterface::TYPE_ROW_CONTAINER,
            null,
        ];
        yield 'phase container job' => [
            JobFactory::ORCHESTRATOR_COMPONENT,
            [
                'configData' => [
                    'phaseId' => '123',
                ],
            ],
            JobInterface::TYPE_PHASE_CONTAINER,
            null,
        ];
        yield 'orchestration job' => [
            JobFactory::ORCHESTRATOR_COMPONENT,
            [],
            JobInterface::TYPE_ORCHESTRATION_CONTAINER,
            null,
        ];
    }

    /**
     * @dataProvider resolveDefaultBackendContextData
     */
    public function testResolveDefaultBackendContext(
        string $componentId,
        array $customJobData,
        string $expectedJobType,
        ?string $expectedContext
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(1))->method('apiGet')
            ->withConsecutive(
                [sprintf('branch/default/components/%s', $componentId)]
            )->willReturnOnConsecutiveCalls(
                $componentData
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(1))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

        $jobData = $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]]);
        self::assertSame($expectedJobType, $jobData['type']);
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290']
            )->willReturnOnConsecutiveCalls(
                $componentData,
                $configuration
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

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
                'type' => 'container',
                'variableValuesId' => null,
            ],
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]])
        );
    }

    public function testResolveRuntimeSettingsInConfigData(): void
    {
        $jobData = self::JOB_DATA;
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake']
            )->willReturnOnConsecutiveCalls(
                $componentData
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

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
                    'containerType' => 'mass-produced',
                    'context' => 'wml',
                ],
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
                'type' => 'container',
                'variableValuesId' => '123',
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]])
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290'],
            )->willReturnOnConsecutiveCalls(
                $componentData,
                $configuration
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

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
                'tag' => '4.5.6',
                'parallelism' => '5',
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
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]])
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
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ];

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290']
            )->willReturnOnConsecutiveCalls(
                $componentData,
                $configuration
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
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
                'tag' => '4.5.6',
                'parallelism' => '5',
                'type' => 'container',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]])
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290']
            )->willReturnOnConsecutiveCalls(
                $this->getTestComponentData(),
                $configuration
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
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
                'type' => 'standard',
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]])
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290']
            )->willReturnOnConsecutiveCalls(
                $this->getTestExtractorComponentData(),
                $configuration
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

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
                'tag' => '9.9.9',
                'parallelism' => null,
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, [])
        );
    }

    public function testResolveInvalidConfigurationFailsWithClientException(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configData'] = [
            'variableValuesData' => '123',
            'parameters' => ['foo' => 'bar'],
        ];

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(1))->method('apiGet')
            ->with('branch/default/components/keboola.ex-db-snowflake')->willReturn(
                $this->getTestExtractorComponentData()
            )
        ;
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(1))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
        self::expectException(ClientException::class);
        self::expectExceptionCode(0);
        self::expectExceptionMessage('Invalid configuration: Invalid type for path "overrides.variableValuesData".');
        $jobRuntimeResolver->resolveJobData($jobData, []);
    }

    public function testResolveRuntimeSettingsConfigurationNotFound(): void
    {
        $jobData = self::JOB_DATA;
        $clientMock = self::createMock(Client::class);
        $countMatcher = self::exactly(2);
        $clientMock->expects($countMatcher)->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290']
            )->willReturnCallback(function () use ($countMatcher) {
                if ($countMatcher->getInvocationCount() === 1) {
                    return $this->getTestComponentData();
                }

                throw new StorageClientException('Configuration "454124290" not found', 404);
            })
        ;
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
        self::expectExceptionMessage('Cannot resolve job parameters: Configuration "454124290" not found');
        self::expectExceptionCode(0);
        self::expectException(ClientException::class);
        $jobRuntimeResolver->resolveJobData($jobData, []);
    }

    public function testResolveNoConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        unset($jobData['configId']);

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(1))->method('apiGet')
            ->with('branch/default/components/keboola.ex-db-snowflake')->willReturn(
                $this->getTestExtractorComponentData()
            )
        ;
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(1))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
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
                'tag' => '9.9.9',
                'parallelism' => null,
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, [])
        );
    }

    public function testResolveEmptyConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configId'] = '';

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(1))->method('apiGet')
            ->with('branch/default/components/keboola.ex-db-snowflake')->willReturn(
                $this->getTestExtractorComponentData()
            )
        ;
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(1))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
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
                'tag' => '9.9.9',
                'parallelism' => null,
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, [])
        );
    }

    public function testResolveEmptyConfigurationData(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configId'] = '123456';

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/123456'],
            )
            ->willReturn(
                $this->getTestExtractorComponentData(),
                ['configuration' => null]
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
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
                'tag' => '9.9.9',
                'parallelism' => null,
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, [])
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(4))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290'],
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290']
            )
            ->willReturnOnConsecutiveCalls(
                $this->getTestComponentData(),
                $configuration,
                $this->getTestComponentData(),
                $configuration
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(4))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with('branch/default/components/keboola.ex-db-snowflake')->willReturn($component);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
        self::expectExceptionMessage('The component "keboola.ex-db-snowflake" is not runnable.');
        self::expectException(ClientException::class);
        $jobRuntimeResolver->resolveJobData($jobData, []);
    }

    public function testResolveBranchConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['branchId'] = 'dev-branch';
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

        $clientMock = self::createMock(BranchAwareClient::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake'],
                ['components/keboola.ex-db-snowflake/configs/454124290']
            )->willReturnOnConsecutiveCalls($this->getTestComponentData(), $configuration);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            // this is the important bit - branchId is passed as 2nd argument
            ->withConsecutive(
                [new ClientOptions(null, 'KBC::ProjectSecure::token', null)],
                [new ClientOptions(null, 'KBC::ProjectSecure::token', 'dev-branch')]
            )
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
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
                'branchId' => 'dev-branch',
                'tag' => '4.5.6',
                'parallelism' => '5',
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
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]])
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290']
            )->willReturnOnConsecutiveCalls(
                $this->getTestComponentData(),
                $configuration
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

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
        array $tokenInfo
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly($expectedApiCallCount))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake']
            )->willReturnOnConsecutiveCalls(
                $this->getTestComponentData($stagingInput)
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly($expectedApiCallCount))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

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
                'type' => 'container',
                'variableValuesId' => null,
            ],
            $jobRuntimeResolver->resolveJobData($jobData, $tokenInfo)
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
            'custom',
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
            'custom',
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-synapse' => [
            'custom',
            null,
            'workspace-synapse',
            1,
            'custom',
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-abs' => [
            'custom',
            null,
            'workspace-abs',
            1,
            'custom',
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-exasol' => [
            'custom',
            null,
            'workspace-exasol',
            1,
            'custom',
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom workspace-teradata' => [
            'custom',
            null,
            'workspace-teradata',
            1,
            'custom',
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
            'custom',
            null,
            null,
            ['owner' => ['features' => []]],
        ];
        yield 'custom invalid' => [
            'custom',
            null,
            null,
            1,
            'custom',
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

    public function mergeBackendsData(): Generator
    {
        yield 'default context' => [
            [],
            [],
            [
                'type' => null,
                'containerType' => null,
                'context' => '123-extractor',
            ],
        ];
        yield 'job data + default context' => [
            [
                'type' => 'small',
            ],
            [],
            [
                'type' => 'small',
                'containerType' => null,
                'context' => '123-extractor',
            ],
        ];
        yield 'config data + default context' => [
            [],
            [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                    ],
                ],
            ],
            [
                'type' => 'large',
                'containerType' => null,
                'context' => '123-extractor',
            ],
        ];
        yield 'job data' => [
            [
                'type' => 'small',
                'containerType' => 'smallType',
                'context' => '123-wlm',
            ],
            [],
            [
                'type' => 'small',
                'containerType' => 'smallType',
                'context' => '123-wlm',
            ],
        ];
        yield 'config data' => [
            [],
            [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                        'containerType' => 'largeType',
                        'context' => '123-test',
                    ],
                ],
            ],
            [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-test',
            ],
        ];
        yield 'job data + config data' => [
            [
                'type' => 'small',
                'containerType' => 'smallType',
                'context' => '123-wlm',
            ],
            [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                        'containerType' => 'largeType',
                        'context' => '123-test',
                    ],
                ],
            ],
            [
                'type' => 'large',
                'containerType' => 'smallType', // container type can be set only via jobData backend
                'context' => '123-test',
            ],
        ];
        yield 'job data + config data - do not merge nulls from config data' => [
            [
                'type' => 'small',
                'containerType' => 'smallType',
                'context' => '123-wlm',
            ],
            [
                'runtime' => [
                    'backend' => [
                        'type' => null,
                        'containerType' => null,
                        'context' => null,
                    ],
                ],
            ],
            [
                'type' => 'small',
                'containerType' => 'smallType', // container type can be set only via jobData backend
                'context' => '123-wlm',
            ],
        ];
        yield 'job data + config data - do not merge nulls from job data' => [
            [
                'type' => null,
                'containerType' => null,
                'context' => null,
            ],
            [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                        'containerType' => 'largeType',
                        'context' => '123-test',
                    ],
                ],
            ],
            [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-test',
            ],
        ];
        yield 'job data + config data - partial merge' => [
            [
                'context' => '123-wlm',
            ],
            [
                'runtime' => [
                    'backend' => [
                        'type' => 'large',
                    ],
                ],
            ],
            [
                'type' => 'large',
                'containerType' => null, // container type can be set only via jobData backend
                'context' => '123-wlm',
            ],
        ];
    }

    /**
     * @dataProvider mergeBackendsData
     */
    public function testMergeJobDataBackendWithConfigDataBackend(
        array $jobDataBackend,
        array $configData,
        array $expectedBackend
    ): void {
        $jobData = $this::JOB_DATA;
        $jobData['tag'] = '1.2.3';
        $jobData['backend'] = $jobDataBackend;
        unset($jobData['configId']);
        $jobData['configData'] = $configData;

        $componentData = [
            'type' => 'extractor',
        ];

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(1))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
            )->willReturnOnConsecutiveCalls(
                $componentData
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(1))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

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
                'tag' => '1.2.3',
                'backend' => $expectedBackend,
                'configData' => $jobData['configData'],
                'parallelism' => null,
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]])
        );
    }

    /**
     * @dataProvider mergeBackendsData
     */
    public function testMergeJobDataBackendWithBackendFromConfig(
        array $jobDataBackend,
        array $configData,
        array $expectedBackend
    ): void {
        $jobData = $this::JOB_DATA;
        $jobData['tag'] = '1.2.3';
        $jobData['backend'] = $jobDataBackend;

        $componentData = [
            'type' => 'extractor',
        ];

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['branch/default/components/keboola.ex-db-snowflake'],
                ['branch/default/components/keboola.ex-db-snowflake/configs/454124290']
            )->willReturnOnConsecutiveCalls(
                $componentData,
                [
                    'configuration' => $configData,
                ]
            );
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

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
                'tag' => '1.2.3',
                'backend' => $expectedBackend,
                'parallelism' => null,
                'type' => 'standard',
                'variableValuesId' => null,
                'variableValuesData' => [],
            ],
            $jobRuntimeResolver->resolveJobData($jobData, ['owner' => ['features' => []]])
        );
    }
}
