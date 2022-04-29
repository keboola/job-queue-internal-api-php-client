<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Generator;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\ConfigurationDisabledException;
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
    ];

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
        $jobData['backend'] = ['type' => 'custom'];
        $jobData['parallelism'] = '5';
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
                ['components/keboola.ex-db-snowflake']
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
                'tag' => '1.2.3',
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'foo',
                            'value' => 'bar',
                        ],
                    ],
                ],
                'backend' => [
                    'type' => null,
                    'containerType' => 'custom',
                ],
                'parallelism' => '5',
                'variableValuesId' => null,
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
        );
    }

    public function testResolveRuntimeSettingsInConfigData(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configData'] = [
            'variableValuesId' => '123',
            'runtime' => [
                'tag' => '3.2.1',
                'backend' => ['type' => 'mass-produced'],
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
                ['components/keboola.ex-db-snowflake']
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
                'configData' => [
                    'variableValuesId' => '123',
                    'runtime' => [
                        'tag' => '3.2.1',
                        'backend' => [
                            'type' => 'mass-produced',
                        ],
                        'parallelism' => '5',
                    ],
                    'parameters' => [
                        'foo' => 'bar',
                    ],
                ],
                'variableValuesId' => '123',
                'variableValuesData' => [],
                'backend' => [
                    'type' => null,
                    'containerType' => 'mass-produced',
                ],
                'tag' => '3.2.1',
                'parallelism' => '5',
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
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
                ['components/keboola.ex-db-snowflake/configs/454124290'],
                ['components/keboola.ex-db-snowflake']
            )->willReturnOnConsecutiveCalls(
                $configuration,
                $componentData
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
                'variableValuesId' => null,
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'bar',
                            'value' => 'Kochba',
                        ],
                    ],
                ],
                'backend' => [
                    'type' => null,
                    'containerType' => 'stereotyped',
                ],
                'tag' => '4.5.6',
                'parallelism' => '5',
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
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
                ['components/keboola.ex-db-snowflake/configs/454124290'],
                ['components/keboola.ex-db-snowflake']
            )->willReturnOnConsecutiveCalls(
                $configuration,
                $componentData
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
                'variableValuesId' => null,
                'variableValuesData' => [],
                'backend' => [
                    'type' => null,
                    'containerType' => 'stereotyped',
                ],
                'tag' => '4.5.6',
                'parallelism' => '5',
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
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
                ['components/keboola.ex-db-snowflake/configs/454124290'],
                ['components/keboola.ex-db-snowflake'],
            )->willReturnOnConsecutiveCalls(
                $configuration,
                $componentData
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
                'variableValuesData' => [],
                'backend' => [
                    'type' => null,
                    'containerType' => 'stereotyped',
                ],
                'tag' => '4.5.6',
                'parallelism' => '0',
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
        );
    }

    public function testResolveRuntimeSettingsNowhere(): void
    {
        $jobData = self::JOB_DATA;
        $component = [
            'id' => 'keboola.ex-db-snowflake',
            'data' => [
                'definition' => [
                    'tag' => '9.9.9',
                ],
            ],
        ];
        $configuration = [
            'id' => '454124290',
            'configuration' => [
                'parameters' => ['foo' => 'bar'],
            ],
        ];

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake/configs/454124290'],
                ['components/keboola.ex-db-snowflake']
            )->willReturnOnConsecutiveCalls($configuration, $component);
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
                'variableValuesId' => null,
                'variableValuesData' => [],
                'backend' => [
                    'type' => null,
                    'containerType' => null,
                ],
                'tag' => '9.9.9',
                'parallelism' => null,
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
        );
    }

    public function testResolveInvalidConfigurationFailsWithClientException(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configData'] = [
            'variableValuesData' => '123',
            'parameters' => ['foo' => 'bar'],
        ];

        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock->expects(self::never())->method('createClientWrapper');

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid configuration: Invalid type for path "overrides.variableValuesData".');
        $jobRuntimeResolver->resolveJobData($jobData);
    }

    public function testResolveRuntimeSettingsConfigurationNotFound(): void
    {
        $jobData = self::JOB_DATA;
        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with(
                'components/keboola.ex-db-snowflake/configs/454124290',
            )->willThrowException(new StorageClientException('Configuration "454124290" not found', 404));
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
        self::expectExceptionMessage('Cannot resolve job parameters: Configuration "454124290" not found');
        self::expectException(ClientException::class);
        $jobRuntimeResolver->resolveJobData($jobData);
    }

    public function testResolveNoConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        unset($jobData['configId']);
        $component = [
            'id' => 'keboola.ex-db-snowflake',
            'data' => [
                'definition' => [
                    'tag' => '9.9.9',
                ],
            ],
        ];

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')->willReturn($component);
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
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'variableValuesId' => null,
                'variableValuesData' => [],
                'backend' => [
                    'type' => null,
                    'containerType' => null,
                ],
                'tag' => '9.9.9',
                'parallelism' => null,
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
        );
    }

    public function testResolveEmptyConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configId'] = '';
        $component = [
            'id' => 'keboola.ex-db-snowflake',
            'data' => [
                'definition' => [
                    'tag' => '9.9.9',
                ],
            ],
        ];

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake')->willReturn($component);
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
                'configId' => '',
                'componentId' => 'keboola.ex-db-snowflake',
                'mode' => 'run',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => '123',
                'tokenId' => '456',
                '#tokenString' => 'KBC::ProjectSecure::token',
                'variableValuesId' => null,
                'variableValuesData' => [],
                'backend' => [
                    'type' => null,
                    'containerType' => null,
                ],
                'tag' => '9.9.9',
                'parallelism' => null,
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
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
            ->with()->willReturn($configuration);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(4))
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);
        $jobRuntimeResolver->resolveJobData($jobData);
        $jobRuntimeResolver->resolveJobData($jobData);
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
            ->with('components/keboola.ex-db-snowflake')->willReturn($component);
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
        $jobRuntimeResolver->resolveJobData($jobData);
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
        $component = [
            'data' => [
                'staging_storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ];

        $clientMock = self::createMock(BranchAwareClient::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake/configs/454124290'],
                ['components/keboola.ex-db-snowflake'],
            )->willReturnOnConsecutiveCalls($configuration, $component);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('createClientWrapper')
            // this is the important bit - branchId is passed as 2nd argument
            ->withConsecutive(
                [new ClientOptions(null, 'KBC::ProjectSecure::token', 'dev-branch')],
                [new ClientOptions(null, 'KBC::ProjectSecure::token', null)]
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
                'branchId' => 'dev-branch',
                'variableValuesId' => null,
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'bar',
                            'value' => 'Kochba',
                        ],
                    ],
                ],
                'backend' => [
                    'type' => null,
                    'containerType' => 'stereotyped',
                ],
                'tag' => '4.5.6',
                'parallelism' => '5',
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
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
        $clientMock->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake/configs/454124290')->willReturn($configuration);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);
        $jobRuntimeResolver = new JobRuntimeResolver($storageClientFactoryMock);

        self::expectException(ConfigurationDisabledException::class);
        self::expectExceptionMessage('Configuration "454124290" of component "keboola.ex-db-snowflake" is disabled.');
        $jobRuntimeResolver->resolveJobData($jobData);
    }

    /**
     * @dataProvider backendProvider
     */
    public function testResolveBackend(
        ?string $inputBackendType,
        ?string $stagingInput,
        int $expectedApiCallCount,
        ?string $expectedType,
        ?string $expectedContainerType
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
        $jobData['backend'] = ['type' => $inputBackendType];
        $jobData['parallelism'] = '5';

        $componentData = [
            'data' => [
                'staging_storage' => [
                    'input' => $stagingInput,
                    'output' => 'local',
                ],
            ],
        ];

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly($expectedApiCallCount))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake']
            )->willReturnOnConsecutiveCalls(
                $componentData
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
                'tag' => '1.2.3',
                'variableValuesData' => [
                    'values' => [
                        [
                            'name' => 'foo',
                            'value' => 'bar',
                        ],
                    ],
                ],
                'backend' => [
                    'type' => $expectedType,
                    'containerType' => $expectedContainerType,
                ],
                'parallelism' => '5',
                'variableValuesId' => null,
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
        );
    }

    public function backendProvider(): Generator
    {
        yield 'null backend' => [
            null,
            'local',
            0,
            null,
            null,
        ];
        yield 'custom local' => [
            'custom',
            'local',
            1,
            null,
            'custom',
        ];
        yield 'custom s3' => [
            'custom',
            's3',
            1,
            null,
            'custom',
        ];
        yield 'custom abs' => [
            'custom',
            'abs',
            1,
            null,
            'custom',
        ];
        yield 'custom none' => [
            'custom',
            'none',
            1,
            null,
            'custom',
        ];
        yield 'custom workspace-snowflake' => [
            'custom',
            'workspace-snowflake',
            1,
            'custom',
            null,
        ];
        yield 'custom workspace-redshift' => [
            'custom',
            'workspace-redshift',
            1,
            null,
            null,
        ];
        yield 'custom workspace-synapse' => [
            'custom',
            'workspace-synapse',
            1,
            null,
            null,
        ];
        yield 'custom workspace-abs' => [
            'custom',
            'workspace-abs',
            1,
            null,
            null,
        ];
        yield 'custom workspace-exasol' => [
            'custom',
            'workspace-exasol',
            1,
            null,
            null,
        ];
        yield 'custom unknown' => [
            'custom',
            'unknown',
            1,
            null,
            null,
        ];
        yield 'custom invalid' => [
            'custom',
            null,
            1,
            null,
            null,
        ];
    }
}
