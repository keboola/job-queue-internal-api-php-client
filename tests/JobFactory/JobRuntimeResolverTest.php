<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\Client as InternalApiClient;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\StorageClientFactory;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

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
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::never())->method('getClientWrapper');
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
                    'type' => 'custom',
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
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::never())->method('getClientWrapper');
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
                    'type' => 'mass-produced',
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake/configs/454124290')->willReturn($configuration);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('getClientWrapper')
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
                    'type' => 'stereotyped',
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake/configs/454124290')->willReturn($configuration);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('getClientWrapper')
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
                    'type' => 'stereotyped',
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

        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake/configs/454124290')->willReturn($configuration);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('getClientWrapper')
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
                    'type' => 'stereotyped',
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
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('getClientWrapper')
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

        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::never())->method('getClientWrapper');

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
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('getClientWrapper')
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
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('getClientWrapper')
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
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('getClientWrapper')
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
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->with()->willReturn($configuration);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::exactly(2))
            ->method('getClientWrapper')
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
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('getClientWrapper')
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

        $clientMock = self::createMock(BranchAwareClient::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake/configs/454124290')->willReturn($configuration);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBranchClientIfAvailable')->willReturn($clientMock);
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock
            ->expects(self::once())
            ->method('getClientWrapper')
            // this is the important bit - branchId is passed as 2nd argument
            ->with('KBC::ProjectSecure::token', 'dev-branch')
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
                    'type' => 'stereotyped',
                ],
                'tag' => '4.5.6',
                'parallelism' => '5',
            ],
            $jobRuntimeResolver->resolveJobData($jobData)
        );
    }
}
