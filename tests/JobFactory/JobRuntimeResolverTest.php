<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\StorageClientFactory;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException as StorageClientException;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class JobRuntimeResolverTest extends TestCase
{
    /** @var array */
    private const JOB_DATA = [
        'id' => '123456456',
        'configId' => '454124290',
        'componentId' => 'keboola.ex-db-snowflake',
        'mode' => 'run',
        'status' => 'created',
        'desiredStatus' => 'processing',
        'projectId' => '123',
        'tokenId' => '456',
        '#tokenString' => 'KBC::ProjectSecure::token',
    ];

    private function getObjectEncryptorFactoryMock(): ObjectEncryptorFactory
    {
        $objectEncryptorMock = self::createMock(ObjectEncryptor::class);
        $objectEncryptorMock->expects(self::any())->method('decrypt')->willReturnArgument(0);

        $objectEncryptorFactoryMock = self::createMock(ObjectEncryptorFactory::class);
        $objectEncryptorFactoryMock->expects(self::any())->method('getEncryptor')
            ->willReturn($objectEncryptorMock);
        return $objectEncryptorFactoryMock;
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
        $jobData['backend'] = ['type' => 'custom'];

        $job = new Job($this->getObjectEncryptorFactoryMock(), $jobData);

        $logger = new TestLogger();
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::never())->method('getClient');
        $jobFactoryMock = self::createMock(JobFactory::class);

        $jobFactoryMock->expects(self::once())->method('modifyJob')
            ->with(
                $job,
                [
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
                    'tag' => '1.2.3',
                ]
            )->willReturn($job);

        $jobRuntimeResolver = new JobRuntimeResolver($logger, $storageClientFactoryMock, $jobFactoryMock, $job);
        $jobRuntimeResolver->resolve();
    }

    public function testResolveRuntimeSettingsInConfigData(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configData'] = [
            'variableValuesId' => '123',
            'runtime' => [
                'tag' => '3.2.1',
                'backend' => ['type' => 'mass-produced'],
            ],
            'parameters' => ['foo' => 'bar'],
        ];

        $job = new Job($this->getObjectEncryptorFactoryMock(), $jobData);

        $logger = new TestLogger();
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::never())->method('getClient');
        $jobFactoryMock = self::createMock(JobFactory::class);

        $jobFactoryMock->expects(self::once())->method('modifyJob')
            ->with(
                $job,
                [
                    'variableValuesId' => '123',
                    'backend' => [
                        'type' => 'mass-produced',
                    ],
                    'tag' => '3.2.1',
                ]
            )->willReturn($job);

        $jobRuntimeResolver = new JobRuntimeResolver($logger, $storageClientFactoryMock, $jobFactoryMock, $job);
        $jobRuntimeResolver->resolve();
    }

    public function testResolveRuntimeSettingsInConfiguration(): void
    {
        $jobData = self::JOB_DATA;
        $job = new Job($this->getObjectEncryptorFactoryMock(), $jobData);
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

        $logger = new TestLogger();
        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake/configs/454124290')->willReturn($configuration);
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::once())->method('getClient')->willReturn($clientMock);
        $jobFactoryMock = self::createMock(JobFactory::class);

        $jobFactoryMock->expects(self::once())->method('modifyJob')
            ->with(
                $job,
                [
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
                ]
            )->willReturn($job);

        $jobRuntimeResolver = new JobRuntimeResolver($logger, $storageClientFactoryMock, $jobFactoryMock, $job);
        $jobRuntimeResolver->resolve();
    }

    public function testResolveRuntimeSettingsPriority(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['variableValuesId'] = '123';
        $jobData['configData'] = [
            'variableValuesId' => '456',
            'runtime' => [
                'tag' => '4.5.6',
            ],
            'parameters' => ['foo' => 'bar'],
        ];
        $job = new Job($this->getObjectEncryptorFactoryMock(), $jobData);
        $configuration = [
            'id' => '454124290',
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'stereotyped',
                    ],
                    'tag' => '7.8.9',
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

        $logger = new TestLogger();
        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with('components/keboola.ex-db-snowflake/configs/454124290')->willReturn($configuration);
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::once())->method('getClient')->willReturn($clientMock);
        $jobFactoryMock = self::createMock(JobFactory::class);

        $jobFactoryMock->expects(self::once())->method('modifyJob')
            ->with(
                $job,
                [
                    'variableValuesId' => '123',
                    'backend' => [
                        'type' => 'stereotyped',
                    ],
                    'tag' => '4.5.6',
                ]
            )->willReturn($job);

        $jobRuntimeResolver = new JobRuntimeResolver($logger, $storageClientFactoryMock, $jobFactoryMock, $job);
        $jobRuntimeResolver->resolve();
    }

    public function testResolveRuntimeSettingsNowhere(): void
    {
        $jobData = self::JOB_DATA;
        $job = new Job($this->getObjectEncryptorFactoryMock(), $jobData);
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

        $logger = new TestLogger();
        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::exactly(2))->method('apiGet')
            ->withConsecutive(
                ['components/keboola.ex-db-snowflake/configs/454124290'],
                ['components/keboola.ex-db-snowflake']
            )->willReturnOnConsecutiveCalls($configuration, $component);
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::once())->method('getClient')->willReturn($clientMock);
        $jobFactoryMock = self::createMock(JobFactory::class);

        $jobFactoryMock->expects(self::once())->method('modifyJob')
            ->with(
                $job,
                [
                    'backend' => [],
                    'tag' => '9.9.9',
                ]
            )->willReturn($job);

        $jobRuntimeResolver = new JobRuntimeResolver($logger, $storageClientFactoryMock, $jobFactoryMock, $job);
        $jobRuntimeResolver->resolve();
    }

    public function testResolveInvalidConfigurationFailsWithClientException(): void
    {
        $jobData = self::JOB_DATA;
        $jobData['configData'] = [
            'variableValuesData' => '123',
            'parameters' => ['foo' => 'bar'],
        ];
        $job = new Job($this->getObjectEncryptorFactoryMock(), $jobData);

        $logger = new TestLogger();
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::never())->method('getClient');
        $jobFactoryMock = self::createMock(JobFactory::class);

        $jobRuntimeResolver = new JobRuntimeResolver($logger, $storageClientFactoryMock, $jobFactoryMock, $job);
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid configuration: Invalid type for path "overrides.variableValuesData".');
        $jobRuntimeResolver->resolve();
    }

    public function testResolveRuntimeSettingsConfigurationNotFound(): void
    {
        $jobData = self::JOB_DATA;
        $job = new Job($this->getObjectEncryptorFactoryMock(), $jobData);
        $logger = new TestLogger();
        $clientMock = self::createMock(Client::class);
        $clientMock->expects(self::once())->method('apiGet')
            ->with(
                'components/keboola.ex-db-snowflake/configs/454124290',
            )->willThrowException(new StorageClientException('Configuration "454124290" not found', 404));
        $storageClientFactoryMock = self::createMock(StorageClientFactory::class);
        $storageClientFactoryMock->expects(self::once())->method('getClient')->willReturn($clientMock);
        $jobFactoryMock = self::createMock(JobFactory::class);
        $jobRuntimeResolver = new JobRuntimeResolver($logger, $storageClientFactoryMock, $jobFactoryMock, $job);
        self::expectExceptionMessage('Cannot resolve job parameters: Configuration "454124290" not found');
        self::expectException(ClientException::class);
        $jobRuntimeResolver->resolve();
    }
}
