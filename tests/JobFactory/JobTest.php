<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Executor;
use Keboola\JobQueueInternalClient\Tests\BaseTest;
use Keboola\PermissionChecker\BranchType;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use RuntimeException;
use Symfony\Component\Uid\Uuid;

class JobTest extends BaseTest
{
    private array $jobData = [
        'id' => '123456456',
        'runId' => '123456456',
        'configId' => '454124290',
        'componentId' => 'keboola.ex-db-snowflake',
        'mode' => 'run',
        'configData' => [
            'parameters' => ['foo' => 'bar'],
        ],
        'status' => 'created',
        'desiredStatus' => 'processing',
        'projectId' => '123',
        'tokenId' => '456',
        'tokenDescription' => 'My token',
        '#tokenString' => 'KBC::ProjectSecure::token',
        'branchId' => '987',
        'variableValuesId' => '1357',
        'durationSeconds' => '12',
        'variableValuesData' => [
            'values' => [
                [
                    'name' => 'foo',
                    'value' => 'bar',
                ],
            ],
        ],
        'metrics' => [
            'storage' => [
                'inputTablesBytesSum' => 567,
                'outputTablesBytesSum' => 987,
            ],
            'backend' => [
                'size' => 'medium',
                'context' => 'wml',
            ],
        ],
        'orchestrationJobId' => '123456789',
        'branchType' => 'default',
    ];

    public function testConstants(): void
    {
        self::assertCount(9, JobInterface::STATUSES_ALL);
        self::assertCount(5, JobInterface::STATUSES_FINISHED);
        self::assertCount(3, JobInterface::STATUSES_KILLABLE);
    }

    public function testGetComponentId(): void
    {
        self::assertEquals('keboola.ex-db-snowflake', $this->getJob()->getComponentId());
    }

    public function testGetConfigData(): void
    {
        self::assertEquals(['parameters' => ['foo' => 'bar']], $this->getJob()->getConfigData());
    }

    public function testGetConfigId(): void
    {
        self::assertEquals('454124290', $this->getJob()->getConfigId());

        $jobDataWithoutConfigId = $this->jobData;
        unset($jobDataWithoutConfigId['configId']);
        self::assertNull($this->getJob($jobDataWithoutConfigId)->getConfigId());
    }

    public function testGetId(): void
    {
        self::assertEquals('123456456', $this->getJob()->getId());
    }

    public function testGetParentRunId(): void
    {
        self::assertEquals('', $this->getJob()->getParentRunId());

        $jobData = $this->jobData;
        $jobData['runId'] = '1234.567';
        self::assertSame('1234', $this->getJob($jobData)->getParentRunId());
    }

    public function testGetRunId(): void
    {
        self::assertEquals('123456456', $this->getJob()->getRunId());

        $jobData = $this->jobData;
        $jobData['runId'] = '1234.567';
        self::assertEquals('1234.567', $this->getJob($jobData)->getRunId());
    }

    public function testGetMode(): void
    {
        self::assertEquals('run', $this->getJob()->getMode());
    }

    public function testGetProjectId(): void
    {
        self::assertEquals('123', $this->getJob()->getProjectId());
    }

    public function testGetResult(): void
    {
        self::assertEquals([], $this->getJob()->getResult());
    }

    public function testGetConfigRowIds(): void
    {
        self::assertIsArray($this->getJob()->getConfigRowIds());
        self::assertEmpty($this->getJob()->getConfigRowIds());

        $jobDataWithRowId = $this->jobData;
        $jobDataWithRowId['configRowIds'] = ['123456789'];
        self::assertSame(['123456789'], $this->getJob($jobDataWithRowId)->getConfigRowIds());
    }

    public function testGetStatus(): void
    {
        self::assertEquals('created', $this->getJob()->getStatus());
    }

    public function testGetTag(): void
    {
        self::assertNull($this->getJob()->getTag());

        $jobDataWithTag = $this->jobData;
        $jobDataWithTag['tag'] = '1.1';
        self::assertSame('1.1', $this->getJob($jobDataWithTag)->getTag());
    }

    public function testGetToken(): void
    {
        self::assertStringStartsWith('KBC::ProjectSecure::', $this->getJob()->getTokenString());
        self::assertEquals('456', $this->getJob()->getTokenId());
        self::assertEquals('My token', $this->getJob()->getTokenDescription());
    }

    public function testIsFinished(): void
    {
        self::assertFalse($this->getJob()->isFinished());
    }

    public function testGetBranch(): void
    {
        self::assertEquals('987', $this->getJob()->getBranchId());
    }

    public function testGetVariableValuesId(): void
    {
        self::assertEquals('1357', $this->getJob()->getVariableValuesId());
        $jobDataWithoutVariableValuesId = $this->jobData;
        unset($jobDataWithoutVariableValuesId['variableValuesId']);
        self::assertNull($this->getJob($jobDataWithoutVariableValuesId)->getVariableValuesId());
    }

    public function testGetVariableValuesData(): void
    {
        self::assertEquals(
            [
                'values' => [
                    [
                        'name' => 'foo',
                        'value' => 'bar',
                    ],
                ],
            ],
            $this->getJob()->getVariableValuesData(),
        );
    }

    public function testHasVariables(): void
    {
        $job = $this->getJob();
        self::assertSame(true, $job->hasVariables());

        $jobDataWithoutVariableValuesId = $this->jobData;
        unset($jobDataWithoutVariableValuesId['variableValuesId']);
        $job = $this->getJob($jobDataWithoutVariableValuesId);
        self::assertSame(true, $job->hasVariables());

        $jobDataWithoutVariableValuesData = $this->jobData;
        unset($jobDataWithoutVariableValuesData['variableValuesData']);
        $job = $this->getJob($jobDataWithoutVariableValuesData);
        self::assertSame(true, $job->hasVariables());

        $jobDataWithoutVariables = $this->jobData;
        unset($jobDataWithoutVariables['variableValuesData']);
        unset($jobDataWithoutVariables['variableValuesId']);
        $job = $this->getJob($jobDataWithoutVariables);
        self::assertSame(false, $job->hasVariables());
    }

    public function testJsonSerialize(): void
    {
        $expected = $this->jobData;
        $expected['runId'] = '123456456';
        $expected['parentRunId'] = '';
        $expected['isFinished'] = false;
        self::assertEquals($expected, $this->getJob()->jsonSerialize());
    }

    public function testGetNoneBackend(): void
    {
        $backend = $this->getJob()->getBackend();
        self::assertNull($backend->getType());
    }

    public function testGetCustomBackend(): void
    {
        $jobData = $this->jobData;
        $jobData['backend']['type'] = 'custom';

        $backend = $this->getJob($jobData)->getBackend();
        self::assertSame('custom', $backend->getType());
    }

    public function testGetDefaultExecutor(): void
    {
        $executor = $this->getJob()->getExecutor();
        self::assertSame(Executor::DIND, $executor);
    }

    public function testGetCustomExecutor(): void
    {
        $jobData = $this->jobData;
        $jobData['executor'] = Executor::K8S_CONTAINERS->value;

        $executor = $this->getJob($jobData)->getExecutor();
        self::assertSame(Executor::K8S_CONTAINERS, $executor);
    }

    private function getJob(?array $jobData = null): Job
    {
        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $storageClientFactoryMock = $this->createMock(StorageClientPlainFactory::class);
        return new Job($objectEncryptorMock, $storageClientFactoryMock, $jobData ?? $this->jobData);
    }

    public function testGetDuration(): void
    {
        $jobDataWithDuration = $this->jobData;
        $job = $this->getJob($jobDataWithDuration);
        self::assertSame(12, $job->getDurationSeconds());
        $jobDataWithoutDuration = $this->jobData;
        unset($jobDataWithoutDuration['durationSeconds']);
        $job = $this->getJob($jobDataWithoutDuration);
        self::assertNull($job->getDurationSeconds());
    }

    public function testGetMetrics(): void
    {
        $job = $this->getJob($this->jobData);
        self::assertSame(567, $job->getMetrics()->getInputTablesBytesSum());
        self::assertSame(987, $job->getMetrics()->getOutputTablesBytesSum());
        self::assertSame('medium', $job->getMetrics()->getBackendSize());
        self::assertSame('wml', $job->getMetrics()->getBackendContext());
    }

    public function testGetNoMetrics(): void
    {
        $jobData = $this->jobData;
        unset($jobData['metrics']);
        $job = $this->getJob($jobData);
        self::assertNull($job->getMetrics()->getInputTablesBytesSum());
        self::assertNull($job->getMetrics()->getOutputTablesBytesSum());
        self::assertNull($job->getMetrics()->getBackendSize());
        self::assertNull($job->getMetrics()->getBackendContext());
    }

    public function testGetNoBehavior(): void
    {
        $jobData = $this->jobData;

        $behavior = $this->getJob($jobData)->getBehavior();
        self::assertSame(['onError' => null], $behavior->toDataArray());
    }

    public function testGetEmptyBehavior(): void
    {
        $jobData = $this->jobData;
        $jobData['behavior'] = [];
        $behavior = $this->getJob($jobData)->getBehavior();
        self::assertSame(['onError' => null], $behavior->toDataArray());
    }

    public function testGetNonEmptyBehavior(): void
    {
        $jobData = $this->jobData;
        $jobData['behavior'] = ['onError' => 'warning'];
        $behavior = $this->getJob($jobData)->getBehavior();
        self::assertSame(['onError' => 'warning'], $behavior->toDataArray());
    }

    public function testParallelismInfinity(): void
    {
        $jobData = $this->jobData;
        $jobData['parallelism'] = 'infinity';
        $job = $this->getJob($jobData);
        self::assertSame('infinity', $job->getParallelism());
    }

    public function testParallelismNumeric(): void
    {
        $jobData = $this->jobData;
        $jobData['parallelism'] = '3';
        $job = $this->getJob($jobData);
        self::assertSame('3', $job->getParallelism());
    }

    public function testIsInRunMode(): void
    {
        $jobData = $this->jobData;
        $jobData['mode'] = 'run';
        $job = $this->getJob($jobData);
        self::assertTrue($job->isInRunMode());

        $jobData = $this->jobData;
        $jobData['mode'] = 'forceRun';
        $job = $this->getJob($jobData);
        self::assertTrue($job->isInRunMode());

        $jobData = $this->jobData;
        $jobData['mode'] = 'debug';
        $job = $this->getJob($jobData);
        self::assertFalse($job->isInRunMode());
    }

    public function testCacheDecryptedToken(): void
    {
        $tokenEncrypted = $this->jobData['#tokenString'];
        $tokenDecrypted = 'decrypted-token-123';

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                $tokenEncrypted,
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn($tokenDecrypted)
        ;

        $storageClientFactoryMock = $this->createMock(StorageClientPlainFactory::class);

        $job = new Job($objectEncryptorMock, $storageClientFactoryMock, $this->jobData);

        // first call - calls the Encryptor API (mock)
        self::assertSame($tokenDecrypted, $job->getTokenDecrypted());

        // second call - should be cached
        self::assertSame($tokenDecrypted, $job->getTokenDecrypted());
    }

    public function testCacheDecryptedComponentConfig(): void
    {
        $tokenEncrypted = $this->jobData['#tokenString'];
        $tokenDecrypted = 'decrypted-token-123';

        $componentDataEncrypted = [
            'id' => 'encrypted-test',
            'data' => [
                'definition' => [
                    'uri' => 'some-uri',
                    'type' => 'aws-ecr',
                ],
            ],
        ];
        $componentDataDecrypted = [
            'id' => 'decrypted-test',
            'data' => [
                'definition' => [
                    'uri' => 'some-uri',
                    'type' => 'aws-ecr',
                ],
            ],
        ];

        // expect 2 calls - one for token, one for component config
        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::exactly(2))
            ->method('decrypt')
            ->with(
                self::logicalOr(
                    $this::equalTo($tokenEncrypted),
                    $this::equalTo($componentDataEncrypted),
                ),
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturnCallback(function (mixed $encrypted) use (
                $tokenEncrypted,
                $tokenDecrypted,
                $componentDataEncrypted,
                $componentDataDecrypted,
            ) {
                return match ($encrypted) {
                    $tokenEncrypted => $tokenDecrypted,
                    $componentDataEncrypted => $componentDataDecrypted,
                    default => throw new RuntimeException('Unexpected encrypted value'),
                };
            })
        ;

        $storageApiClientMock = $this->createMock(BranchAwareClient::class);
        $storageApiClientMock
            ->method('apiGet')
            ->willReturn($componentDataEncrypted)
        ;

        $storageApiClientWrapperMock = $this->createMock(ClientWrapper::class);
        $storageApiClientWrapperMock->expects(self::once())
            ->method('getBranchClient')
            ->willReturn($storageApiClientMock)
        ;

        $storageClientFactoryMock = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactoryMock->expects(self::once())
            ->method('createClientWrapper')
            ->with(new ClientOptions(null, $tokenDecrypted, '987'))
            ->willReturn($storageApiClientWrapperMock)
        ;

        $job = new Job($objectEncryptorMock, $storageClientFactoryMock, $this->jobData);

        // first call - calls the Encryptor API (mock)
        self::assertSame($componentDataDecrypted, $job->getComponentConfigurationDecrypted());

        // second call - should be cached
        self::assertSame($componentDataDecrypted, $job->getComponentConfigurationDecrypted());
    }

    public function testCacheDecryptedConfigData(): void
    {
        $configDataEncrypted = [
            'parameters' => ['#secret-foo' => 'encrypted-bar'],
        ];

        $configDataDecrypted = [
            'parameters' => ['#secret-foo' => 'decrypted-bar'],
        ];

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                $configDataEncrypted,
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn($configDataDecrypted)
        ;

        $storageClientFactoryMock = $this->createMock(StorageClientPlainFactory::class);

        $jobData = $this->jobData;
        $jobData['configData'] = $configDataEncrypted;

        $job = new Job($objectEncryptorMock, $storageClientFactoryMock, $jobData);

        // first call - calls the Encryptor API (mock)
        self::assertSame($configDataDecrypted, $job->getConfigDataDecrypted());

        // second call - should be cached
        self::assertSame($configDataDecrypted, $job->getConfigDataDecrypted());
    }

    public function testGetBranchType(): void
    {
        $jobData = $this->jobData;
        $jobData['branchType'] = 'default';
        $job = $this->getJob($jobData);
        self::assertSame(BranchType::DEFAULT, $job->getBranchType());

        $jobData = $this->jobData;
        $jobData['branchType'] = 'dev';
        $job = $this->getJob($jobData);
        self::assertSame(BranchType::DEV, $job->getBranchType());
    }

    public function testGetComponentSpecification(): void
    {
        $componentData = [
            'id' => 'test',
            'data' => [
                'definition' => [
                    'uri' => 'some-uri',
                    'type' => 'aws-ecr',
                ],
            ],
        ];
        $clientMock = $this->createMock(BranchAwareClient::class);
        $clientMock->method('apiGet')->willReturn($componentData);
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock
            ->expects(self::once())->method('getBranchClient')->willReturn($clientMock);
        $factory = $this->createMock(StorageClientPlainFactory::class);
        $factory->expects(self::once())->method('createClientWrapper')
            ->with(new ClientOptions(null, 'token', '987'))
            ->willReturn($clientWrapperMock);

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                'KBC::ProjectSecure::token',
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn('token')
        ;

        $job = new Job($objectEncryptorMock, $factory, $this->jobData);
        self::assertSame('test', $job->getComponentSpecification()->getId());
        self::assertSame(256000000, $job->getComponentSpecification()->getMemoryLimitBytes());
        self::assertSame('256m', $job->getComponentSpecification()->getMemoryLimit());
    }

    public function testGetComponentSpecificationWhenComponentDoesNotExist(): void
    {
        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())
            ->method('apiGet')
            ->with(sprintf('components/%s', $this->jobData['componentId']))
            ->willThrowException(new StorageApiClientException('Component not found', 404))
        ;

        $storageClientWrapperMock = $this->createMock(ClientWrapper::class);
        $storageClientWrapperMock->expects(self::once())
            ->method('getBranchClient')
            ->willReturn($storageClient)
        ;

        $storageClientFactory = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactory->expects(self::once())
            ->method('createClientWrapper')
            ->with(new ClientOptions(null, 'token', $this->jobData['branchId']))
            ->willReturn($storageClientWrapperMock)
        ;

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                'KBC::ProjectSecure::token',
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn('token')
        ;

        $job = new Job(
            $objectEncryptorMock,
            $storageClientFactory,
            $this->jobData,
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Failed to fetch component specification: Component not found');

        $job->getComponentSpecification();
    }

    public function testGetComponentConfiguration(): void
    {
        $componentConfigData = [
            'uri' => 'some-uri',
            'type' => 'aws-ecr',
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())
            ->method('apiGet')
            ->with(sprintf('components/%s/configs/%s', $this->jobData['componentId'], $this->jobData['configId']))
            ->willReturn($componentConfigData)
        ;

        $storageClientWrapperMock = $this->createMock(ClientWrapper::class);
        $storageClientWrapperMock->expects(self::once())
            ->method('getBranchClient')
            ->willReturn($storageClient)
        ;

        $storageClientFactory = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactory->expects(self::once())
            ->method('createClientWrapper')
            ->with(new ClientOptions(null, 'token', $this->jobData['branchId']))
            ->willReturn($storageClientWrapperMock)
        ;

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                'KBC::ProjectSecure::token',
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn('token')
        ;

        $job = new Job(
            $objectEncryptorMock,
            $storageClientFactory,
            $this->jobData,
        );

        $result = $job->getComponentConfiguration();

        self::assertSame($componentConfigData, $result);

        // fetch configuration again to test it's not loaded form API again
        $job->getComponentConfiguration();
    }

    public function testGetComponentConfigurationWhenJobHasNoConfigIdSet(): void
    {
        $storageClientFactory = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactory->expects(self::never())->method(self::anything());

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::never())->method('decrypt');

        $jobData = $this->jobData;
        $jobData['configId'] = null;

        $job = new Job(
            $objectEncryptorMock,
            $storageClientFactory,
            $jobData,
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Can\'t fetch component configuration: job has no configId set');

        $job->getComponentConfiguration();
    }

    public function testGetComponentConfigurationWhenConfigDoesNotExist(): void
    {
        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())
            ->method('apiGet')
            ->with(sprintf('components/%s/configs/%s', $this->jobData['componentId'], $this->jobData['configId']))
            ->willThrowException(new StorageApiClientException('Config not found', 404))
        ;

        $storageClientWrapperMock = $this->createMock(ClientWrapper::class);
        $storageClientWrapperMock->expects(self::once())
            ->method('getBranchClient')
            ->willReturn($storageClient)
        ;

        $storageClientFactory = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactory->expects(self::once())
            ->method('createClientWrapper')
            ->with(new ClientOptions(null, 'token', $this->jobData['branchId']))
            ->willReturn($storageClientWrapperMock)
        ;

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                'KBC::ProjectSecure::token',
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn('token')
        ;

        $job = new Job(
            $objectEncryptorMock,
            $storageClientFactory,
            $this->jobData,
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Failed to fetch component configuration: Config not found');

        $job->getComponentConfiguration();
    }

    public function testGetOrchestrationId(): void
    {
        self::assertEquals('123456789', $this->getJob()->getOrchestrationJobId());
    }

    public function testGetRunnerId(): void
    {
        $runnerId = (string) Uuid::v4();
        $jobData = $this->jobData;
        $jobData['runnerId'] = $runnerId;
        $job = $this->getJob($jobData);
        self::assertEquals($runnerId, $job->getRunnerId());

        // not set
        $job = $this->getJob($this->jobData);
        self::assertNull($job->getRunnerId());
    }

    public function testGetProjectFeatures(): void
    {
        $tokenData = [
            'owner' => [
                'id' => 123,
                'name' => 'dummy',
                'features' => [
                    'feature1',
                    'feature2',
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())
            ->method('verifyToken')
            ->willReturn($tokenData)
        ;

        $storageClientWrapperMock = $this->createMock(ClientWrapper::class);
        $storageClientWrapperMock->expects(self::once())
            ->method('getBranchClient')
            ->willReturn($storageClient)
        ;

        $storageClientFactory = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactory->expects(self::once())
            ->method('createClientWrapper')
            ->with(new ClientOptions(null, 'token', $this->jobData['branchId']))
            ->willReturn($storageClientWrapperMock)
        ;

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                'KBC::ProjectSecure::token',
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn('token')
        ;

        $job = new Job(
            $objectEncryptorMock,
            $storageClientFactory,
            $this->jobData,
        );

        $result = $job->getProjectFeatures();

        self::assertSame(['feature1', 'feature2'], $result);

        // fetch features again to test it's not loaded form API again
        $job->getProjectFeatures();
    }

    public function testGetExecutionTokenDecryptedWithFeatureBranchDefault(): void
    {
        $applicationToken = '4pPl1cAti0nT0k3n';
        $privilegedTokenResponse = [
            'id' => '123456',
            'created' => '2023-06-29T09:49:51+0200',
            'refreshed' => '2023-06-29T09:49:51+0200',
            'expires' => '2023-13-29T09:49:51+0200',
            'canManageProtectedDefaultBranch' => true,
            'canCreateJobs' => false,
            'token' => 'th1s-i5-pr1vIl3ged-70k3n',
        ];

        $tokenData = [
            'owner' => [
                'id' => 123,
                'name' => 'dummy',
                'features' => [
                    JobFactory::PROTECTED_DEFAULT_BRANCH_FEATURE,
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())
            ->method('verifyToken')
            ->willReturn($tokenData)
        ;
        $storageClient->expects(self::once())
            ->method('apiPostJson')
            ->with(
                'tokens',
                [
                    'canManageProtectedDefaultBranch' => true,
                    'expiresIn' => 604800,
                    'description' => 'Execution Token for job 123456456',
                    'canReadAllFileUploads' => true,
                    'canManageBuckets' => true,
                ],
                $applicationToken,
            )
            ->willReturn($privilegedTokenResponse)
        ;

        $storageClientWrapperMock = $this->createMock(ClientWrapper::class);
        $storageClientWrapperMock->expects(self::atLeastOnce())
            ->method('getBranchClient')
            ->willReturn($storageClient)
        ;
        $storageClientWrapperMock->expects(self::atLeastOnce())
            ->method('getBasicClient')
            ->willReturn($storageClient)
        ;

        $storageClientFactory = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactory->expects(self::atLeastOnce())
            ->method('createClientWrapper')
            ->with(new ClientOptions(null, 'token', $this->jobData['branchId']))
            ->willReturn($storageClientWrapperMock)
        ;

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                'KBC::ProjectSecure::token',
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn('token')
        ;

        $job = new Job(
            $objectEncryptorMock,
            $storageClientFactory,
            array_merge($this->jobData, ['branchType' => BranchType::DEFAULT->value]),
        );

        $executionToken = $job->getExecutionTokenDecrypted($applicationToken);
        self::assertSame('th1s-i5-pr1vIl3ged-70k3n', $executionToken);

        // test the token is cached (Storage Client method apiPostJson is only called once)
        $executionToken = $job->getExecutionTokenDecrypted($applicationToken);
        self::assertSame('th1s-i5-pr1vIl3ged-70k3n', $executionToken);
    }

    public function testGetExecutionTokenDecryptedWithFeatureBranchDev(): void
    {
        $applicationToken = '4pPl1cAti0nT0k3n';

        $tokenData = [
            'owner' => [
                'id' => 123,
                'name' => 'dummy',
                'features' => [
                    JobFactory::PROTECTED_DEFAULT_BRANCH_FEATURE,
                ],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())
            ->method('verifyToken')
            ->willReturn($tokenData)
        ;
        // the privileged token is not created
        $storageClient->expects(self::never())
            ->method('apiPostJson')
        ;

        $storageClientWrapperMock = $this->createMock(ClientWrapper::class);
        $storageClientWrapperMock->expects(self::atLeastOnce())
            ->method('getBranchClient')
            ->willReturn($storageClient)
        ;

        $storageClientFactory = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactory->expects(self::atLeastOnce())
            ->method('createClientWrapper')
            ->with(new ClientOptions(null, 'token', $this->jobData['branchId']))
            ->willReturn($storageClientWrapperMock)
        ;

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                'KBC::ProjectSecure::token',
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn('token')
        ;

        $job = new Job(
            $objectEncryptorMock,
            $storageClientFactory,
            array_merge($this->jobData, ['branchType' => BranchType::DEV->value]),
        );

        $executionToken = $job->getExecutionTokenDecrypted($applicationToken);

        // returned execution token is "normal" Storage token
        self::assertSame('token', $executionToken);
    }

    public function testGetExecutionTokenDecryptedNoFeature(): void
    {
        $applicationToken = '4pPl1cAti0nT0k3n';

        $tokenData = [
            'owner' => [
                'id' => 123,
                'name' => 'dummy',
                'features' => [],
            ],
        ];

        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->expects(self::once())
            ->method('verifyToken')
            ->willReturn($tokenData)
        ;
        $storageClient->expects(self::never())
            ->method('apiPostJson')
        ;

        $storageClientWrapperMock = $this->createMock(ClientWrapper::class);
        $storageClientWrapperMock->expects(self::atLeastOnce())
            ->method('getBranchClient')
            ->willReturn($storageClient)
        ;

        $storageClientFactory = $this->createMock(StorageClientPlainFactory::class);
        $storageClientFactory->expects(self::atLeastOnce())
            ->method('createClientWrapper')
            ->with(new ClientOptions(null, 'token', $this->jobData['branchId']))
            ->willReturn($storageClientWrapperMock)
        ;

        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $objectEncryptorMock->expects(self::once())
            ->method('decrypt')
            ->with(
                'KBC::ProjectSecure::token',
                $this->jobData['componentId'],
                $this->jobData['projectId'],
                $this->jobData['configId'],
            )
            ->willReturn('token')
        ;

        $job = new Job(
            $objectEncryptorMock,
            $storageClientFactory,
            array_merge($this->jobData, ['branchType' => BranchType::DEFAULT->value]),
        );

        $executionToken = $job->getExecutionTokenDecrypted($applicationToken);

        self::assertSame('token', $executionToken);
    }
}
