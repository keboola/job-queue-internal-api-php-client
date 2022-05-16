<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\Tests\BaseTest;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

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
            ],
            'backend' => [
                'size' => 'medium',
            ],
        ],
    ];

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
            $this->getJob()->getVariableValuesData()
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

    public function testIsLegacy(): void
    {
        self::assertFalse($this->getJob()->isLegacyComponent());
    }

    public function testIsLegacyOrchestrator(): void
    {
        $jobData = $this->jobData;
        $jobData['componentId'] = 'orchestrator';
        self::assertTrue($this->getJob($jobData)->isLegacyComponent());
    }

    public function testLegacyOrchestratorJob(): void
    {
        $jobData = $this->jobData;
        unset($jobData['componentId']);
        $job = $this->getJob($jobData);
        self::assertEquals('', $job->getComponentId());
        self::assertTrue($job->isLegacyComponent());
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

    private function getJob(?array $jobData = null): Job
    {
        $objectEncryptorFactoryMock = self::createMock(ObjectEncryptorFactory::class);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);
        return new Job($objectEncryptorFactoryMock, $storageClientFactoryMock, $jobData ?? $this->jobData);
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
        self::assertSame('medium', $job->getMetrics()->getBackendSize());
    }

    public function testGetNoMetrics(): void
    {
        $jobData = $this->jobData;
        unset($jobData['metrics']);
        $job = $this->getJob($jobData);
        self::assertNull($job->getMetrics()->getInputTablesBytesSum());
        self::assertNull($job->getMetrics()->getBackendSize());
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
        $objectEncryptorFactoryMock = self::createMock(ObjectEncryptorFactory::class);
        $objectEncryptorMock = self::createMock(ObjectEncryptor::class);
        $objectEncryptorFactoryMock
            ->expects(self::once())
            ->method('getEncryptor')
            ->willReturn($objectEncryptorMock);

        $objectEncryptorMock
            ->expects(self::once())
            ->method('decrypt')
            ->willReturn('decrypted-token-123');
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);

        $job = new Job($objectEncryptorFactoryMock, $storageClientFactoryMock, $this->jobData);

        // first call - calls the Encryptor API (mock)
        $decryptedToken = $job->getTokenDecrypted();
        self::assertEquals('decrypted-token-123', $decryptedToken);

        // second call - should be cached
        $decryptedToken = $job->getTokenDecrypted();
        self::assertEquals('decrypted-token-123', $decryptedToken);
    }

    public function testCacheDecryptedConfigData(): void
    {
        $objectEncryptorFactoryMock = self::createMock(ObjectEncryptorFactory::class);
        $objectEncryptorMock = self::createMock(ObjectEncryptor::class);
        $objectEncryptorFactoryMock
            ->expects(self::once())
            ->method('getEncryptor')
            ->willReturn($objectEncryptorMock);

        $expectedConfigData = [
            'parameters' => ['#secret-foo' => 'decrypted-bar'],
        ];
        $objectEncryptorMock
            ->expects(self::once())
            ->method('decrypt')
            ->willReturn($expectedConfigData);
        $storageClientFactoryMock = self::createMock(StorageClientPlainFactory::class);

        $job = new Job($objectEncryptorFactoryMock, $storageClientFactoryMock, $this->jobData);

        // first call - calls the Encryptor API (mock)
        $decryptedConfigData = $job->getConfigDataDecrypted();
        self::assertEquals($expectedConfigData, $decryptedConfigData);

        // second call - should be cached
        $decryptedConfigData = $job->getConfigDataDecrypted();
        self::assertEquals($expectedConfigData, $decryptedConfigData);
    }

    public function testComponentSpecification(): void
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
        $clientWrapperMock->expects(self::once())->method('getBranchClient')->willReturn($clientMock);
        $factory = $this->createMock(StorageClientPlainFactory::class);
        $factory->expects(self::once())->method('createClientWrapper')->willReturn($clientWrapperMock);

        $objectEncryptorFactoryMock = self::createMock(ObjectEncryptorFactory::class);
        $job = new Job($objectEncryptorFactoryMock, $factory, $this->jobData);
        self::assertSame('test', $job->getComponentSpecification()->getId());
        self::assertSame(256000000, $job->getComponentSpecification()->getMemoryLimitBytes());
        self::assertSame('256m', $job->getComponentSpecification()->getMemoryLimit());
    }
}
