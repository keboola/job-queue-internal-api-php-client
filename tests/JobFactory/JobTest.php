<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\Tests\BaseTest;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;

class JobTest extends BaseTest
{
    /** @var array */
    private $jobData = [
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
        '#tokenString' => 'KBC::ProjectSecure::token',
        'branchId' => '987',
        'variableValuesId' => '1357',
        'variableValuesData' => [
            'values' => [
                [
                    'name' => 'foo',
                    'value' => 'bar',
                ],
            ],
        ],
    ];

    public function setUp(): void
    {
        parent::setUp();
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
        $objectEncryptorFactoryMock = self::getMockBuilder(ObjectEncryptorFactory::class)
            ->disableOriginalConstructor()
            ->getMock();
        /** @var ObjectEncryptorFactory $objectEncryptorFactoryMock */
        return new Job($objectEncryptorFactoryMock, $jobData ?? $this->jobData);
    }
}
