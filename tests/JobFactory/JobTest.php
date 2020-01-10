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
        'params' => [
            'config' => '454124290',
            'component' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
            'configData' => [
                'parameters' => ['foo' => 'bar'],
            ],
        ],
        'status' => 'created',
        'project' => [
            'id' => '123',
        ],
        'token' => [
            'id' => '456',
            'token' => 'KBC::ProjectSecure::token',
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

        $jobDataWithConfigIdInt = $this->jobData;
        $jobDataWithConfigIdInt['params']['config'] = 123456789;
        self::assertSame('123456789', $this->getJob($jobDataWithConfigIdInt)->getConfigId());

        $jobDataWithoutConfigId = $this->jobData;
        unset($jobDataWithoutConfigId['params']['config']);
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

        $jobData = $this->jobData;
        $jobData['runId'] = 1234.567;
        self::assertSame('1234', $this->getJob($jobData)->getParentRunId());
    }

    public function testGetRunId(): void
    {
        self::assertEquals('123456456', $this->getJob()->getRunId());

        $jobData = $this->jobData;
        $jobData['runId'] = '1234.567';
        self::assertEquals('1234.567', $this->getJob($jobData)->getRunId());

        $jobData = $this->jobData;
        $jobData['runId'] = 1234.567;
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

    public function testGetRowId(): void
    {
        self::assertNull($this->getJob()->getRowId());

        $jobDataWithRowIdInt = $this->jobData;
        $jobDataWithRowIdInt['params']['row'] = 123456789;
        self::assertSame('123456789', $this->getJob($jobDataWithRowIdInt)->getRowId());

        $jobDataWithoutRowId = $this->jobData;
        unset($jobDataWithoutRowId['params']['row']);
        self::assertNull($this->getJob($jobDataWithoutRowId)->getRowId());
    }

    public function testGetStatus(): void
    {
        self::assertEquals('created', $this->getJob()->getStatus());
    }

    public function testGetTag(): void
    {
        self::assertNull($this->getJob()->getTag());

        $jobDataWithTagNumeric = $this->jobData;
        $jobDataWithTagNumeric['params']['tag'] = 1.1;
        self::assertSame('1.1', $this->getJob($jobDataWithTagNumeric)->getTag());

        $jobDataWithoutTag = $this->jobData;
        unset($jobDataWithoutTag['params']['tag']);
        self::assertNull($this->getJob($jobDataWithoutTag)->getTag());
    }

    public function testGetToken(): void
    {
        self::assertStringStartsWith('KBC::ProjectSecure::', $this->getJob()->getToken());
    }

    public function testIsFinished(): void
    {
        self::assertFalse($this->getJob()->isFinished());
    }

    public function testIsLegacy(): void
    {
        self::assertFalse($this->getJob()->isLegacyComponent());
    }

    public function testIsLegacyOrchestrator(): void
    {
        $jobData = $this->jobData;
        $jobData['params']['component'] = 'orchestrator';
        self::assertTrue($this->getJob($jobData)->isLegacyComponent());
    }

    public function testLegacyOrchestratorJob(): void
    {
        $jobData = $this->jobData;
        unset($jobData['params']['component']);
        $job = $this->getJob($jobData);
        self::assertEquals('', $job->getComponentId());
        self::assertTrue($job->isLegacyComponent());
    }

    public function testJsonSerialize(): void
    {
        $expected = $this->jobData;
        $expected['runId'] = '123456456';
        self::assertEquals($expected, $this->getJob()->jsonSerialize());
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
