<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use PHPUnit\Framework\TestCase;

class JobTest extends TestCase
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

    /** @var Job */
    private $job;

    public function setUp(): void
    {
        parent::setUp();
        $this->job = $this->getJob();
    }

    public function testGetComponentId(): void
    {
        $this->assertEquals('keboola.ex-db-snowflake', $this->job->getComponentId());
    }

    public function testGetConfigData(): void
    {
        $this->assertEquals(['parameters' => ['foo' => 'bar']], $this->job->getConfigData());
    }

    public function testGetConfigId(): void
    {
        $this->assertEquals('454124290', $this->job->getConfigId());
    }

    public function testGetId(): void
    {
        $this->assertEquals('123456456', $this->job->getId());
    }

    public function testGetMode(): void
    {
        $this->assertEquals('run', $this->job->getMode());
    }

    public function testGetProjectId(): void
    {
        $this->assertEquals('123', $this->job->getProjectId());
    }

    public function testGetResult(): void
    {
        $this->assertNull($this->job->getResult());
    }

    public function testGetRowId(): void
    {
        $this->assertNull($this->job->getRowId());
    }

    public function testGetStatus(): void
    {
        $this->assertEquals('created', $this->job->getStatus());
    }

    public function testGetTag(): void
    {
        $this->assertNull($this->job->getTag());
    }

    public function testGetToken(): void
    {
        $this->assertStringStartsWith('KBC::ProjectSecure::', $this->job->getToken());
    }

    public function testIsFinished(): void
    {
        $this->assertFalse($this->job->isFinished());
    }

    public function testJsonSerialize(): void
    {
        $this->assertEquals($this->jobData, $this->job->jsonSerialize());
    }

    private function getJob(): Job
    {
        $objectEncryptorFactoryMock = self::getMockBuilder(ObjectEncryptorFactory::class)
            ->disableOriginalConstructor()
            ->getMock();
        /** @var ObjectEncryptorFactory $objectEncryptorFactoryMock */
        return new Job($objectEncryptorFactoryMock, $this->jobData);
    }
}
