<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\ExistingPlainJobFactory;
use Keboola\JobQueueInternalClient\JobFactory\PlainJob;
use Keboola\JobQueueInternalClient\JobFactory\PlainJobInterface;
use Keboola\PermissionChecker\BranchType;

class ExistingPlainJobFactoryTest extends BaseTest
{
    public function testLoadFromExistingJobData(): void
    {
        $factory = new ExistingPlainJobFactory();
        $data = [
            'id' => '123',
            'runId' => '123',
            'projectId' => '123',
            'branchType' => BranchType::DEFAULT->value,
            'tokenId' => '1234',
            'status' => PlainJobInterface::STATUS_CREATED,
            'desiredStatus' => PlainJobInterface::DESIRED_STATUS_PROCESSING,
            '#tokenString' => 'KBC::ProjectSecure::token',
            'configId' => '456',
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'componentId' => 'keboola.component',
            'mode' => 'run',
        ];

        $job = $factory->loadFromExistingJobData($data);

        self::assertEquals('123', $job->getId());
        self::assertEquals('keboola.component', $job->getComponentId());
        self::assertEquals('456', $job->getConfigId());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
    }

    public function testLoadFromExistingJobDataWithInvalidData(): void
    {
        $factory = new ExistingPlainJobFactory();
        $data = [];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('The child config "id" under "job" must be configured.');

        $factory->loadFromExistingJobData($data);
    }

    public function testLoadFromElasticJobData(): void
    {
        $factory = new ExistingPlainJobFactory();
        $data = [
            'id' => '123',
            'runId' => '123',
            'projectId' => '123',
            'branchType' => BranchType::DEFAULT->value,
            'tokenId' => '1234',
            'status' => PlainJobInterface::STATUS_CREATED,
            'desiredStatus' => PlainJobInterface::DESIRED_STATUS_PROCESSING,
            'configId' => '456',
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'componentId' => 'keboola.component',
            'mode' => 'run',
        ];

        $job = $factory->loadFromElasticJobData($data);

        self::assertEquals('123', $job->getId());
        self::assertEquals('keboola.component', $job->getComponentId());
        self::assertEquals('456', $job->getConfigId());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
    }

    public function testLoadFromElasticJobDataWithInvalidData(): void
    {
        $factory = new ExistingPlainJobFactory();
        $data = [];

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('The child config "id" under "job" must be configured.');

        $factory->loadFromElasticJobData($data);
    }
}
