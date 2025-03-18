<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobPatchData;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\JobQueueInternalClient\Result\JobResult;
use PHPUnit\Framework\TestCase;

class JobPatchDataTest extends TestCase
{
    public function testAccessors(): void
    {
        $jobPatchData = new JobPatchData();
        $jobPatchData
            ->setStatus(JobInterface::STATUS_PROCESSING)
            ->setDesiredStatus(JobInterface::DESIRED_STATUS_PROCESSING)
            ->setResult((new JobResult())->setMessage('processing'))
            ->setMetrics((new JobMetrics())->setBackendSize('large'))
            ->setUsageData(['foo' => 'bar'])
            ->setRunnerId('runner-id');

        $expectedResult = [
            'message' => 'processing',
            'configVersion' => null,
            'images' => [],
            'input' => [
                'tables' => [],
            ],
            'output' => [
                'tables' => [],
            ],
        ];
        $expectedMetrics = [
            'storage' => [
                'inputTablesBytesSum' => null,
                'outputTablesBytesSum' => null,
            ],
            'backend' => [
                'size' => 'large',
                'containerSize' => null,
                'context' => null,
            ],
        ];
        self::assertSame(JobInterface::STATUS_PROCESSING, $jobPatchData->getStatus());
        self::assertSame(JobInterface::DESIRED_STATUS_PROCESSING, $jobPatchData->getDesiredStatus());
        self::assertSame($expectedResult, $jobPatchData->getResult()?->jsonSerialize());
        self::assertSame($expectedMetrics, $jobPatchData->getMetrics()?->jsonSerialize());
        self::assertSame(['foo' => 'bar'], $jobPatchData->getUsageData());
        self::assertSame('runner-id', $jobPatchData->getRunnerId());
        self::assertSame(
            [
                'status' => JobInterface::STATUS_PROCESSING,
                'desiredStatus' => JobInterface::DESIRED_STATUS_PROCESSING,
                'result' => $expectedResult,
                'metrics' => $expectedMetrics,
                'usageData' => [
                    'foo' => 'bar',
                ],
                'runnerId' => 'runner-id',
            ],
            $jobPatchData->jsonSerialize(),
        );
    }

    public function testAccessorsIncomplete(): void
    {
        $jobPatchData = new JobPatchData();
        $jobPatchData->setStatus(JobInterface::STATUS_PROCESSING);

        self::assertSame(JobInterface::STATUS_PROCESSING, $jobPatchData->getStatus());
        self::assertNull($jobPatchData->getDesiredStatus());
        self::assertNull($jobPatchData->getResult());
        self::assertNull($jobPatchData->getUsageData());
        self::assertSame(['status' => JobInterface::STATUS_PROCESSING], $jobPatchData->jsonSerialize());

        $jobPatchData2 = new JobPatchData();
        $jobPatchData2->setDesiredStatus(JobInterface::DESIRED_STATUS_TERMINATING);

        self::assertSame(JobInterface::DESIRED_STATUS_TERMINATING, $jobPatchData2->getDesiredStatus());
        self::assertNull($jobPatchData2->getStatus());
        self::assertNull($jobPatchData2->getResult());
        self::assertNull($jobPatchData2->getUsageData());
        self::assertSame(
            ['desiredStatus' => JobInterface::DESIRED_STATUS_TERMINATING],
            $jobPatchData2->jsonSerialize(),
        );
    }

    public function testInvalidStatus(): void
    {
        $jobPatchData = new JobPatchData();
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid status: "new york".');
        $jobPatchData->setStatus('new york');
    }

    public function testInvalidDesiredStatus(): void
    {
        $jobPatchData = new JobPatchData();
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid desiredStatus: "prague".');
        $jobPatchData->setDesiredStatus('prague');
    }
}
