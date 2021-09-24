<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobPatchData;
use Keboola\JobQueueInternalClient\Result\JobResult;
use PHPUnit\Framework\TestCase;

class JobPatchDataTest extends TestCase
{
    public function testAccessors(): void
    {
        $jobPatchData = new JobPatchData();
        $jobPatchData
            ->setStatus(JobFactory::STATUS_PROCESSING)
            ->setDesiredStatus(JobFactory::DESIRED_STATUS_PROCESSING)
            ->setResult((new JobResult())->setMessage('processing'))
            ->setUsageData(['foo' => 'bar']);

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
        self::assertSame(JobFactory::STATUS_PROCESSING, $jobPatchData->getStatus());
        self::assertSame(JobFactory::DESIRED_STATUS_PROCESSING, $jobPatchData->getDesiredStatus());
        $result = is_null($jobPatchData->getResult()) ?: $jobPatchData->getResult()->jsonSerialize();
        self::assertSame($expectedResult, $result);
        self::assertSame(['foo' => 'bar'], $jobPatchData->getUsageData());
        self::assertSame(
            [
                'status' => JobFactory::STATUS_PROCESSING,
                'desiredStatus' => JobFactory::DESIRED_STATUS_PROCESSING,
                'result' => $expectedResult,
                'usageData' => [
                    'foo' => 'bar',
                ],
            ],
            $jobPatchData->jsonSerialize()
        );
    }

    public function testAccessorsIncomplete(): void
    {
        $jobPatchData = new JobPatchData();
        $jobPatchData->setStatus(JobFactory::STATUS_PROCESSING);

        self::assertSame(JobFactory::STATUS_PROCESSING, $jobPatchData->getStatus());
        self::assertNull($jobPatchData->getDesiredStatus());
        self::assertNull($jobPatchData->getResult());
        self::assertNull($jobPatchData->getUsageData());
        self::assertSame(['status' => JobFactory::STATUS_PROCESSING], $jobPatchData->jsonSerialize());

        $jobPatchData2 = new JobPatchData();
        $jobPatchData2->setDesiredStatus(JobFactory::DESIRED_STATUS_TERMINATING);

        self::assertSame(JobFactory::DESIRED_STATUS_TERMINATING, $jobPatchData2->getDesiredStatus());
        self::assertNull($jobPatchData2->getStatus());
        self::assertNull($jobPatchData2->getResult());
        self::assertNull($jobPatchData2->getUsageData());
        self::assertSame(['desiredStatus' => JobFactory::DESIRED_STATUS_TERMINATING], $jobPatchData2->jsonSerialize());
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
