<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result;

use Keboola\JobQueueInternalClient\Result\JobMetrics;
use PHPUnit\Framework\TestCase;

class JobMetricsTest extends TestCase
{
    public function testAccessors(): void
    {
        $metrics = new JobMetrics();
        $metrics->setInputTablesBytesSum(123);
        self::assertSame(123, $metrics->getInputTablesBytesSum());
        self::assertSame(
            [
                'storage' => [
                    'inputTablesBytesSum' => 123,
                ],
            ],
            $metrics->jsonSerialize()
        );

        $metrics = new JobMetrics();
        self::assertSame(
            [
                'storage' => [
                    'inputTablesBytesSum' => null,
                ],
            ],
            $metrics->jsonSerialize()
        );
    }

    public function testFromArray(): void
    {
        $data = [
            'metrics' => [
                'storage' => [
                    'inputTablesBytesSum' => 123,
                ],
            ],
        ];
        $jobMetrics = JobMetrics::fromDataArray($data);
        self::assertSame(123, $jobMetrics->getInputTablesBytesSum());

        $jobMetrics = JobMetrics::fromDataArray([]);
        self::assertNull($jobMetrics->getInputTablesBytesSum());
    }
}
