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
        $metrics->setBackendSize('large');
        self::assertSame(123, $metrics->getInputTablesBytesSum());
        self::assertSame('large', $metrics->getBackendSize());
        self::assertSame(
            [
                'storage' => [
                    'inputTablesBytesSum' => 123,
                ],
                'backend' => [
                    'size' => 'large',
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
                'backend' => [
                    'size' => null,
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
                'backend' => [
                    'size' => 'medium',
                ],
            ],
        ];
        $jobMetrics = JobMetrics::fromDataArray($data);
        self::assertSame(123, $jobMetrics->getInputTablesBytesSum());
        self::assertSame('medium', $jobMetrics->getBackendSize());

        $jobMetrics = JobMetrics::fromDataArray([]);
        self::assertNull($jobMetrics->getInputTablesBytesSum());
        self::assertNull($jobMetrics->getBackendSize());
    }
}
