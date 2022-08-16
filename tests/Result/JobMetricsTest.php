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
        $metrics->setOutputTablesBytesSum(456);
        $metrics->setBackendSize('large');
        $metrics->setBackendContainerSize('small');
        self::assertSame(123, $metrics->getInputTablesBytesSum());
        self::assertSame('large', $metrics->getBackendSize());
        self::assertSame(
            [
                'storage' => [
                    'inputTablesBytesSum' => 123,
                    'outputTablesBytesSum' => 456,
                ],
                'backend' => [
                    'size' => 'large',
                    'containerSize' => 'small',
                ],
            ],
            $metrics->jsonSerialize()
        );

        $metrics = new JobMetrics();
        self::assertSame(
            [
                'storage' => [
                    'inputTablesBytesSum' => null,
                    'outputTablesBytesSum' => null,
                ],
                'backend' => [
                    'size' => null,
                    'containerSize' => null,
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
                    'outputTablesBytesSum' => 456,
                ],
                'backend' => [
                    'size' => 'medium',
                    'containerSize' => 'large',
                ],
            ],
        ];
        $jobMetrics = JobMetrics::fromDataArray($data);
        self::assertSame(123, $jobMetrics->getInputTablesBytesSum());
        self::assertSame(456, $jobMetrics->getOutputTablesBytesSum());
        self::assertSame('medium', $jobMetrics->getBackendSize());
        self::assertSame('large', $jobMetrics->getBackendContainerSize());

        $jobMetrics = JobMetrics::fromDataArray([]);
        self::assertNull($jobMetrics->getInputTablesBytesSum());
        self::assertNull($jobMetrics->getBackendSize());
        self::assertNull($jobMetrics->getBackendContainerSize());
    }
}
