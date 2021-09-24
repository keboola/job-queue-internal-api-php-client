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
    }
}
