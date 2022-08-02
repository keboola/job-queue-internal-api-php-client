<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\JobFactory;
use PHPUnit\Framework\TestCase;

class JobFactoryTest extends TestCase
{
    public function testStaticGetters(): void
    {
        self::assertCount(5, JobFactory::getFinishedStatuses());
        self::assertCount(9, JobFactory::getAllStatuses());
        self::assertCount(3, JobFactory::getKillableStatuses());
    }
}
