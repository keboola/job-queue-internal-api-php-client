<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\JobType;
use PHPUnit\Framework\TestCase;

class JobTypeTest extends TestCase
{
    public function testIsContainer(): void
    {
        self::assertFalse(JobType::STANDARD->isContainer());
        self::assertTrue(JobType::ROW_CONTAINER->isContainer());
        self::assertTrue(JobType::PHASE_CONTAINER->isContainer());
        self::assertTrue(JobType::ORCHESTRATION_CONTAINER->isContainer());
    }
}
