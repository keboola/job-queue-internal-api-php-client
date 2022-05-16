<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\ListLatestConfigurationsJobsOptions;
use PHPUnit\Framework\TestCase;

class ListLatestConfigurationsJobsOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new ListLatestConfigurationsJobsOptions('12345');

        self::assertSame('12345', $options->getProjectId());
        self::assertSame('default', $options->getBranchId());
        self::assertNull($options->getOffset());
        self::assertNull($options->getLimit());

        self::assertSame([
            'projectId=12345',
            'branchId=default',
        ], $options->getQueryParameters());
    }

    public function testAllValues(): void
    {
        $options = new ListLatestConfigurationsJobsOptions('12345');
        $options->setOffset(6);
        $options->setLimit(7);
        $options->setBranchId('main');
        $options->setProjectId('54321');

        self::assertSame('54321', $options->getProjectId());
        self::assertSame(6, $options->getOffset());
        self::assertSame(7, $options->getLimit());
        self::assertSame('main', $options->getBranchId());

        self::assertSame([
            'projectId=54321',
            'branchId=main',
            'offset=6',
            'limit=7',
        ], $options->getQueryParameters());
    }
}
