<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\ListLatestConfigurationsJobsOptions;
use PHPUnit\Framework\TestCase;

class ListLatestConfigurationsJobsOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new ListLatestConfigurationsJobsOptions('12345');

        self::assertSame('12345', $options->getProjectId());
        self::assertNull($options->getOffset());
        self::assertNull($options->getLimit());
        self::assertNull($options->getSortBy());
        self::assertNull($options->getSortOrder());
        self::assertNull($options->getBranchId());
        self::assertNull($options->getType());

        self::assertSame(['projectId=12345'], $options->getQueryParameters());
    }

    public function testAllValues(): void
    {
        $options = new ListLatestConfigurationsJobsOptions('12345');
        $options->setOffset(6);
        $options->setLimit(7);
        $options->setSort('configId', 'asc');
        $options->setBranchId('main');
        $options->setType(JobFactory::TYPE_ORCHESTRATION_CONTAINER);

        self::assertSame('12345', $options->getProjectId());
        self::assertSame(6, $options->getOffset());
        self::assertSame(7, $options->getLimit());
        self::assertSame('configId', $options->getSortBy());
        self::assertSame('asc', $options->getSortOrder());
        self::assertSame('main', $options->getBranchId());
        self::assertSame(JobFactory::TYPE_ORCHESTRATION_CONTAINER, $options->getType());

        self::assertSame([
            'projectId=12345',
            'offset=6',
            'limit=7',
            'sortBy=configId',
            'sortOrder=asc',
            'branchId=main',
            'type=orchestrationContainer',
        ], $options->getQueryParameters());
    }

    public function testSortOrderIsValidated(): void
    {
        $options = new ListLatestConfigurationsJobsOptions('12345');

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid sort order "xxx", expected one of: asc, desc');

        $options->setSort('configId', 'xxx');
    }

    public function testSortingReset(): void
    {
        $options = new ListLatestConfigurationsJobsOptions('12345');

        $options->setSort('field');
        self::assertSame('field', $options->getSortBy());
        self::assertSame('asc', $options->getSortOrder());

        $options->setSort(null);
        self::assertNull($options->getSortBy());
        self::assertNull($options->getSortOrder());
    }
}
