<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\ListConfigurationsJobsOptions;
use PHPUnit\Framework\TestCase;

class ListConfigurationsJobsOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new ListConfigurationsJobsOptions(['abc', 'efg']);

        self::assertSame(['abc', 'efg'], $options->getConfigIds());
        self::assertNull($options->getProjectId());
        self::assertNull($options->getJobsPerConfig());
        self::assertNull($options->getOffset());
        self::assertNull($options->getLimit());
        self::assertNull($options->getSortBy());
        self::assertNull($options->getSortOrder());
        self::assertSame([], $options->getBranchIds());

        self::assertSame([
            'configId[]=abc',
            'configId[]=efg',
        ], $options->getQueryParameters());
    }

    public function testAllValues(): void
    {
        $options = new ListConfigurationsJobsOptions(['abc', 'efg']);
        $options->setProjectId('my-project');
        $options->setJobsPerConfig(5);
        $options->setOffset(6);
        $options->setLimit(7);
        $options->setSort('configId', 'asc');
        $options->setBranchIds(['main', 'test']);

        self::assertSame(['abc', 'efg'], $options->getConfigIds());
        self::assertSame('my-project', $options->getProjectId());
        self::assertSame(5, $options->getJobsPerConfig());
        self::assertSame(6, $options->getOffset());
        self::assertSame(7, $options->getLimit());
        self::assertSame('configId', $options->getSortBy());
        self::assertSame('asc', $options->getSortOrder());
        self::assertSame(['main', 'test'], $options->getBranchIds());

        self::assertSame([
            'configId[]=abc',
            'configId[]=efg',
            'branchId[]=main',
            'branchId[]=test',
            'jobsPerConfiguration=5',
            'projectId=my-project',
            'offset=6',
            'limit=7',
            'sortBy=configId',
            'sortOrder=asc',
        ], $options->getQueryParameters());
    }

    public function testConfigIdsListIsValidated(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('All configuration IDs must be strings');

        new ListConfigurationsJobsOptions([1]);
    }

    public function testSortOrderIsValidated(): void
    {
        $options = new ListConfigurationsJobsOptions(['123']);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid sort order "xxx", expected one of: asc, desc');

        $options->setSort('configId', 'xxx');
    }

    public function testSortingReset(): void
    {
        $options = new ListConfigurationsJobsOptions(['123']);

        $options->setSort('field');
        self::assertSame('field', $options->getSortBy());
        self::assertSame('asc', $options->getSortOrder());

        $options->setSort(null);
        self::assertNull($options->getSortBy());
        self::assertNull($options->getSortOrder());
    }
}
