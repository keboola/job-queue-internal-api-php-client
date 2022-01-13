<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\ListConfigurationsJobsOptions;
use PHPUnit\Framework\TestCase;

class ListConfigurationsJobsOptionsTest extends TestCase
{
    private const COMPONENT_ID = 'keboola.runner-config-test';

    public function testDefaultValues(): void
    {
        $options = new ListConfigurationsJobsOptions(['abc', 'efg'], self::COMPONENT_ID);

        self::assertSame(['abc', 'efg'], $options->getConfigIds());
        self::assertSame('keboola.runner-config-test', $options->getComponentId());
        self::assertNull($options->getProjectId());
        self::assertNull($options->getJobsPerConfig());
        self::assertNull($options->getOffset());
        self::assertNull($options->getLimit());
        self::assertNull($options->getSortBy());
        self::assertNull($options->getSortOrder());
        self::assertNull($options->getBranchId());

        self::assertSame([
            'configId[]=abc',
            'configId[]=efg',
            'componentId=keboola.runner-config-test',
        ], $options->getQueryParameters());
    }

    public function testAllValues(): void
    {
        $options = new ListConfigurationsJobsOptions(['abc', 'efg'], self::COMPONENT_ID);
        $options->setProjectId('my-project');
        $options->setJobsPerConfig(5);
        $options->setOffset(6);
        $options->setLimit(7);
        $options->setSort('configId', 'asc');
        $options->setBranchId('main');

        self::assertSame(['abc', 'efg'], $options->getConfigIds());
        self::assertSame('my-project', $options->getProjectId());
        self::assertSame(5, $options->getJobsPerConfig());
        self::assertSame(6, $options->getOffset());
        self::assertSame(7, $options->getLimit());
        self::assertSame('configId', $options->getSortBy());
        self::assertSame('asc', $options->getSortOrder());
        self::assertSame('main', $options->getBranchId());

        self::assertSame([
            'configId[]=abc',
            'configId[]=efg',
            'jobsPerConfiguration=5',
            'projectId=my-project',
            'offset=6',
            'limit=7',
            'sortBy=configId',
            'sortOrder=asc',
            'branchId=main',
            'componentId=keboola.runner-config-test',
        ], $options->getQueryParameters());
    }

    public function testConfigIdsListIsValidated(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('All configuration IDs must be strings');

        new ListConfigurationsJobsOptions([1], self::COMPONENT_ID);
    }

    public function testSortOrderIsValidated(): void
    {
        $options = new ListConfigurationsJobsOptions(['123'], self::COMPONENT_ID);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid sort order "xxx", expected one of: asc, desc');

        $options->setSort('configId', 'xxx');
    }

    public function testSortingReset(): void
    {
        $options = new ListConfigurationsJobsOptions(['123'], self::COMPONENT_ID);

        $options->setSort('field');
        self::assertSame('field', $options->getSortBy());
        self::assertSame('asc', $options->getSortOrder());

        $options->setSort(null);
        self::assertNull($options->getSortBy());
        self::assertNull($options->getSortOrder());
    }
}
