<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobsSortOptions;
use PHPUnit\Framework\TestCase;

class JobsSortOptionsTest extends TestCase
{
    public function testSetSortOptions(): void
    {
        $jobSortOptions = new JobsSortOptions();

        $jobSortOptions->setSortBy('id');
        self::assertSame('id', $jobSortOptions->getSortBy());

        $jobSortOptions->setSortOrder(JobsSortOptions::SORT_ORDER_DESC);
        self::assertSame('desc', $jobSortOptions->getSortOrder());

        $jobSortOptions->setSortOrder(JobsSortOptions::SORT_ORDER_ASC);
        self::assertSame('asc', $jobSortOptions->getSortOrder());
    }

    public function testSetSortOrderWrongValue(): void
    {
        $jobListOptions = new JobsSortOptions();

        self::expectException(ClientException::class);
        self::expectExceptionMessage('Allowed values for "sortOrder" are [asc, desc].');

        $jobListOptions->setSortOrder('left');
    }
}
