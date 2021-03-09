<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\JobResult;
use Keboola\JobQueueInternalClient\JobListOptions;
use Keboola\JobQueueInternalClient\JobPatchData;
use PHPUnit\Framework\TestCase;

class JobListOptionsTest extends TestCase
{
    public function testGetQueryParameters(): void
    {
        $jobListOptions = new JobListOptions();

        $jobListOptions->setIds(['1', '2', '3']);
        $jobListOptions->setRunIds(['1', '2', '3']);
        $jobListOptions->setTokenIds(['1', '2', '3']);
        $jobListOptions->setTokenDescriptions(['new token', 'old token', 'bad token', 'good token']);
        $jobListOptions->setComponents(['writer', 'extractor', 'orchestrator']);
        $jobListOptions->setConfigs(['1', '2', '3']);
        $jobListOptions->setModes(['run', 'debug']);
        $jobListOptions->setStatuses([JobFactory::STATUS_SUCCESS, JobFactory::STATUS_PROCESSING]);

        $dtStartFrom = new \DateTime('-7 days 8:00');
        $dtStartTo = new \DateTime('-1 day 8:00');
        $dtCreatedFrom = new \DateTime('-7 days 8:00');
        $dtCreatedTo = new \DateTime('-1 day 8:00');
        $dtEndFrom = new \DateTime('-7 days 8:00');
        $dtEndTo = new \DateTime('-1 day 8:00');

        $jobListOptions->setStartTimeFrom('-7 days 8:00');
        $jobListOptions->setStartTimeTo('-1 day 8:00');
        $jobListOptions->setCreatedTimeFrom('-7 days 8:00');
        $jobListOptions->setCreatedTimeTo('-1 day 8:00');
        $jobListOptions->setEndTimeTo('-7 days 8:00');
        $jobListOptions->setEndTimeFrom('-1 day 8:00');
        $jobListOptions->setOffset(20);
        $jobListOptions->setLimit(100);
        $jobListOptions->setSortBy('id');
        $jobListOptions->setSortOrder(JobListOptions::SORT_ORDER_DESC);

        $expected = $jobListOptions->getQueryParameters();

        var_dump($expected);
    }

    public function testSetSortOrderWrong(): void
    {
        $jobListOptions = new JobListOptions();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Allowed values for "sortOrder" are [asc, desc].');
        $jobListOptions->setSortOrder('left');
    }
}
