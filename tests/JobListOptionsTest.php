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
        $jobListOptions->setBranchIds(['branch1', 'branch2', 'branch3']);
        $jobListOptions->setTokenIds(['1', '2', '3']);
        $jobListOptions->setTokenDescriptions(['new token', 'old token', 'bad token', 'good token']);
        $jobListOptions->setComponents(['writer', 'extractor', 'orchestrator']);
        $jobListOptions->setConfigs(['1', '2', '3']);
        $jobListOptions->setConfigRowIds(['1', '2', '3']);
        $jobListOptions->setModes(['run', 'debug']);
        $jobListOptions->setStatuses([JobFactory::STATUS_SUCCESS, JobFactory::STATUS_PROCESSING]);

        $from = (new \DateTime('-7 days 8:00'))->format('c');
        $to = (new \DateTime('-1 day 8:00'))->format('c');

        $jobListOptions->setStartTimeFrom($from);
        $jobListOptions->setStartTimeTo($to);
        $jobListOptions->setCreatedTimeFrom($from);
        $jobListOptions->setCreatedTimeTo($to);
        $jobListOptions->setEndTimeFrom($from);
        $jobListOptions->setEndTimeTo($to);
        $jobListOptions->setDurationSecondsFrom(5);
        $jobListOptions->setDurationSecondsTo(7200);
        $jobListOptions->setOffset(20);
        $jobListOptions->setLimit(100);
        $jobListOptions->setSortBy('id');
        $jobListOptions->setSortOrder(JobListOptions::SORT_ORDER_DESC);

        $expected = [
            'id[]=1',
            'id[]=2',
            'id[]=3',
            'runId[]=1',
            'runId[]=2',
            'runId[]=3',
            'branchId[]=branch1',
            'branchId[]=branch2',
            'branchId[]=branch3',
            'tokenId[]=1',
            'tokenId[]=2',
            'tokenId[]=3',
            'tokenDescription[]=new+token',
            'tokenDescription[]=old+token',
            'tokenDescription[]=bad+token',
            'tokenDescription[]=good+token',
            'componentId[]=writer',
            'componentId[]=extractor',
            'componentId[]=orchestrator',
            'configId[]=1',
            'configId[]=2',
            'configId[]=3',
            'configRowIds[]=1',
            'configRowIds[]=2',
            'configRowIds[]=3',
            'mode[]=run',
            'mode[]=debug',
            'status[]=success',
            'status[]=processing',
            'startTimeFrom=' . urlencode($from),
            'startTimeTo=' . urlencode($to),
            'createdTimeFrom=' . urlencode($from),
            'createdTimeTo=' . urlencode($to),
            'endTimeFrom=' . urlencode($from) ,
            'endTimeTo=' . urlencode($to) ,
            'durationSecondsFrom=5',
            'durationSecondsTo=7200',
            'offset=20',
            'limit=100',
            'sortBy=id',
            'sortOrder=desc',
        ];

        self::assertSame($expected, $jobListOptions->getQueryParameters());
    }

    public function testSetSortOrderWrong(): void
    {
        $jobListOptions = new JobListOptions();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Allowed values for "sortOrder" are [asc, desc].');
        $jobListOptions->setSortOrder('left');
    }
}
