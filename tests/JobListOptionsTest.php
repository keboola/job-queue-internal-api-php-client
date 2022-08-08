<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobListOptions;
use PHPUnit\Framework\TestCase;
use Safe\DateTime;

class JobListOptionsTest extends TestCase
{
    public function testGetQueryParameters(): void
    {
        $jobListOptions = new JobListOptions();

        $jobListOptions->setIds(['1', '2', '3']);
        $jobListOptions->setRunIds(['5', '6', '7']);
        $jobListOptions->setBranchIds(['branch1', 'branch2', 'branch3']);
        $jobListOptions->setTokenIds(['8', '9', '10']);
        $jobListOptions->setTokenDescriptions(['new token', 'old token', 'bad token', 'good token']);
        $jobListOptions->setComponents(['writer', 'extractor', 'orchestrator']);
        $jobListOptions->setConfigs(['a', 'b', 'c']);
        $jobListOptions->setConfigRowIds(['d', 'e', 'f']);
        $jobListOptions->setProjects(['12', '13']);
        $jobListOptions->setModes(['run', 'debug']);
        $jobListOptions->setStatuses([JobInterface::STATUS_SUCCESS, JobInterface::STATUS_PROCESSING]);
        $jobListOptions->setParentRunId('123');
        $jobListOptions->setType(JobInterface::TYPE_STANDARD);
        $jobListOptions->setCreatedTimeFrom(DateTime::createFromFormat('Y-m-d H:i:s', '2022-02-02 1:12:23'));
        $jobListOptions->setCreatedTimeTo(DateTime::createFromFormat('Y-m-d H:i:s', '2022-02-20 1:12:23'));
        $jobListOptions->setStartTimeFrom(DateTime::createFromFormat('Y-m-d H:i:s', '2021-02-02 1:12:23'));
        $jobListOptions->setStartTimeTo(DateTime::createFromFormat('Y-m-d H:i:s', '2021-02-20 1:12:23'));
        $jobListOptions->setEndTimeFrom(DateTime::createFromFormat('Y-m-d H:i:s', '2020-02-02 1:12:23'));
        $jobListOptions->setEndTimeTo(DateTime::createFromFormat('Y-m-d H:i:s', '2020-02-20 1:12:23'));
        $jobListOptions->setDurationSecondsFrom(5);
        $jobListOptions->setDurationSecondsTo(7200);
        $jobListOptions->setSortOrder(JobListOptions::SORT_ORDER_DESC);
        $jobListOptions->setSortBy('id');
        $jobListOptions->setOffset(20);
        $jobListOptions->setLimit(100);

        self::assertSame(['1', '2', '3'], $jobListOptions->getIds());
        self::assertSame(['5', '6', '7'], $jobListOptions->getRunIds());
        self::assertSame(['branch1', 'branch2', 'branch3'], $jobListOptions->getBranchIds());
        self::assertSame(['8', '9', '10'], $jobListOptions->getTokenIds());
        self::assertSame(
            ['new token', 'old token', 'bad token', 'good token'],
            $jobListOptions->getTokenDescriptions()
        );
        self::assertSame(['writer', 'extractor', 'orchestrator'], $jobListOptions->getComponents());
        self::assertSame(['a', 'b', 'c'], $jobListOptions->getConfigs());
        self::assertSame(['d', 'e', 'f'], $jobListOptions->getConfigRowIds());
        self::assertSame(['12', '13'], $jobListOptions->getProjects());
        self::assertSame(['run', 'debug'], $jobListOptions->getModes());
        self::assertSame(
            [JobInterface::STATUS_SUCCESS, JobInterface::STATUS_PROCESSING],
            $jobListOptions->getStatuses()
        );
        self::assertSame('123', $jobListOptions->getParentRunId());
        self::assertSame(JobInterface::TYPE_STANDARD, $jobListOptions->getType());
        self::assertSame('2022-02-02 01:12:23', $jobListOptions->getCreatedTimeFrom()->format('Y-m-d H:i:s'));
        self::assertSame('2022-02-20 01:12:23', $jobListOptions->getCreatedTimeTo()->format('Y-m-d H:i:s'));
        self::assertSame('2021-02-02 01:12:23', $jobListOptions->getStartTimeFrom()->format('Y-m-d H:i:s'));
        self::assertSame('2021-02-20 01:12:23', $jobListOptions->getStartTimeTo()->format('Y-m-d H:i:s'));
        self::assertSame('2020-02-02 01:12:23', $jobListOptions->getEndTimeFrom()->format('Y-m-d H:i:s'));
        self::assertSame('2020-02-20 01:12:23', $jobListOptions->getEndTimeTo()->format('Y-m-d H:i:s'));
        self::assertSame(5, $jobListOptions->getDurationSecondsFrom());
        self::assertSame(7200, $jobListOptions->getDurationSecondsTo());
        self::assertSame('id', $jobListOptions->getSortBy());
        self::assertSame(JobListOptions::SORT_ORDER_DESC, $jobListOptions->getSortOrder());
        self::assertSame(20, $jobListOptions->getOffset());
        self::assertSame(100, $jobListOptions->getLimit());

        $expected = [
            'id[]=1',
            'id[]=2',
            'id[]=3',
            'runId[]=5',
            'runId[]=6',
            'runId[]=7',
            'branchId[]=branch1',
            'branchId[]=branch2',
            'branchId[]=branch3',
            'tokenId[]=8',
            'tokenId[]=9',
            'tokenId[]=10',
            'tokenDescription[]=new+token',
            'tokenDescription[]=old+token',
            'tokenDescription[]=bad+token',
            'tokenDescription[]=good+token',
            'componentId[]=writer',
            'componentId[]=extractor',
            'componentId[]=orchestrator',
            'configId[]=a',
            'configId[]=b',
            'configId[]=c',
            'configRowIds[]=d',
            'configRowIds[]=e',
            'configRowIds[]=f',
            'mode[]=run',
            'mode[]=debug',
            'projectId[]=12',
            'projectId[]=13',
            'status[]=success',
            'status[]=processing',
            'durationSecondsFrom=5',
            'durationSecondsTo=7200',
            'offset=20',
            'limit=100',
            'sortBy=id',
            'sortOrder=desc',
            'type=standard',
            'parentRunId=123',
            'startTimeFrom=' . urlencode('2021-02-02T01:12:23+00:00'),
            'startTimeTo=' . urlencode('2021-02-20T01:12:23+00:00'),
            'createdTimeFrom=' . urlencode('2022-02-02T01:12:23+00:00'),
            'createdTimeTo=' . urlencode('2022-02-20T01:12:23+00:00'),
            'endTimeFrom=' . urlencode('2020-02-02T01:12:23+00:00'),
            'endTimeTo=' . urlencode('2020-02-20T01:12:23+00:00'),
        ];

        self::assertSame($expected, $jobListOptions->getQueryParameters());
    }

    public function testGetQueryParametersForParametersWithEmptyValueAllowed(): void
    {
        // default values
        $jobListOptions = new JobListOptions();

        self::assertSame(['limit=100'], $jobListOptions->getQueryParameters());
        self::assertNull($jobListOptions->getParentRunId());

        // empty string
        $jobListOptions->setParentRunId('');
        self::assertSame(
            [
                'limit=100',
                'parentRunId=',
            ],
            $jobListOptions->getQueryParameters()
        );
        self::assertSame('', $jobListOptions->getParentRunId());

        // null
        $jobListOptions->setParentRunId(null);
        self::assertSame(
            [
                'limit=100',
            ],
            $jobListOptions->getQueryParameters()
        );
        self::assertNull($jobListOptions->getParentRunId());
    }

    public function testSetSortOrderWrong(): void
    {
        $jobListOptions = new JobListOptions();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Allowed values for "sortOrder" are [asc, desc].');
        $jobListOptions->setSortOrder('left');
    }
}
