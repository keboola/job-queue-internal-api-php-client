<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use DateTimeImmutable;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\ExistingJobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\SearchJobsFilters;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class ClientSearchTest extends TestCase
{
    public static function provideSearchJobsTestData(): iterable
    {
        yield 'no params' => [
            'filters' => null,
            'sortBy' => null,
            'sortOrder' => null,
            'offset' => null,
            'limit' => null,
            'expectedQuery' => '',
        ];

        yield 'with filters' => [
            'filters' => new SearchJobsFilters(
                id: [1, 2],
                startTimeFrom: new DateTimeImmutable('2021-01-01'),
                startTimeTo: new DateTimeImmutable('2021-01-02'),
                createdTimeFrom: new DateTimeImmutable('2021-01-03'),
                createdTimeTo: new DateTimeImmutable('2021-01-04'),
                endTimeFrom: new DateTimeImmutable('2021-01-05'),
                endTimeTo: new DateTimeImmutable('2021-01-06'),
            ),
            'sortBy' => null,
            'sortOrder' => null,
            'offset' => null,
            'limit' => null,
            'expectedQuery' => http_build_query([
                'filters' => [
                    'id' => [1, 2],
                    'startTimeFrom' => '2021-01-01T00:00:00+00:00',
                    'startTimeTo' => '2021-01-02T00:00:00+00:00',
                    'createdTimeFrom' => '2021-01-03T00:00:00+00:00',
                    'createdTimeTo' => '2021-01-04T00:00:00+00:00',
                    'endTimeFrom' => '2021-01-05T00:00:00+00:00',
                    'endTimeTo' => '2021-01-06T00:00:00+00:00',
                ],
            ]),
        ];

        yield 'sorting' => [
            'filters' => new SearchJobsFilters(
                id: [1],
            ),
            'sortBy' => 'startTime',
            'sortOrder' => 'desc',
            'offset' => null,
            'limit' => null,
            'expectedQuery' => http_build_query([
                'filters' => [
                    'id' => [1],
                ],
                'sortBy' => 'startTime',
                'sortOrder' => 'desc',
            ]),
        ];

        yield 'pagination' => [
            'filters' => new SearchJobsFilters(
                id: [1],
            ),
            'sortBy' => null,
            'sortOrder' => null,
            'offset' => 10,
            'limit' => 100,
            'expectedQuery' => http_build_query([
                'filters' => [
                    'id' => [1],
                ],
                'offset' => 10,
                'limit' => 100,
            ]),
        ];
    }

    /**
     * @dataProvider provideSearchJobsTestData
     * @param non-empty-string|null $sortBy
     * @param "asc"|"desc"|null $sortOrder
     * @param positive-int|null $offset
     * @param int<1, 500>|null $limit
     */
    public function testSearchJobs(
        ?SearchJobsFilters $filters,
        ?string $sortBy,
        ?string $sortOrder,
        ?int $offset,
        ?int $limit,
        string $expectedQuery,
    ): void {
        $requests = [];
        $responses = [
            new Response(200, body: (string) json_encode([
                $this->createJobData(1),
                $this->createJobData(2),
            ])),
        ];

        $client = $this->createClient($requests, $responses);
        $jobs = $client->searchJobs($filters, $sortBy, $sortOrder, $offset, $limit);

        self::assertCount(2, $jobs);
        self::assertSame('1', $jobs[0]->getId());
        self::assertSame('2', $jobs[1]->getId());

        self::assertCount(1, $requests);
        $request = $requests[0]['request'];

        self::assertSame('GET', $request->getMethod());
        self::assertSame('/search/jobs', $request->getUri()->getPath());
        self::assertSame($expectedQuery, $request->getUri()->getQuery());
    }

    public static function provideSearchJobsGroupedTestData(): iterable
    {
        yield 'no params' => [
            'groupBy' => ['componentId'],
            'filters' => null,
            'sortBy' => null,
            'sortOrder' => null,
            'jobsPerGroup' => null,
            'limit' => null,
            'expectedQuery' => http_build_query([
                'groupBy' => ['componentId'],
            ]),
        ];

        yield 'with filters' => [
            'groupBy' => ['projectId'],
            'filters' => new SearchJobsFilters(
                id: [1, 2],
                startTimeFrom: new DateTimeImmutable('2021-01-01'),
                startTimeTo: new DateTimeImmutable('2021-01-02'),
                createdTimeFrom: new DateTimeImmutable('2021-01-03'),
                createdTimeTo: new DateTimeImmutable('2021-01-04'),
                endTimeFrom: new DateTimeImmutable('2021-01-05'),
                endTimeTo: new DateTimeImmutable('2021-01-06'),
            ),
            'sortBy' => null,
            'sortOrder' => null,
            'jobsPerGroup' => null,
            'limit' => null,
            'expectedQuery' => http_build_query([
                'groupBy' => ['projectId'],
                'filters' => [
                    'id' => [1, 2],
                    'startTimeFrom' => '2021-01-01T00:00:00+00:00',
                    'startTimeTo' => '2021-01-02T00:00:00+00:00',
                    'createdTimeFrom' => '2021-01-03T00:00:00+00:00',
                    'createdTimeTo' => '2021-01-04T00:00:00+00:00',
                    'endTimeFrom' => '2021-01-05T00:00:00+00:00',
                    'endTimeTo' => '2021-01-06T00:00:00+00:00',
                ],
            ]),
        ];

        yield 'sorting' => [
            'groupBy' => ['componentId', 'projectId'],
            'filters' => new SearchJobsFilters(
                id: [1],
            ),
            'sortBy' => 'startTime',
            'sortOrder' => 'desc',
            'jobsPerGroup' => null,
            'limit' => null,
            'expectedQuery' => http_build_query([
                'groupBy' => ['componentId', 'projectId'],
                'filters' => [
                    'id' => [1],
                ],
                'sortBy' => 'startTime',
                'sortOrder' => 'desc',
            ]),
        ];

        yield 'jobsPerGroup' => [
            'groupBy' => ['componentId'],
            'filters' => new SearchJobsFilters(
                id: [1],
            ),
            'sortBy' => null,
            'sortOrder' => null,
            'jobsPerGroup' => 10,
            'limit' => 100,
            'expectedQuery' => http_build_query([
                'groupBy' => ['componentId'],
                'filters' => [
                    'id' => [1],
                ],
                'jobsPerGroup' => 10,
                'limit' => 100,
            ]),
        ];
    }

    /**
     * @dataProvider provideSearchJobsGroupedTestData
     * @param non-empty-array<non-empty-string> $groupBy
     * @param non-empty-string|null $sortBy
     * @param "asc"|"desc"|null $sortOrder
     * @param int<1, 500>|null $jobsPerGroup
     * @param int<1, 500>|null $limit
     */
    public function testSearchJobsGrouped(
        array $groupBy,
        ?SearchJobsFilters $filters,
        ?string $sortBy,
        ?string $sortOrder,
        ?int $jobsPerGroup,
        ?int $limit,
        string $expectedQuery,
    ): void {
        $requests = [];
        $responses = [
            new Response(200, body: (string) json_encode([
                [
                    'group' => array_fill_keys($groupBy, true),
                    'jobs' => [
                        $this->createJobData(1),
                        $this->createJobData(2),
                    ],
                ],
            ])),
        ];

        $client = $this->createClient($requests, $responses);
        $jobsGrouped = $client->searchJobsGrouped($groupBy, $filters, $sortBy, $sortOrder, $jobsPerGroup, $limit);

        $jobs = $jobsGrouped[0]['jobs'];
        self::assertCount(2, $jobs);
        self::assertSame('1', $jobs[0]->getId());
        self::assertSame('2', $jobs[1]->getId());

        $groups = $jobsGrouped[0]['group'];
        self::assertCount(count($groupBy), $groups);
        self::assertSame($groupBy, array_keys($groups));

        self::assertCount(1, $requests);
        $request = $requests[0]['request'];

        self::assertSame('GET', $request->getMethod());
        self::assertSame('/search/grouped-jobs', $request->getUri()->getPath());
        self::assertSame($expectedQuery, $request->getUri()->getQuery());
    }

    public function testSearchAllJobs(): void
    {
        $requests = [];
        $responses = [
            new Response(200, body: (string) json_encode(
                array_map($this->createJobData(...), range(1, 100)),
            )),
            new Response(200, body: (string) json_encode(
                array_map($this->createJobData(...), range(101, 200)),
            )),
            new Response(200, body: (string) json_encode(
                array_map($this->createJobData(...), range(201, 210)),
            )),
        ];

        $client = $this->createClient($requests, $responses);
        $jobs = $client->searchAllJobs(
            filters: new SearchJobsFilters(branchId: ['123']),
            sortBy: 'id',
            sortOrder: 'asc',
        );
        $jobs = [...$jobs]; // load iterable data

        self::assertCount(210, $jobs);
        self::assertSame('1', $jobs[0]->getId());
        self::assertSame('2', $jobs[1]->getId());

        self::assertCount(3, $requests);

        // all requests should be the same filter, sort & limit, but different offset
        self::assertSame('GET', $requests[0]['request']->getMethod());
        self::assertSame('/search/jobs', $requests[0]['request']->getUri()->getPath());
        self::assertSame(
            'filters%5BbranchId%5D%5B0%5D=123&sortBy=id&sortOrder=asc&offset=0&limit=100',
            $requests[0]['request']->getUri()->getQuery(),
        );

        self::assertSame('GET', $requests[1]['request']->getMethod());
        self::assertSame('/search/jobs', $requests[1]['request']->getUri()->getPath());
        self::assertSame(
            'filters%5BbranchId%5D%5B0%5D=123&sortBy=id&sortOrder=asc&offset=100&limit=100',
            $requests[1]['request']->getUri()->getQuery(),
        );

        self::assertSame('GET', $requests[2]['request']->getMethod());
        self::assertSame('/search/jobs', $requests[2]['request']->getUri()->getPath());
        self::assertSame(
            'filters%5BbranchId%5D%5B0%5D=123&sortBy=id&sortOrder=asc&offset=200&limit=100',
            $requests[2]['request']->getUri()->getQuery(),
        );
    }

    public function testSearchJobsRawQueryParametersPropagation(): void
    {
        $requests = [];
        $responses = [new Response(body: '[]')];

        $client = $this->createClient($requests, $responses);
        $client->searchJobsRaw([
            'sortBy' => '1',
            'sortOrder' => '1',
            'offset' => '1',
            'limit' => '1',
            'filters' => [
                'id' => ['1'],
                'runId' => ['1'],
                'branchId' => ['1'],
                'configId' => ['1'],
                'configRowIds' => ['1'],
                'projectId' => ['1'],
                'tokenId' => ['1'],
                'tokenDescription' => ['1'],
                'componentId' => ['1'],
                'status' => ['1'],
                'desiredStatus' => ['1'],
                'mode' => ['1'],
                'tag' => ['1'],
                'startTimeFrom' => '1',
                'startTimeTo' => '1',
                'createdTimeFrom' => '1',
                'createdTimeTo' => '1',
                'endTimeFrom' => '1',
                'endTimeTo' => '1',
                'durationSecondsFrom' => '1',
                'durationSecondsTo' => '1',
                'variableValuesId' => ['1'],
                'parentRunId' => ['1'],
                'type' => ['1'],
            ],
        ]);

        self::assertSame(
            'sortBy=1&sortOrder=1&offset=1&limit=1&filters%5Bid%5D%5B0%5D=1&' .
            'filters%5BrunId%5D%5B0%5D=1&filters%5BbranchId%5D%5B0%5D=1&filters%5BconfigId%5D%5B0%5D=1&' .
            'filters%5BconfigRowIds%5D%5B0%5D=1&filters%5BprojectId%5D%5B0%5D=1&filters%5BtokenId%5D%5B0%5D=1&' .
            'filters%5BtokenDescription%5D%5B0%5D=1&filters%5BcomponentId%5D%5B0%5D=1&filters%5Bstatus%5D%5B0%5D=1&' .
            'filters%5BdesiredStatus%5D%5B0%5D=1&filters%5Bmode%5D%5B0%5D=1&filters%5Btag%5D%5B0%5D=1&' .
            'filters%5BstartTimeFrom%5D=1&filters%5BstartTimeTo%5D=1&filters%5BcreatedTimeFrom%5D=1&' .
            'filters%5BcreatedTimeTo%5D=1&filters%5BendTimeFrom%5D=1&filters%5BendTimeTo%5D=1&' .
            'filters%5BdurationSecondsFrom%5D=1&filters%5BdurationSecondsTo%5D=1&' .
            'filters%5BvariableValuesId%5D%5B0%5D=1&filters%5BparentRunId%5D%5B0%5D=1&filters%5Btype%5D%5B0%5D=1',
            $requests[0]['request']->getUri()->getQuery(),
        );
    }

    public function testSearchJobsGroupedRawQueryParametersPropagation(): void
    {
        $requests = [];
        $responses = [new Response(body: '[]')];

        $client = $this->createClient($requests, $responses);
        $client->searchJobsGroupedRaw([
            'sortBy' => '1',
            'sortOrder' => '1',
            'jobsPerGroup' => '1',
            'limit' => '1',
            'groupBy' => ['1'],
            'filters' => [
                'id' => ['1'],
                'runId' => ['1'],
                'branchId' => ['1'],
                'configId' => ['1'],
                'configRowIds' => ['1'],
                'projectId' => ['1'],
                'tokenId' => ['1'],
                'tokenDescription' => ['1'],
                'componentId' => ['1'],
                'status' => ['1'],
                'desiredStatus' => ['1'],
                'mode' => ['1'],
                'tag' => ['1'],
                'startTimeFrom' => '1',
                'startTimeTo' => '1',
                'createdTimeFrom' => '1',
                'createdTimeTo' => '1',
                'endTimeFrom' => '1',
                'endTimeTo' => '1',
                'durationSecondsFrom' => '1',
                'durationSecondsTo' => '1',
                'variableValuesId' => ['1'],
                'parentRunId' => ['1'],
                'type' => ['1'],
            ],
        ]);

        self::assertSame(
            'sortBy=1&sortOrder=1&jobsPerGroup=1&limit=1&groupBy%5B0%5D=1&filters%5Bid%5D%5B0%5D=1&' .
            'filters%5BrunId%5D%5B0%5D=1&filters%5BbranchId%5D%5B0%5D=1&filters%5BconfigId%5D%5B0%5D=1&' .
            'filters%5BconfigRowIds%5D%5B0%5D=1&filters%5BprojectId%5D%5B0%5D=1&filters%5BtokenId%5D%5B0%5D=1&' .
            'filters%5BtokenDescription%5D%5B0%5D=1&filters%5BcomponentId%5D%5B0%5D=1&filters%5Bstatus%5D%5B0%5D=1&' .
            'filters%5BdesiredStatus%5D%5B0%5D=1&filters%5Bmode%5D%5B0%5D=1&filters%5Btag%5D%5B0%5D=1&' .
            'filters%5BstartTimeFrom%5D=1&filters%5BstartTimeTo%5D=1&filters%5BcreatedTimeFrom%5D=1&' .
            'filters%5BcreatedTimeTo%5D=1&filters%5BendTimeFrom%5D=1&filters%5BendTimeTo%5D=1&' .
            'filters%5BdurationSecondsFrom%5D=1&filters%5BdurationSecondsTo%5D=1&' .
            'filters%5BvariableValuesId%5D%5B0%5D=1&filters%5BparentRunId%5D%5B0%5D=1&filters%5Btype%5D%5B0%5D=1',
            $requests[0]['request']->getUri()->getQuery(),
        );
    }

    private function createClient(array &$requests, array $responses): Client
    {
        $httpHandler = new MockHandler($responses);

        $handlerStack = HandlerStack::create($httpHandler);
        $handlerStack->push(Middleware::history($requests));

        $existingJobFactory = $this->createMock(ExistingJobFactory::class);
        $existingJobFactory->method('loadFromElasticJobData')
            ->willReturnCallback(function (array $data) {
                $job = $this->createMock(Job::class);
                $job->method('getId')->willReturn((string) $data['id']);
                return $job;
            })
        ;

        return new Client(
            new Logger('test'),
            $existingJobFactory,
            'http://example.com',
            internalQueueToken: 'internal-token',
            storageApiToken: null,
            applicationToken: null,
            options: [
                'handler' => $handlerStack,
            ],
        );
    }

    private function createJobData(int $id): array
    {
        return [
            'id' => $id,
        ];
    }
}
