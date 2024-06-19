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
            'filters' => null,
            'sortBy' => null,
            'sortOrder' => null,
            'jobsPerGroup' => null,
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
            'jobsPerGroup' => null,
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
            'jobsPerGroup' => null,
            'limit' => null,
            'expectedQuery' => http_build_query([
                'filters' => [
                    'id' => [1],
                ],
                'sortBy' => 'startTime',
                'sortOrder' => 'desc',
            ]),
        ];

        yield 'jobsPerGroup' => [
            'filters' => new SearchJobsFilters(
                id: [1],
            ),
            'sortBy' => null,
            'sortOrder' => null,
            'jobsPerGroup' => 10,
            'limit' => 100,
            'expectedQuery' => http_build_query([
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
     * @param non-empty-string|null $sortBy
     * @param "asc"|"desc"|null $sortOrder
     * @param int<1, 500>|null $jobsPerGroup
     * @param int<1, 500>|null $limit
     */
    public function testSearchJobsGrouped(
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
                $this->createJobData(1),
                $this->createJobData(2),
            ])),
        ];

        $client = $this->createClient($requests, $responses);
        $jobs = $client->searchJobsGrouped($filters, $sortBy, $sortOrder, $jobsPerGroup, $limit);

        self::assertCount(2, $jobs);
        self::assertSame('1', $jobs[0]->getId());
        self::assertSame('2', $jobs[1]->getId());

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

    private function createClient(array &$requests, array $responses): Client
    {
        $httpHandler = new MockHandler($responses);

        $handlerStack = HandlerStack::create($httpHandler);
        $handlerStack->push(Middleware::history($requests));

        $existingJobFactory = $this->createMock(ExistingJobFactory::class);
        $existingJobFactory->method('loadFromExistingJobData')
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
